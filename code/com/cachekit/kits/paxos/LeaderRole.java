package com.cachekit.kits.paxos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cachekit.kits.paxos.comm.CommLayer;
import com.cachekit.kits.paxos.comm.Member;
import com.cachekit.kits.paxos.comm.Tick;
import com.cachekit.kits.paxos.messages.Abort;
import com.cachekit.kits.paxos.messages.Accept;
import com.cachekit.kits.paxos.messages.Accepted;
import com.cachekit.kits.paxos.messages.BroadcastRequest;
import com.cachekit.kits.paxos.messages.NewView;
import com.cachekit.kits.paxos.messages.NoOp;
import com.cachekit.kits.paxos.messages.SpecialMessage;
import com.cachekit.kits.paxos.messages.Success;
import com.cachekit.kits.paxos.messages.SuccessAck;
import com.cachekit.kits.paxos.messages.ViewAccepted;
//如果leader角色初始化时，选择了自身节点作为leader，则准备一条 Election 消息，准备选举，每一次选举都创建新的纪元(年号)
public class LeaderRole implements FailureListener
{
	private static final NoOp NO_OP = new NoOp();
	private final GroupMembership membership;
	//UDP信使
	private final CommLayer messenger;
	private final Member me;
	//key为seqNo，value为Proposal
	private final Map<Long, Proposal> proposals = new HashMap<Long, Proposal>();
	//发送超过一半以上成功的消息，key为seqNo，value为message
	private final Map<Long, Serializable> successfulMessages = new HashMap<Long, Serializable>();
	//发送超过一半以上成功的消息，key为seqNo，value为msgId
	private final Map<Long, Long> successfulMsgIds = new HashMap<Long, Long>();
	//表示已经广播的消息id
	private final HashSet<Long> messagesCirculating = new HashSet<Long>(); // msgIds of messages that were not
	@SuppressWarnings("rawtypes")
	private final List<MultiRequest> assistants = new LinkedList<MultiRequest>();

	private long viewNumber = 0;
	//提案编号，全局递增不重复
	private long seqNo = 0;
	private boolean iAmElected = false;
	private long time;

	public LeaderRole(GroupMembership membership, CommLayer commLayer, long time)
	{
		this.membership = membership;
		this.messenger = commLayer;
		this.time = time;
		this.me = membership.getUID();
		//系统默认选举一个leader
		Member leader = PaxosUtils.selectLeader(membership.getMembers());
		if (leader.equals(me))
		{
			//如果leader角色初始化时，选择了自身节点作为leader，则准备一条 Election 消息，准备选举，每一次选举都创建新的纪元(年号)
			assistants.add(new Election(membership, messenger, time, viewNumber + newViewNumber()));
		}
	}

	@SuppressWarnings("rawtypes")
	public synchronized void dispatch(Serializable message)
	{
		if (message instanceof SpecialMessage)
		{
			SpecialMessage specialMessage = (SpecialMessage) message;
			switch (specialMessage.getMessageType())
			{
				case ABORT:
					onAbort((Abort) specialMessage);
					break;
				//来自Acceptor的广播请求
				case BROADCAST_REQ:
					onBroadcastRequest((BroadcastRequest) specialMessage);
					break;
				//选举投票
				case NEW_VIEW:
					onNewView((NewView) specialMessage);
					break;
			}
		}
		else if (message instanceof Tick)
		{
			//更新自己的时钟，并将assistance队列中的 MultiRequest消息发送出去
			update((Tick) message);
		}
		//把接收到响应（ViewAccepted、Accepted、SuccessAck）交给各个助理进行统计票数，是否达到一半以上等
		for (MultiRequest assistant : new ArrayList<MultiRequest>(assistants))
		{
			//如果是Tick消息，assistant更新了两次？？？
			assistant.receive(message);
			if (assistant.isFinished())
			{
				assistants.remove(assistant);
			}
		}
	}

	/**
	 * 1.更新自身时钟
	 * 2.检查 assistants 是否有需要发送的 MultiRequest 消息
	 */
	@SuppressWarnings("rawtypes")
	public synchronized void update(Tick tick)
	{
		this.time = tick.time;
		for (MultiRequest assistant : assistants)
		{
			//超过1s，向未收到回复的节点重发消息
			assistant.tick(tick.time);
		}
	}

	/**
	 * 收到自身的 Election 消息则忽略，收到其他节点的 Election 消息，如果viewNumber比自己大，则更新自身的viewNumber
	 */
	private void onNewView(NewView msg)
	{
		if (msg.viewNumber > this.viewNumber)
		{
			this.viewNumber = msg.viewNumber;
			if (!msg.leader.equals(me))
			{
				this.iAmElected = false;
			}
		}
	}

	private void onAbort(Abort abort)
	{
		abortBallot(abort.seqNo);
	}

	/**
	 * 向Acceptor补发所丢失的事务
	 */
	private void sendMissingSuccessMessages(Set<Long> missingSuccess, Member sender)
	{
		for (Long seqNo : missingSuccess)
		{
			if (successfulMessages.containsKey(seqNo))
			{
				Success message = new Success(seqNo, successfulMessages.get(seqNo), successfulMsgIds.get(seqNo));
				messenger.sendTo(sender, PaxosUtils.serialize(message));
			}
		}
	}

	/**
	 * 只有leader才可以发送广播消息，创建proposal，然后交给助理进行广播
	 * @param req 需要广播的消息
	 */
	private void onBroadcastRequest(BroadcastRequest req)
	{
		if (iAmElected)
		{
			//是否已经发送
			if (messagesCirculating.contains(req.msgId))
			{
				return;
			}
			messagesCirculating.add(req.msgId);
			//一条消息对应一个Proposal，创建一个提案，放入proposals中，seqNo递增
			createProposal(++seqNo, req.message, req.msgId);
			//交给助理负责发送
			assistants.add(new MultiAccept(membership, messenger, seqNo, req.message, req.msgId));
		}
		else
		{
			System.out.println("I am not the leader");
		}
	}

	/**
	 * 年号（viewNumber）增大一轮
	 */
	private long newViewNumber()
	{
		int groupSize = membership.groupSize();
		long previousBallot = viewNumber / groupSize;
		viewNumber = (previousBallot + 1) * groupSize + membership.getPositionInGroup();
		return viewNumber;
	}

	/**
	 * 监听leader节点，当有节点下线，就会试图参与选举一次
	 * @param failedMember  下线节点
	 * @param aliveMembers  活着的节点集合
	 */
	public void memberFailed(Member failedMember, Set<Member> aliveMembers)
	{
		if (me.equals(PaxosUtils.selectLeader(aliveMembers)))
		{
			System.out.println(me + ": taking leadership");
			assistants.add(new Election(membership, messenger, time, newViewNumber()));
		}
	}

	/**
	 * 记录其他节点已经接受到的proposal
	 */
	private void registerViewAcceptance(ViewAccepted viewAccepted)
	{
		for (Long seqNo : viewAccepted.accepted.keySet())
		{
			Acceptance acceptance = viewAccepted.accepted.get(seqNo);
			Proposal proposal = proposals.get(seqNo);
			if (proposal == null)
			{
				proposals.put(seqNo, new Proposal(acceptance.viewNumber, acceptance.message, acceptance.msgId));
			}
			else
			{
				//判断是否需要更新提案的消息（acceptance.viewNumber>this.viewNumber?）
				proposal.acceptOutcome(acceptance.viewNumber, acceptance.message, acceptance.msgId);
			}
		}
	}

	/**
	 * 创建proposal，并存储在集合中
	 */
	private void createProposal(long seqNo, Serializable message, long msgId)
	{
		proposals.put(seqNo, new Proposal(viewNumber, message, msgId));
	}

	private void registerAcceptance(long viewNo, long seqNo, long msgId)
	{
		proposals.get(seqNo).acceptDefault(viewNo, msgId);
	}

	private void abortBallot(long seqNo)
	{
		proposals.remove(seqNo);
	}

	private class Election extends MultiRequest<NewView, ViewAccepted>
	{
		//选举年号
		private final long viewNumber;

		public Election(GroupMembership membership, CommLayer messenger, long time, long viewNumber)
		{
			super(membership, messenger, new NewView(me, viewNumber), time);
			this.viewNumber = viewNumber;
		}

		/**
		 * 过滤出投自己票的 ViewAccepted 消息，ViewAccepted.viewNumber 要与自己的 viewNumber 一致
		 */
		@Override
		protected ViewAccepted filterResponse(Serializable message)
		{
			if (message instanceof ViewAccepted)
			{
				ViewAccepted viewAccepted = (ViewAccepted) message;
				//不同意自己当选leader，直接丢弃该消息
				if (viewAccepted.viewNumber != viewNumber)
				{
					return null;
				}
				//记录来其他节点中已经接收到的提案
				registerViewAcceptance(viewAccepted);
				return viewAccepted;
			}
			else
			{
				return null;
			}
		}

		/**
		 * 超过半数投票支持，自己被选为leader
		 * 新官上任，要执行一些动作？？？？？？？
		 */
		@Override
		protected void onQuorumReached()
		{
			System.out.println(me + ": I am the leader");
			iAmElected = true;

			//此时proposals已经汇集了半数以上节点的提案，并且提案具有最近的数据，然后leader将这些提案发送给所有节点进行数据同步
			for (Long seqNo : proposals.keySet())
			{
				Proposal proposal = proposals.get(seqNo);
				if (proposal != null)
				{
					//提案中最新的数据
					Serializable choice = proposal.newestOutcome;
					long msgId = proposal.getMsgId();
					messagesCirculating.add(msgId);
					assistants.add(new MultiAccept(membership, messenger, seqNo, choice, msgId));
				}
			}
			//更新seqNo，保证leader的seqNo是最大的
			LeaderRole.this.seqNo = PaxosUtils.findMax(proposals.keySet());
			for (long seqNo = 1; seqNo < LeaderRole.this.seqNo; seqNo++)
			{
				if (!proposals.containsKey(seqNo))
				{
					createProposal(seqNo, NO_OP, 0l);
					assistants.add(new MultiAccept(membership, messenger, seqNo, NO_OP, 0l));
				}
			}
		}
	}

	/**
	 * 每一个 MultiAccept 负责发送一个 Accept,一个 Accept 对应一条消息
	 */
	private class MultiAccept extends MultiRequest<Accept, Accepted>
	{
		private final long seqNo;
		private final Serializable message;
		private final long msgId;

		public MultiAccept(GroupMembership membership, CommLayer messenger, long seqNo, Serializable message, long msgId)
		{
			super(membership, messenger, new Accept(viewNumber, seqNo, message, msgId, me), time);
			this.seqNo = seqNo;
			this.message = message;
			this.msgId = msgId;
		}

		@Override
		protected Accepted filterResponse(Serializable message)
		{
			if (message instanceof Accepted)
			{
				Accepted accepted = (Accepted) message;
				//viewNumber与leader不同或者提案ID不同，则抛弃
				if (accepted.viewNo != viewNumber || accepted.seqNo != seqNo)
				{
					return null;
				}
				registerAcceptance(accepted.viewNo, accepted.seqNo, accepted.msgId);
				//补发余粮
				sendMissingSuccessMessages(accepted.missingSuccess, accepted.sender);
				return accepted;
			}
			else
			{
				return null;
			}
		}

		@Override
		protected void onQuorumReached()
		{
			successfulMessages.put(seqNo, message);
			successfulMsgIds.put(seqNo, msgId);
			assistants.add(new MultiSuccess(membership, messenger, seqNo, message, msgId));
		}
	}

	private class MultiSuccess extends MultiRequest<Success, SuccessAck>
	{
		private final long seqNo;
		private final long msgId;

		public MultiSuccess(GroupMembership membership, CommLayer messenger, long seqNo, Serializable msg, long msgId)
		{
			super(membership, messenger, new Success(seqNo, msg, msgId), time);
			this.seqNo = seqNo;
			this.msgId = msgId;
		}

		@Override
		protected SuccessAck filterResponse(Serializable message)
		{
			if (message instanceof SuccessAck)
			{
				SuccessAck successAck = (SuccessAck) message;
				return (msgId != successAck.getMsgId()) ? null : successAck;
			}
			else
			{
				return null;
			}
		}

		@Override
		protected void onCompleted()
		{
			successfulMessages.remove(seqNo);
			successfulMsgIds.remove(msgId);
			messagesCirculating.remove(msgId);
			finish();
		}
	}
}
