package com.cachekit.kits.paxos;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.cachekit.kits.paxos.comm.CommLayer;
import com.cachekit.kits.paxos.comm.Member;
import com.cachekit.kits.paxos.messages.Abort;
import com.cachekit.kits.paxos.messages.Accept;
import com.cachekit.kits.paxos.messages.Accepted;
import com.cachekit.kits.paxos.messages.BroadcastRequest;
import com.cachekit.kits.paxos.messages.NewView;
import com.cachekit.kits.paxos.messages.SpecialMessage;
import com.cachekit.kits.paxos.messages.Success;
import com.cachekit.kits.paxos.messages.SuccessAck;
import com.cachekit.kits.paxos.messages.ViewAccepted;

public class AcceptorRole
{
	public static final long MAX_CIRCULATING_MESSAGES = 1000000l;
	private final GroupMembership membership;
	//UDPMessenger信使
	private final CommLayer messenger;
	private final BufferedReceiver receiver;
	private final Member me;
	private final WaitingTimer waitingForResponse = new WaitingTimer();
	private final int myPositionInGroup;
	//存储接收到的事务，key为seqNo
	Map<Long, Acceptance> accepted = new HashMap<Long, Acceptance>(); // what we accepted for each seqNo
	private Member leader;
	private long viewNumber;
	private MissingMessagesTracker missing = new MissingMessagesTracker(); // missing SUCCESS messages
	private AtomicLong msgIdGenerator = new AtomicLong(0);

	public AcceptorRole(GroupMembership membership, CommLayer messenger, Receiver receiver)
	{
		this.membership = membership;
		this.messenger = messenger;
		this.receiver = new BufferedReceiver(receiver);
		this.me = membership.getUID();
		this.myPositionInGroup = membership.getPositionInGroup();
		this.leader = me;
	}

	/**
	 * PaxosCache接收到消息，驱动AcceptorRole消息广播给leader去处理
	 * @param message CacheElementPaxos
	 */
	public void broadcast(Serializable message)
	{
		long msgId = createMsgId(message);
		boolean broadcastSuccessful = false;
		try
		{
			//向leader发送消息,加锁，只能单线程广播
			while (!broadcastSuccessful)
			{
				messenger.sendTo(leader, PaxosUtils.serialize(new BroadcastRequest(message, msgId)));
				broadcastSuccessful = waitingForResponse.waitALittle(msgId);
			}
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}

	private long createMsgId(Serializable message)
	{
		return myPositionInGroup * MAX_CIRCULATING_MESSAGES + msgIdGenerator.incrementAndGet() % MAX_CIRCULATING_MESSAGES;
	}

	public void dispatch(Serializable message)
	{
		if (message instanceof SpecialMessage)
		{
			SpecialMessage specialMessage = (SpecialMessage) message;
			switch (specialMessage.getMessageType())
			{
				case NEW_VIEW:
					onNewView((NewView) specialMessage);
					break;
				case ACCEPT:
					onAccept((Accept) specialMessage);
					break;
				case SUCCESS:
					onSuccess((Success) specialMessage);
					break;
			}
		}
	}

	/**
	 * 更新viewNumber和leader，并发送ViewAccepted消息
	 */
	private void onNewView(NewView newView)
	{
		if (newView.viewNumber > viewNumber)
		{
			System.out.println(me + ": setting leader to " + newView.leader);
			this.leader = newView.leader;
			this.viewNumber = newView.viewNumber;
			messenger.sendTo(leader, PaxosUtils.serialize(new ViewAccepted(viewNumber, accepted, me)));
		}
		else if (newView.viewNumber == viewNumber && newView.leader.equals(leader))
		{
			messenger.sendTo(leader, PaxosUtils.serialize(new ViewAccepted(viewNumber, accepted, me)));
		}
	}

	/**
	 * 接收leader的消息，保存到 accepted 哈希表中，并返回leader一个 Accepted 响应，Accepted包括其到 accept.seqNo 为止所丢失的 seqNo
	 */
	private void onAccept(Accept accept)
	{
		if (accept.viewNo < viewNumber)
		{
			messenger.sendTo(accept.sender, PaxosUtils.serialize(new Abort(accept.viewNo, accept.seqNo)));
		}
		else
		{
			//保存leader发送过来的消息
			accepted.put(accept.seqNo, new Acceptance(accept.viewNo, accept.message, accept.msgId));
			//给leader返回 Accepted 响应，向leader汇报自己丢失的seqNo
			messenger.sendTo(accept.sender, PaxosUtils.serialize(new Accepted(accept.viewNo, accept.seqNo, accept.msgId, missing.getMissing(accept.seqNo), me)));
		}
	}

	private void onSuccess(Success success)
	{
		receiver.receive(success.seqNo, success.message);
		missing.received(success.seqNo);
		waitingForResponse.unblock(success.msgId);
		messenger.sendTo(leader, PaxosUtils.serialize(new SuccessAck(success.msgId, me)));
	}
}
