package com.cachekit.kits.paxos;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.cachekit.kits.paxos.comm.CommLayer;
import com.cachekit.kits.paxos.comm.Member;
import com.cachekit.kits.paxos.comm.Tick;
import com.cachekit.kits.paxos.messages.MessageWithSender;

/**
 * 向其他节点发送消息，然后接收响应，存入map集合
 * @param <T> 发送消息类型
 * @param <R> 接收消息类型
 */
public abstract class MultiRequest<T extends Serializable, R extends MessageWithSender>
{
	public static final int RESEND_INTERVAL = 1000;
	private final GroupMembership membership;
	//UDPMessenger
	protected final CommLayer messenger;
	//待发送的消息
	protected final byte[] request;
	//接收到其他节点的响应
	protected Map<Member, R> responses = new HashMap<Member, R>();
	//上一次发送的时间
	private long lastResend = 0;
	//是否收到全部响应
	private boolean finished = false;
	//是否收到一半以上节点的响应
	private boolean quorumHasBeenReached = false;
	//是否全部接收到响应
	private boolean allMembersHaveReplied = false;

	public MultiRequest(GroupMembership membership, CommLayer messenger, T req, long time)
	{
		this.membership = membership;
		this.messenger = messenger;
		this.request = PaxosUtils.serialize(req);
		messenger.sendTo(membership.getMembers(), this.request);
		this.lastResend = time;
	}

	@SuppressWarnings("unchecked")
	protected R filterResponse(Serializable message)
	{
		try
		{
			return (R) message;
		}
		catch (ClassCastException e)
		{
			return null;
		}
	}

	protected void onQuorumReached()
	{
	}

	protected void onCompleted()
	{
		finish();
	}

	/**
	 * 由leader的Tick所驱动，超过 RESEND_INTERVAL 1s,向未收到回复的节点重发
	 * @param time Tick.time
	 */
	public void tick(long time)
	{
		if (time > lastResend + RESEND_INTERVAL)
		{
			resendRequests(time);
		}
	}

	protected boolean haveQuorum()
	{
		return responses.size() > membership.groupSize() / 2;
	}

	final protected void finish()
	{
		this.finished = true;
	}

	public boolean isFinished()
	{
		return finished;
	}

	/**
	 * 接收消息，统计投票
	 * @param message leader接收到的消息，发给助理进行统计处理
	 */
	final public void receive(Serializable message)
	{
		if (message instanceof Tick)
		{
			tick(((Tick) message).time);
		}

		R resp = filterResponse(message);
		if (resp != null)
		{
			responses.put(resp.getSender(), resp);
			if (haveQuorum() && !quorumHasBeenReached)
			{
				onQuorumReached();
				quorumHasBeenReached = true;
			}
			if (allMembersReplied() && !allMembersHaveReplied)
			{
				onCompleted();
				allMembersHaveReplied = true;
			}
		}
	}

	/**
	 * 由tick函数调用，向没有响应的节点重新发送
	 */
	protected void resendRequests(long time)
	{
		for (Member member : membership.getMembers())
		{
			if (!responses.containsKey(member))
			{
				messenger.sendTo(member, request);
			}
		}
		lastResend = time;
	}

	/**
	 * 判断 GroupMembership 里的节点是否全部响应
	 */
	protected boolean allMembersReplied()
	{
		return responses.size() == membership.groupSize();
	}
}
