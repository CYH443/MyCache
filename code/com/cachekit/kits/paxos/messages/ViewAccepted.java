package com.cachekit.kits.paxos.messages;

import java.util.Map;

import com.cachekit.kits.paxos.Acceptance;
import com.cachekit.kits.paxos.comm.Member;

/**
 * 告知leader，提名投票已经收到，相当于同意当选的
 */
public class ViewAccepted implements SpecialMessage, MessageWithSender
{
	private static final long serialVersionUID = 1L;
	public final long viewNumber;
	//key 为 seqNo
	public final Map<Long, Acceptance> accepted;
	public final Member sender;

	public ViewAccepted(long viewNumber, Map<Long, Acceptance> accepted, Member sender)
	{
		this.viewNumber = viewNumber;
		this.accepted = accepted;
		this.sender = sender;
	}

	public MessageType getMessageType()
	{
		return MessageType.VIEW_ACCEPTED;
	}

	public Member getSender()
	{
		return sender;
	}

	@Override
	public String toString()
	{
		return "VIEW_ACCEPTED " + viewNumber + " " + sender;
	}
}
