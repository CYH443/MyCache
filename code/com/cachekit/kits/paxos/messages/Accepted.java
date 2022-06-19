package com.cachekit.kits.paxos.messages;

import java.util.Set;

import com.cachekit.kits.paxos.comm.Member;

/**
 * Accepted有成员发出，告知leader，消息已经接受
 */
public class Accepted implements SpecialMessage, MessageWithSender
{
	private static final long serialVersionUID = 1L;
	//leader的viewNumber
	public long viewNo;
	public long seqNo;
	public long msgId;
	//存储的是seqNo
	public Set<Long> missingSuccess;
	public Member sender;

	public Accepted(long viewNo, long seqNo, long msgId, Set<Long> missingSuccess, Member me)
	{
		this.viewNo = viewNo;
		this.seqNo = seqNo;
		this.msgId = msgId;
		this.missingSuccess = missingSuccess;
		sender = me;
	}

	public MessageType getMessageType()
	{
		return MessageType.ACCEPTED;
	}

	@Override
	public String toString()
	{
		return "ACCEPTED " + msgId + " missing(" + missingSuccess + ") from " + sender;
	}

	public Member getSender()
	{
		return sender;
	}
}
