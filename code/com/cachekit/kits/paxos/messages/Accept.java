package com.cachekit.kits.paxos.messages;

import java.io.Serializable;

import com.cachekit.kits.paxos.comm.Member;

/**
 * Accept由当选的leader发出，要求成员接受消息
 */
public class Accept implements SpecialMessage
{
	private static final long serialVersionUID = 1L;
	//leader的viewNumber
	public long viewNo;
	//提案ID
	public long seqNo;
	//广播的消息
	public Serializable message;
	public long msgId;
	public final Member sender;

	public Accept(long viewNo, long seqNo, Serializable message, long msgId, Member sender)
	{
		this.viewNo = viewNo;
		this.seqNo = seqNo;
		this.message = message;
		this.msgId = msgId;
		this.sender = sender;
	}

	public MessageType getMessageType()
	{
		return MessageType.ACCEPT;
	}

	@Override
	public String toString()
	{
		return "ACCEPT " + seqNo + " " + msgId;
	}
}
