package com.cachekit.kits.paxos;

import java.io.Serializable;

public class Proposal
{
	//广播的消息
	Serializable proposedMessage;
	//广播的消息ID
	long newestMsgId;
	//leader的年号viewNumber
	long newestView;

	Serializable newestOutcome;
	Serializable choice;

	Proposal(long viewNo, Serializable proposedMessage, long msgId)
	{
		this.proposedMessage = proposedMessage;
		this.newestView = viewNo;
		this.newestOutcome = proposedMessage;
		this.newestMsgId = msgId;
	}

	public Serializable getChoice()
	{
		return choice;
	}

	public void acceptDefault(long viewNo, long msgId)
	{
		acceptOutcome(viewNo, proposedMessage, msgId);
	}

	/**
	 * 接受其他节点已经提交过的proposal
	 */
	public void acceptOutcome(long viewNo, Serializable outcome, long msgId)
	{
		if (viewNo > newestView)
		{
			newestView = viewNo;
			newestOutcome = outcome;
			newestMsgId = msgId;
		}
	}

	public long getMsgId()
	{
		return newestMsgId;
	}
}
