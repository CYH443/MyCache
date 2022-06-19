package com.cachekit.kits.paxos.messages;

import java.io.Serializable;

import com.cachekit.kits.paxos.comm.Member;

public interface MessageWithSender extends Serializable
{
	Member getSender();
}
