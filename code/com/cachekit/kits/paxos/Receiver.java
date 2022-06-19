package com.cachekit.kits.paxos;

import java.io.Serializable;

public interface Receiver
{
	void receive(Serializable message);
}
