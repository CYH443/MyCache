package com.cachekit.kits.paxos;

import java.io.Serializable;

import com.cachekit.core.model.ICacheElement;

public class CacheElementPaxos implements Serializable
{

	private static final long serialVersionUID = 1L;

	private ICacheElement ce;

	private long requesterId;

	public CacheElementPaxos(ICacheElement ce, long requesterId)
	{
		this.ce = ce;
		this.requesterId = requesterId;
	}

	public ICacheElement getCacheElement()
	{
		return ce;
	}

	public long getRequesterId()
	{
		return requesterId;
	}
}
