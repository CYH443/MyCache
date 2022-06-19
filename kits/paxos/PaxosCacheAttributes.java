package com.cachekit.kits.paxos;

import com.cachekit.kits.AbstractKitCacheAttributes;

public class PaxosCacheAttributes extends AbstractKitCacheAttributes
{
	private static final long serialVersionUID = 1L;

	private String servers;

	private String myPosition;

	public String getMyPosition()
	{
		return myPosition;
	}

	public void setMyPosition(String myPosition)
	{
		this.myPosition = myPosition;
	}

	public String getServers()
	{
		return servers;
	}

	public void setServers(String servers)
	{
		this.servers = servers;
	}

}
