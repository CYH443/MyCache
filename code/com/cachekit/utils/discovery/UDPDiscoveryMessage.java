package com.cachekit.utils.discovery;

import java.io.Serializable;
import java.util.ArrayList;

public class UDPDiscoveryMessage implements Serializable
{
	private static final long serialVersionUID = 1L;

	public enum BroadcastType
	{
		//服务组被动广播
		PASSIVE,
		//调用方请求服务组发送 PASSIVE 广播
		REQUEST,
		//服务组下线或故障，发送的删除报文
		REMOVE
	}

	private BroadcastType messageType = BroadcastType.PASSIVE;
	//发送方port
	private int port = 6789;
	//发送方ip
	private String host = "224.0.0.1";

	private long requesterId;

	private ArrayList<String> cacheNames = new ArrayList<String>();

	public void setPort(int port)
	{
		this.port = port;
	}

	public int getPort()
	{
		return port;
	}

	public void setHost(String host)
	{
		this.host = host;
	}

	public String getHost()
	{
		return host;
	}

	public void setRequesterId(long requesterId)
	{
		this.requesterId = requesterId;
	}

	public long getRequesterId()
	{
		return requesterId;
	}

	public void setMessageType(BroadcastType messageType)
	{
		this.messageType = messageType;
	}

	public BroadcastType getMessageType()
	{
		return messageType;
	}

	public void setCacheNames(ArrayList<String> cacheNames)
	{
		this.cacheNames = cacheNames;
	}

	public ArrayList<String> getCacheNames()
	{
		return cacheNames;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("\n host = [" + host + "]");
		sb.append("\n port = [" + port + "]");
		sb.append("\n requesterId = [" + requesterId + "]");
		sb.append("\n messageType = [" + messageType + "]");
		sb.append("\n Cache Names");
		for (String name : cacheNames)
		{
			sb.append(" cacheName = [" + name + "]");
		}
		return sb.toString();
	}
}
