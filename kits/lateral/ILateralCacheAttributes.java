package com.cachekit.kits.lateral;

import com.cachekit.kits.KitCacheAttributes;

public interface ILateralCacheAttributes extends KitCacheAttributes
{
	enum Type
	{

		UDP,

		TCP
	}

	int DEFAULT_DAEMON_QUEUE_MAX_SIZE = 1000;

	void setUdpMulticastAddr(String multicastAddr);

	String getUdpMulticastAddr();

	void setUdpMulticastPort(int multicastPort);

	int getUdpMulticastPort();

	void setTransmissionType(Type type);

	Type getTransmissionType();

	void setTransmissionTypeName(String typeName);

	String getTransmissionTypeName();

	void setPutOnlyMode(boolean mode);

	boolean getPutOnlyMode();

	void setReceive(boolean receive);

	boolean isReceive();

	void setDaemonQueueMaxSize(int daemonQueueMaxSize);

	int getDaemonQueueMaxSize();
}
