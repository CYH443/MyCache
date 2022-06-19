package com.cachekit.core.model;

import java.io.IOException;

import com.cachekit.core.stats.IStats;

public interface ICacheEventQueue<K, V>
{
	enum QueueType
	{
		//单一线程的事件队列
		SINGLE,
		//线程池的事件队列
		POOLED
	}

	QueueType getQueueType();

	void addPutEvent(ICacheElement<K, V> ce) throws IOException;

	void addRemoveEvent(K key) throws IOException;

	void addRemoveAllEvent() throws IOException;

	void addDisposeEvent() throws IOException;

	long getListenerId();

	void destroy();

	boolean isAlive();

	boolean isWorking();

	int size();

	boolean isEmpty();

	IStats getStatistics();
}
