package com.cachekit.core;

import com.cachekit.core.model.ICacheListener;
import com.cachekit.core.model.ICacheObserver;
import com.cachekit.core.model.IDaemon;

public class DaemonCacheWatch implements ICacheObserver, IDaemon
{

	@Override
	public <K, V> void addCacheListener(String cacheName, ICacheListener<K, V> obj)
	{

	}

	@Override
	public <K, V> void addCacheListener(ICacheListener<K, V> obj)
	{

	}

	@Override
	public <K, V> void removeCacheListener(String cacheName, ICacheListener<K, V> obj)
	{

	}

	@Override
	public <K, V> void removeCacheListener(ICacheListener<K, V> obj)
	{

	}
}
