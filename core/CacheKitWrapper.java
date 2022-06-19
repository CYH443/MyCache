package com.cachekit.core;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.model.ICache;
import com.cachekit.core.model.ICacheElement;
import com.cachekit.core.model.ICacheListener;

/**
 * 将 LaterCache 注入 CacheKitWrapper 中
 */
public class CacheKitWrapper<K, V> implements ICacheListener<K, V>
{
	private static final Log log = LogFactory.getLog(CacheKitWrapper.class);
	//cache为 LaterCache
	private final ICache<K, V> cache;

	private long listenerId = 0;

	@Override
	public void setListenerId(long id) throws IOException
	{
		this.listenerId = id;
		log.debug("listenerId = " + id);
	}

	@Override
	public long getListenerId() throws IOException
	{
		return this.listenerId;
	}

	public CacheKitWrapper(ICache<K, V> cache)
	{
		this.cache = cache;
	}

	@Override
	public void handlePut(ICacheElement<K, V> item) throws IOException
	{
		try
		{
			cache.update(item);
		}
		catch (Exception e)
		{

		}
	}

	@Override
	public void handleRemove(String cacheName, K key) throws IOException
	{
		cache.remove(key);
	}

	@Override
	public void handleRemoveAll(String cacheName) throws IOException
	{
		cache.removeAll();
	}

	@Override
	public void handleDispose(String cacheName) throws IOException
	{
		cache.dispose();
	}
}
