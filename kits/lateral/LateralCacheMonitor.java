package com.cachekit.kits.lateral;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.cachekit.core.CacheStatus;
import com.cachekit.core.DaemonCacheServiceRemote;
import com.cachekit.core.model.ICacheServiceRemote;
import com.cachekit.kits.AbstractKitCacheMonitor;
import com.cachekit.kits.lateral.tcp.ITCPLateralCacheAttributes;
import com.cachekit.kits.lateral.tcp.LateralTCPCacheFactory;

/**
 * 一个线程：检测 LateralCacheAsync 组件的错误并进行修正，没有错误该线程就会阻塞
 */
public class LateralCacheMonitor extends AbstractKitCacheMonitor
{
	private ConcurrentHashMap<String, LateralCacheAsync<?, ?>> caches;

	private LateralTCPCacheFactory factory;

	protected static void forceShortIdlePeriod(long idlePeriod)
	{
		LateralCacheMonitor.idlePeriod = idlePeriod;
	}

	public LateralCacheMonitor(LateralTCPCacheFactory factory)
	{
		super("CacheKitLateralCacheMonitor");
		this.factory = factory;
		this.caches = new ConcurrentHashMap<String, LateralCacheAsync<?, ?>>();
		setIdlePeriod(20000L);
	}

	public void addCache(LateralCacheAsync<?, ?> cache)
	{
		this.caches.put(cache.getCacheName(), cache);
		//线程状态
		if (this.getState() == Thread.State.NEW)
		{
			this.start();
		}
	}

	@Override
	public void dispose()
	{
		this.caches.clear();
	}

	@Override
	public void doWork()
	{

		for (Map.Entry<String, LateralCacheAsync<?, ?>> entry : caches.entrySet())
		{
			String cacheName = entry.getKey();

			@SuppressWarnings("unchecked")
			LateralCacheAsync<Object, Object> cache = (LateralCacheAsync<Object, Object>) entry.getValue();
			if (cache.getStatus() == CacheStatus.ERROR)
			{
				log.info("Find LateralCacheAsync in error, " + cacheName);

				ITCPLateralCacheAttributes attr = (ITCPLateralCacheAttributes) cache.getKitCacheAttributes();
				//创建新的 LaterTCPService
				ICacheServiceRemote<Object, Object> cacheService = factory.getCacheServiceRemoteInstance(attr);

				if (cacheService instanceof DaemonCacheServiceRemote)
				{
					continue;
				}

				cache.fixCache(cacheService);
			}
		}
	}
}
