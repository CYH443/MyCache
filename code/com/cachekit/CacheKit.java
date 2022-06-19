package com.cachekit;

import java.util.Properties;

import com.cachekit.access.CacheKitAccess;
import com.cachekit.access.GroupCacheKitAccess;
import com.cachekit.access.exception.CacheException;
import com.cachekit.core.control.ContextCache;
import com.cachekit.core.control.ContextCacheManager;
import com.cachekit.core.control.group.GroupAttrName;
import com.cachekit.core.model.IContextCacheAttributes;
import com.cachekit.core.model.IElementAttributes;

public abstract class CacheKit
{
	private static String configFilename = null;

	private static Properties configProps = null;

	private static ContextCacheManager cacheMgr;

	public static void setConfigFilename(String configFilename)
	{
		CacheKit.configFilename = configFilename;
	}

	public static void setConfigProperties(Properties configProps)
	{
		CacheKit.configProps = configProps;
	}

	public static void shutdown()
	{
		synchronized (CacheKit.class)
		{
			if (cacheMgr != null && cacheMgr.isInitialized())
			{
				cacheMgr.shutDown();
			}

			cacheMgr = null;
		}
	}

	private static ContextCacheManager getCacheManager() throws CacheException
	{
		synchronized (CacheKit.class)
		{
			if (cacheMgr == null || !cacheMgr.isInitialized())
			{
				if (configProps != null)
				{
					cacheMgr = ContextCacheManager.getUnconfiguredInstance();
					cacheMgr.configure(configProps);
				}
				else if (configFilename != null)
				{
					cacheMgr = ContextCacheManager.getUnconfiguredInstance();
					cacheMgr.configure(configFilename);
				}
				else
				{
					cacheMgr = ContextCacheManager.getInstance();
				}
			}

			return cacheMgr;
		}
	}

	public static <K, V> CacheKitAccess<K, V> getInstance(String cacheName) throws CacheException
	{
		ContextCache<K, V> cache = getCacheManager().getCache(cacheName);
		return new CacheKitAccess<K, V>(cache);
	}

	public static <K, V> CacheKitAccess<K, V> getInstance(String cacheName, IContextCacheAttributes icca)
			throws CacheException
	{
		ContextCache<K, V> cache = getCacheManager().getCache(cacheName, icca);
		return new CacheKitAccess<K, V>(cache);
	}

	public static <K, V> CacheKitAccess<K, V> getInstance(String cacheName, IContextCacheAttributes icca,
			IElementAttributes eattr) throws CacheException
	{
		ContextCache<K, V> cache = getCacheManager().getCache(cacheName, icca, eattr);
		return new CacheKitAccess<K, V>(cache);
	}

	public static <K, V> GroupCacheKitAccess<K, V> getGroupCacheInstance(String cacheName) throws CacheException
	{
		ContextCache<GroupAttrName<K>, V> cache = getCacheManager().getCache(cacheName);
		return new GroupCacheKitAccess<K, V>(cache);
	}

	public static <K, V> GroupCacheKitAccess<K, V> getGroupCacheInstance(String cacheName,
			IContextCacheAttributes icca) throws CacheException
	{
		ContextCache<GroupAttrName<K>, V> cache = getCacheManager().getCache(cacheName, icca);
		return new GroupCacheKitAccess<K, V>(cache);
	}

	public static <K, V> GroupCacheKitAccess<K, V> getGroupCacheInstance(String cacheName,
			IContextCacheAttributes icca, IElementAttributes eattr) throws CacheException
	{
		ContextCache<GroupAttrName<K>, V> cache = getCacheManager().getCache(cacheName, icca, eattr);
		return new GroupCacheKitAccess<K, V>(cache);
	}
}
