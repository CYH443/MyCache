package com.cachekit.core.model;

import java.util.Properties;

import com.cachekit.core.control.ContextCache;
import com.cachekit.kits.KitCache;

public interface IContextCacheManager extends IShutdownObservable
{
	<K, V> ContextCache<K, V> getCache(String cacheName);

	<K, V> KitCache<K, V> getKitCache(String kitName, String cacheName);

	Properties getConfigurationProperties();

	String getStats();
}
