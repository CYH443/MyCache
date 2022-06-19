package com.cachekit.access;

import com.cachekit.access.exception.CacheException;
import com.cachekit.core.model.ICacheElement;
import com.cachekit.core.model.IElementAttributes;

import java.util.Map;
import java.util.Set;

public interface ICacheKitAccess<K, V> extends ICacheKitAccessManagement
{
	V get(K name);

	Map<K, V> getMatching(String pattern);

	void putSafe(K name, V obj) throws CacheException;

	void put(K name, V obj) throws CacheException;

	void put(K name, V obj, IElementAttributes attr) throws CacheException;

	ICacheElement<K, V> getCacheElement(K name);

	Map<K, ICacheElement<K, V>> getCacheElements(Set<K> names);

	Map<K, ICacheElement<K, V>> getMatchingCacheElements(String pattern);

	void remove(K name) throws CacheException;

	void resetElementAttributes(K name, IElementAttributes attributes) throws CacheException;

	IElementAttributes getElementAttributes(K name) throws CacheException;
}
