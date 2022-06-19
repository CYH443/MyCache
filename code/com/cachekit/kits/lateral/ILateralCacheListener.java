package com.cachekit.kits.lateral;

import com.cachekit.core.model.ICacheListener;
import com.cachekit.core.model.IContextCacheManager;

public interface ILateralCacheListener<K, V> extends ICacheListener<K, V>
{
	void init();

	void setCacheManager(IContextCacheManager cacheMgr);

	IContextCacheManager getCacheManager();

	void dispose();
}
