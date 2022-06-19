package com.cachekit.kits;

import java.io.IOException;
import java.util.Set;

import com.cachekit.core.logger.ICacheEventWrapper;
import com.cachekit.core.model.ICache;
import com.cachekit.core.model.IElementSerializer;
import com.cachekit.core.stats.IStats;

/**
 * 缓存插件接口
 */

public interface KitCache<K, V> extends ICache<K, V>
{
	Set<K> getKeySet() throws IOException;

	IStats getStatistics();

	KitCacheAttributes getKitCacheAttributes();

	void setElementSerializer(IElementSerializer elementSerializer);

	void setCacheEventLogger(ICacheEventWrapper cacheEventWrapper);
}
