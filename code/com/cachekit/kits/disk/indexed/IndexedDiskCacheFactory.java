package com.cachekit.kits.disk.indexed;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.logger.ICacheEventWrapper;
import com.cachekit.core.model.IContextCacheManager;
import com.cachekit.core.model.IElementSerializer;
import com.cachekit.kits.AbstractKitCacheFactory;
import com.cachekit.kits.KitCacheAttributes;

public class IndexedDiskCacheFactory extends AbstractKitCacheFactory
{
	private static final Log log = LogFactory.getLog(IndexedDiskCacheFactory.class);

	@Override
	public <K, V> IndexedDiskCache<K, V> createCache(KitCacheAttributes cattr, IContextCacheManager cacheMgr,
			ICacheEventWrapper cacheEventWrapper, IElementSerializer elementSerializer)
	{
		IndexedDiskCacheAttributes idcattr = (IndexedDiskCacheAttributes) cattr;

		if (log.isDebugEnabled())
		{
			log.debug("Create DiskCache for attributes = " + idcattr);
		}

		IndexedDiskCache<K, V> cache = new IndexedDiskCache<K, V>(idcattr, elementSerializer);

		cache.setCacheEventLogger(cacheEventWrapper);

		return cache;
	}
}
