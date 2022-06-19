package com.cachekit.kits.paxos;

import com.cachekit.core.logger.ICacheEventWrapper;
import com.cachekit.core.model.IContextCacheManager;
import com.cachekit.core.model.IElementSerializer;
import com.cachekit.kits.AbstractKitCacheFactory;
import com.cachekit.kits.KitCache;
import com.cachekit.kits.KitCacheAttributes;

public class PaxosCacheFactory extends AbstractKitCacheFactory
{
	@Override
	public <K, V> KitCache<K, V> createCache(KitCacheAttributes attr, IContextCacheManager cacheMgr,
			ICacheEventWrapper cacheEventWrapper, IElementSerializer elementSerializer) throws Exception
	{
		PaxosCache paxos = new PaxosCache((PaxosCacheAttributes) attr);
		return paxos;
	}
}
