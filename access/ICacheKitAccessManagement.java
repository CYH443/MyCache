package com.cachekit.access;

import com.cachekit.access.exception.CacheException;
import com.cachekit.core.model.IContextCacheAttributes;
import com.cachekit.core.model.IElementAttributes;
import com.cachekit.core.stats.ICacheStats;

public interface ICacheKitAccessManagement {
    void dispose();

    void clear() throws CacheException;
    //ElementAttributes
    IElementAttributes getDefaultElementAttributes() throws CacheException;

    void setDefaultElementAttributes(IElementAttributes attr) throws CacheException;
    //ContextCacheAttributes
    IContextCacheAttributes getCacheAttributes();

    void setCacheAttributes(IContextCacheAttributes cattr);

    int freeMemoryElements(int numberToFree) throws CacheException;

    ICacheStats getStatistics();

    String getStats();
}
