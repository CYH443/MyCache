package com.cachekit.kits;

import com.cachekit.core.logger.CacheEvent;
import com.cachekit.core.logger.ICacheEvent;
import com.cachekit.core.logger.ICacheEventWrapper;
import com.cachekit.core.match.IKeyMatcher;
import com.cachekit.core.match.KeyMatcher;
import com.cachekit.core.model.ICacheElement;
import com.cachekit.core.model.IElementSerializer;
import com.cachekit.utils.serialization.StandardSerializer;

public abstract class AbstractKitCache<K, V> implements KitCache<K, V> {
    //缓存事件日志处理器
    private ICacheEventWrapper cacheEventWrapper;
    //序列化器
    private IElementSerializer elementSerializer = new StandardSerializer();
    //模式匹配器
    private IKeyMatcher<K> keyMatcher = new KeyMatcher<K>();

    protected ICacheEvent<K> createICacheEvent(ICacheElement<K, V> item, String eventName)
    {
        if (cacheEventWrapper == null)
        {
            return new CacheEvent<K>();
        }

        String diskLocation = getEventLoggerExtraInfo();
        String cacheName = null;
        K key = null;

        if (item != null)
        {
            cacheName = item.getCacheName();
            key = item.getKey();
        }

        return cacheEventWrapper.createICacheEvent(getKitCacheAttributes().getName(), cacheName, eventName, diskLocation, key);
    }

    protected <T> ICacheEvent<T> createICacheEvent(String cacheName, T key, String eventName)
    {
        if (cacheEventWrapper == null)
        {
            return new CacheEvent<T>();
        }

        String diskLocation = getEventLoggerExtraInfo();
        return cacheEventWrapper.createICacheEvent(getKitCacheAttributes().getName(), cacheName, eventName, diskLocation, key);
    }

    protected <T> void cacheEventLogger(ICacheEvent<T> cacheEvent)
    {
        if (cacheEventWrapper != null)
        {
            //log.debug(cacheEvent)
            cacheEventWrapper.cacheEventLogger(cacheEvent);
        }
    }

    protected void applicationEventLogger(String source, String eventName, String optionalDetails)
    {
        if (cacheEventWrapper != null)
        {
            cacheEventWrapper.applicationEventLogger(source, eventName, optionalDetails);
        }
    }

    protected void errorLogger(String source, String eventName, String errorMessage)
    {
        if (cacheEventWrapper != null)
        {
            cacheEventWrapper.errorLogger(source, eventName, errorMessage);
        }
    }

    public abstract String getEventLoggerExtraInfo();

    @Override
    public void setCacheEventLogger(ICacheEventWrapper cacheEventWrapper)
    {
        this.cacheEventWrapper = cacheEventWrapper;
    }

    public ICacheEventWrapper getCacheEventLogger() {
        return this.cacheEventWrapper;
    }

    @Override
    public void setElementSerializer(IElementSerializer elementSerializer)
    {
        if (elementSerializer != null)
        {
            this.elementSerializer = elementSerializer;
        }
    }

    public IElementSerializer getElementSerializer() {
        return this.elementSerializer;
    }

    @Override
    public void setKeyMatcher(IKeyMatcher<K> keyMatcher)
    {
        if (keyMatcher != null)
        {
            this.keyMatcher = keyMatcher;
        }
    }

    public IKeyMatcher<K> getKeyMatcher() {
        return this.keyMatcher;
    }
}
