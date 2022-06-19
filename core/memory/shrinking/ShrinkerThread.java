package com.cachekit.core.memory.shrinking;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.control.ContextCache;
import com.cachekit.core.control.event.ElementEventType;
import com.cachekit.core.memory.IMemoryCache;
import com.cachekit.core.model.ICacheElement;
import com.cachekit.core.model.IElementAttributes;
//ContextCache中的一个线程任务，负责每30s检查一次内存
public class ShrinkerThread<K, V> implements Runnable {
    private static final Log log = LogFactory.getLog(ShrinkerThread.class);

    private final ContextCache<K, V> cache;
    //内存中元素的最大空转时间
    private final long maxMemoryIdleTime;
    //一次任务中最大刷盘数量
    private final int maxSpoolPerRun;
    //刷盘限制
    private boolean spoolLimit = false;

    public ShrinkerThread(ContextCache<K, V> cache)
    {
        super();

        this.cache = cache;
        //系统默认120分钟
        long maxMemoryIdleTimeSeconds = cache.getCacheAttributes().getMaxMemoryIdleTimeSeconds();

        if (maxMemoryIdleTimeSeconds < 0)
        {
            this.maxMemoryIdleTime = -1;
        }
        else
        {
            this.maxMemoryIdleTime = maxMemoryIdleTimeSeconds * 1000;
        }

        this.maxSpoolPerRun = cache.getCacheAttributes().getMaxSpoolPerRun();
        if (this.maxSpoolPerRun != -1)
        {
            this.spoolLimit = true;
        }
    }

    @Override
    public void run() {
        shrink();
    }

    protected void shrink()
    {
        if (log.isDebugEnabled())
        {
            log.debug("Shrink memory cache for: " + cache.getCacheName());
        }
        IMemoryCache<K, V> memCache = cache.getMemoryCache();

        try
        {
            Set<K> keys = memCache.getKeySet();
            int size = keys.size();

            if (log.isDebugEnabled())
            {
                log.debug("Keys size: " + size);
            }

            ICacheElement<K, V> cacheElement;
            IElementAttributes attributes;

            int spoolCount = 0;

            for (K key : keys)
            {
                cacheElement = memCache.getQuiet(key);

                if (cacheElement == null)
                {
                    continue;
                }

                attributes = cacheElement.getElementAttributes();

                boolean remove = false;
                long now = System.currentTimeMillis();

                //判断该元素是否永生
                if (!cacheElement.getElementAttributes().getIsEternal())
                {
                    //判断元素是否过期：超过最大生存时间 或者 超过最大闲置时间
                    remove = cache.isExpired(
                            cacheElement, now,
                            ElementEventType.EXCEEDED_MAXLIFE_BACKGROUND,
                            ElementEventType.EXCEEDED_IDLETIME_BACKGROUND);

                    if (remove)
                    {
                        memCache.remove(cacheElement.getKey());
                    }
                }
                //判断元素闲置时间是否超过ContextCache的闲置时间最大限制
                if (!remove && maxMemoryIdleTime != -1)
                {
                    if (!spoolLimit || spoolCount < this.maxSpoolPerRun)
                    {
                        final long lastAccessTime = attributes.getLastAccessTime();

                        if (lastAccessTime + maxMemoryIdleTime < now)
                        {
                            if (log.isDebugEnabled())
                            {
                                log.debug("Exceed memory idle time: " + cacheElement.getKey());
                            }

                            spoolCount++;
                            memCache.remove(cacheElement.getKey());
                            memCache.waterfal(cacheElement);

                            key = null;
                            cacheElement = null;
                        }
                    }
                    else
                    {
                        if (log.isDebugEnabled())
                        {
                            log.debug("spoolCount = '" + spoolCount + "'; " + "maxSpoolPerRun = '" + maxSpoolPerRun + "'");
                        }
                        if (spoolLimit && spoolCount >= this.maxSpoolPerRun)
                        {
                            return;
                        }
                    }
                }
            }
        }
        catch (Throwable t)
        {
            log.info("Error occur in shrink", t);
            return;
        }
    }
}
