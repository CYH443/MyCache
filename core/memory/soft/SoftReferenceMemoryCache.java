package com.cachekit.core.memory.soft;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.CacheConstants;
import com.cachekit.core.control.ContextCache;
import com.cachekit.core.control.group.GroupAttrName;
import com.cachekit.core.memory.AbstractMemoryCache;
import com.cachekit.core.memory.util.MemoryElementDescriptor;
import com.cachekit.core.memory.util.SoftReferenceElementDescriptor;
import com.cachekit.core.model.ICacheElement;
import com.cachekit.core.stats.IStatElement;
import com.cachekit.core.stats.IStats;
import com.cachekit.core.stats.StatElement;

public class SoftReferenceMemoryCache<K, V> extends AbstractMemoryCache<K, V> {
    private static final Log log = LogFactory.getLog(SoftReferenceMemoryCache.class);

    private LinkedBlockingQueue<ICacheElement<K, V>> strongReferences;

    @Override
    public synchronized void initialize(ContextCache<K, V> hub)
    {
        super.initialize(hub);

        strongReferences = new LinkedBlockingQueue<ICacheElement<K, V>>();

        log.info("Initialized soft reference memory cache for " + getCacheName());
    }

    @Override
    public ConcurrentMap<K, MemoryElementDescriptor<K, V>> createMap()
    {
        return new ConcurrentHashMap<K, MemoryElementDescriptor<K, V>>();
    }

    @Override
    public Set<K> getKeySet()
    {
        Set<K> keys = new HashSet<K>();
        //考虑软引用被删除的情况
        for (Map.Entry<K, MemoryElementDescriptor<K, V>> e : map.entrySet())
        {
            SoftReferenceElementDescriptor<K, V> sred = (SoftReferenceElementDescriptor<K, V>) e.getValue();
            if (sred.getCacheElement() != null)
            {
                keys.add(e.getKey());
            }
        }
        return keys;
    }

    @Override
    public int getSize()
    {
        int size = 0;
        for (MemoryElementDescriptor<K, V> me : map.values())
        {
            SoftReferenceElementDescriptor<K, V> sred = (SoftReferenceElementDescriptor<K, V>) me;

            if (sred.getCacheElement() != null)
            {
                size++;
            }
        }
        return size;
    }

    @Override
    public IStats getStatistics()
    {
        IStats stats = super.getStatistics();
        stats.setTypeName("soft reference memory cache");

        List<IStatElement<?>> elems = stats.getStatElements();
        int emptyrefs = map.size() - getSize();
        elems.add(new StatElement<Integer>("empty references", Integer.valueOf(emptyrefs)));
        elems.add(new StatElement<Integer>("strong references", Integer.valueOf(strongReferences.size())));

        return stats;
    }

    @Override
    public boolean remove(K key) throws IOException
    {
        if (log.isDebugEnabled())
        {
            log.debug("Remove item for key: " + key);
        }

        boolean removed = false;

        if (key instanceof String && ((String) key).endsWith(CacheConstants.NAME_COMPONENT_DELIMITER))
        {
            for (Iterator<Map.Entry<K, MemoryElementDescriptor<K, V>>> itr = map.entrySet().iterator(); itr.hasNext(); )
            {
                Map.Entry<K, MemoryElementDescriptor<K, V>> entry = itr.next();
                K k = entry.getKey();

                if (k instanceof String && ((String) k).startsWith(key.toString()))
                {
                    lock.lock();
                    try
                    {
                        strongReferences.remove(entry.getValue().getCacheElement());
                        itr.remove();
                        removed = true;
                    }
                    finally
                    {
                        lock.unlock();
                    }
                }
            }
        }
        else if (key instanceof GroupAttrName && ((GroupAttrName<?>) key).attrName == null)
        {
            for (Iterator<Map.Entry<K, MemoryElementDescriptor<K, V>>> itr = map.entrySet().iterator(); itr.hasNext(); )
            {
                Map.Entry<K, MemoryElementDescriptor<K, V>> entry = itr.next();
                K k = entry.getKey();

                if (k instanceof GroupAttrName && ((GroupAttrName<?>) k).groupId.equals(((GroupAttrName<?>) key).groupId))
                {
                    lock.lock();
                    try
                    {
                        strongReferences.remove(entry.getValue().getCacheElement());
                        itr.remove();
                        removed = true;
                    }
                    finally
                    {
                        lock.unlock();
                    }
                }
            }
        }
        else
        {
            lock.lock();
            try
            {
                MemoryElementDescriptor<K, V> me = map.remove(key);
                if (me != null)
                {
                    strongReferences.remove(me.getCacheElement());
                    removed = true;
                }
            }
            finally
            {
                lock.unlock();
            }
        }

        return removed;
    }

    @Override
    public void removeAll() throws IOException
    {
        super.removeAll();
        strongReferences.clear();
    }

    /**
     * 创建一个软引用对象，并加入强引用队列
     */
    @Override
    public void update(ICacheElement<K, V> ce) throws IOException
    {
        putCnt.incrementAndGet();
        ce.getElementAttributes().setLastAccessTimeNow();

        lock.lock();

        try
        {
            map.put(ce.getKey(), new SoftReferenceElementDescriptor<K, V>(ce));
            //add 方法为非阻塞方法，队列满抛出异常
            strongReferences.add(ce);
            trimStrongReferences();
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * 如果强引用队列中的元素大于 MaxObjects，需要从队列中删除元素，并刷新到磁盘组件
     */
    private void trimStrongReferences()
    {
        int max = getCacheAttributes().getMaxObjects();
        int startsize = strongReferences.size();

        for (int cursize = startsize; cursize > max; cursize--)
        {
            //poll为非阻塞方法，队列空返回null
            ICacheElement<K, V> ce = strongReferences.poll();
            waterfal(ce);
        }
    }

    /**
     * LRU策略，每访问一次，都需要将这个 CacheElement 重新放入到强引用队列的尾部
     */
    @Override
    public ICacheElement<K, V> get(K key) throws IOException
    {
        ICacheElement<K, V> val = null;
        lock.lock();

        try
        {
            val = getQuiet(key);
            if (val != null)
            {
                val.getElementAttributes().setLastAccessTimeNow();
                strongReferences.add(val);
                trimStrongReferences();
            }
        }
        finally
        {
            lock.unlock();
        }

        if (val == null)
        {
            missCnt.incrementAndGet();
        }
        else
        {
            hitCnt.incrementAndGet();
        }
        return val;
    }

    @Override
    public int freeElements(int numberToFree) throws IOException
    {
        return 0;
    }
}
