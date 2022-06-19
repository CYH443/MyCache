package com.cachekit.core.memory.fifo;

import java.io.IOException;

import com.cachekit.core.memory.AbstractDoubleLinkedListMemoryCache;
import com.cachekit.core.memory.util.MemoryElementDescriptor;
import com.cachekit.core.model.ICacheElement;

/**
 * 先进先出队列，头进尾出
 */
public class FIFOMemoryCache<K, V> extends AbstractDoubleLinkedListMemoryCache<K, V> {
    @Override
    protected MemoryElementDescriptor<K, V> adjustListForUpdate(ICacheElement<K, V> ce) throws IOException {
        return addFirst(ce);
    }

    @Override
    protected void adjustListForGet(MemoryElementDescriptor<K, V> me) {
    }
}
