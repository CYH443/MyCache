package com.cachekit.core.memory.util;

import com.cachekit.core.model.ICacheElement;
import com.cachekit.utils.struct.DoubleLinkedListNode;

//Linux系统有文件描述符，简称fd，MemoryElementDescriptor与之类似，简称为ed。
public class MemoryElementDescriptor<K, V> extends DoubleLinkedListNode<ICacheElement<K, V>> {

    private static final long serialVersionUID = 1L;

    public MemoryElementDescriptor(ICacheElement<K, V> ce)
    {
        super(ce);
    }

    public ICacheElement<K, V> getCacheElement() {
        return getPayload();
    }
}
