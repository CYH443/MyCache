package com.cachekit.core.memory.mru;

import java.io.IOException;

import com.cachekit.core.memory.AbstractDoubleLinkedListMemoryCache;
import com.cachekit.core.memory.util.MemoryElementDescriptor;
import com.cachekit.core.model.ICacheElement;

public class MRUMemoryCache<K, V> extends AbstractDoubleLinkedListMemoryCache<K, V>
{
	@Override
	protected MemoryElementDescriptor<K, V> adjustListForUpdate(ICacheElement<K, V> ce) throws IOException
	{
		//是不是应该移到链表的末端
		return addFirst(ce);
	}

	@Override
	protected void adjustListForGet(MemoryElementDescriptor<K, V> me)
	{
		list.makeLast(me);
	}
}
