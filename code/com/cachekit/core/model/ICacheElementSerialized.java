package com.cachekit.core.model;

public interface ICacheElementSerialized<K, V> extends ICacheElement<K, V>
{
	byte[] getSerializedValue();
}
