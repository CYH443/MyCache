package com.cachekit.utils.serialization;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.CacheElement;
import com.cachekit.core.CacheElementSerialized;
import com.cachekit.core.model.ICacheElement;
import com.cachekit.core.model.ICacheElementSerialized;
import com.cachekit.core.model.IElementSerializer;

public class SerializationConversionUtil
{
	private static final Log log = LogFactory.getLog(SerializationConversionUtil.class);

	public static <K, V> ICacheElementSerialized<K, V> getSerializedCacheElement(ICacheElement<K, V> element,
			IElementSerializer elementSerializer) throws IOException
	{
		if (element == null)
		{
			return null;
		}

		byte[] serializedValue = null;

		if (element instanceof ICacheElementSerialized)
		{
			serializedValue = ((ICacheElementSerialized<K, V>) element).getSerializedValue();
		}
		else
		{
			if (elementSerializer != null)
			{
				try
				{
					serializedValue = elementSerializer.serialize(element.getVal());
				}
				catch (IOException e)
				{
					log.error("Problem serializing object.", e);
					throw e;
				}
			}
			else
			{
				log.warn("ElementSerializer is null.  Could not serialize object.");
				throw new IOException("Could not serialize object.  The ElementSerializer is null.");
			}
		}
		ICacheElementSerialized<K, V> serialized = new CacheElementSerialized<K, V>(element.getCacheName(),
				element.getKey(), serializedValue, element.getElementAttributes());

		return serialized;
	}

	public static <K, V> ICacheElement<K, V> getDeSerializedCacheElement(ICacheElementSerialized<K, V> serialized,
			IElementSerializer elementSerializer) throws IOException, ClassNotFoundException
	{
		if (serialized == null)
		{
			return null;
		}

		V deSerializedValue = null;

		if (elementSerializer != null)
		{
			try
			{
				try
				{
					deSerializedValue = elementSerializer.deSerialize(serialized.getSerializedValue(), null);
				}
				catch (ClassNotFoundException e)
				{
					log.error("Problem de-serializing object.", e);
					throw e;
				}
			}
			catch (IOException e)
			{
				log.error("Problem de-serializing object.", e);
				throw e;
			}
		}
		else
		{
			log.warn("ElementSerializer is null.  Could not serialize object.");
		}
		ICacheElement<K, V> deSerialized = new CacheElement<K, V>(serialized.getCacheName(), serialized.getKey(),
				deSerializedValue);
		deSerialized.setElementAttributes(serialized.getElementAttributes());

		return deSerialized;
	}
}
