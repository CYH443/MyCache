package com.cachekit.kits.lateral;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.CacheStatus;
import com.cachekit.core.model.ICacheElement;
import com.cachekit.core.stats.IStatElement;
import com.cachekit.core.stats.IStats;
import com.cachekit.core.stats.StatElement;
import com.cachekit.core.stats.Stats;
import com.cachekit.kits.AbstractKitCache;
import com.cachekit.kits.KitCache;
import com.cachekit.kits.KitCacheAttributes;
import com.cachekit.kits.lateral.tcp.TCPLateralCacheAttributes;
//一个cache所拥有的LateralCacheAsync集合，每一个LateralCacheAsync连接远程线性监听组件服务的一个端口
public class LateralCacheAsyncFacade<K, V> extends AbstractKitCache<K, V>
{
	private static final Log log = LogFactory.getLog(LateralCacheAsyncFacade.class);

	public LateralCacheAsync<K, V>[] asyncs;

	private final String cacheName;
	//LaterTCPReceiver
	private ILateralCacheListener<K, V> listener;

	private final ILateralCacheAttributes lateralCacheAttributes;

	private boolean disposed = false;

	public LateralCacheAsyncFacade(ILateralCacheListener<K, V> listener, LateralCacheAsync<K, V>[] asyncs, ILateralCacheAttributes cattr)
	{
		this.listener = listener;
		this.asyncs = asyncs;
		this.cacheName = cattr.getCacheName();
		this.lateralCacheAttributes = cattr;
	}

	/**
	 * 通过与其连接的TCP服务端的地址和端口号进行比较
	 */
	public boolean containsAsync(LateralCacheAsync<K, V> async)
	{
		for (int i = 0; i < asyncs.length; i++)
		{
			//			if (async.equals(asyncs[i]))
			//			{
			//				return true;
			//			}
			//			
			TCPLateralCacheAttributes attr1 = (TCPLateralCacheAttributes) async.getKitCacheAttributes();

			TCPLateralCacheAttributes attr2 = (TCPLateralCacheAttributes) asyncs[i].getKitCacheAttributes();

			if (attr1.getTcpServer().equals(attr2.getTcpServer()))
			{
				return true;
			}
		}
		return false;
	}

	public boolean containsAsync(String tcpServer)
	{
		for (int i = 0; i < asyncs.length; i++)
		{

			TCPLateralCacheAttributes attr = (TCPLateralCacheAttributes) asyncs[i].getKitCacheAttributes();

			if (tcpServer.equals(attr.getTcpServer()))
			{
				return true;
			}

		}
		return false;
	}
	//考虑数组扩容
	public synchronized boolean addAsync(LateralCacheAsync<K, V> async)
	{
		if (async == null)
		{
			return false;
		}

		if (containsAsync(async))
		{
			if (log.isDebugEnabled())
			{
				log.debug("Async already contained, [" + async + "]");
			}
			return false;
		}

		@SuppressWarnings("unchecked")
		LateralCacheAsync<K, V>[] newArray = new LateralCacheAsync[asyncs.length + 1];

		System.arraycopy(asyncs, 0, newArray, 0, asyncs.length);

		newArray[asyncs.length] = async;

		asyncs = newArray;

		log.debug("Async length is [" + asyncs.length + "]");

		return true;
	}

	public synchronized boolean removeAsync(LateralCacheAsync<K, V> async)
	{
		if (async == null)
		{
			return false;
		}

		int position = -1;
		for (int i = 0; i < asyncs.length; i++)
		{
			//			if (async.equals(asyncs[i]))
			//			{
			//				position = i;
			//				break;
			//			}

			TCPLateralCacheAttributes attr1 = (TCPLateralCacheAttributes) async.getKitCacheAttributes();

			TCPLateralCacheAttributes attr2 = (TCPLateralCacheAttributes) asyncs[i].getKitCacheAttributes();

			if (attr1.getTcpServer().equals(attr2.getTcpServer()))
			{
				position = i;
				break;
			}

		}

		if (position == -1)
		{
			return false;
		}

		@SuppressWarnings("unchecked")
		LateralCacheAsync<K, V>[] newArray = new LateralCacheAsync[asyncs.length - 1];

		System.arraycopy(asyncs, 0, newArray, 0, position);
		if (asyncs.length != position)
		{
			System.arraycopy(asyncs, position + 1, newArray, position, asyncs.length - position - 1);
		}
		asyncs = newArray;

		return true;
	}

	public synchronized boolean removeAsync(String tcpServer)
	{
		if (tcpServer == null)
		{
			return false;
		}

		int position = -1;
		for (int i = 0; i < asyncs.length; i++)
		{

			TCPLateralCacheAttributes attr = (TCPLateralCacheAttributes) asyncs[i].getKitCacheAttributes();

			if (tcpServer.equals(attr.getTcpServer()))
			{
				position = i;
				break;
			}

		}

		if (position == -1)
		{
			return false;
		}

		@SuppressWarnings("unchecked")
		LateralCacheAsync<K, V>[] newArray = new LateralCacheAsync[asyncs.length - 1];

		System.arraycopy(asyncs, 0, newArray, 0, position);
		if (asyncs.length != position)
		{
			System.arraycopy(asyncs, position + 1, newArray, position, asyncs.length - position - 1);
		}
		asyncs = newArray;

		return true;
	}

	@Override
	public void update(ICacheElement<K, V> ce) throws IOException
	{
		if (log.isDebugEnabled())
		{
			log.debug("Updating through lateral cache facade, asyncs length = " + asyncs.length);
		}
		try
		{
			for (int i = 0; i < asyncs.length; i++)
			{
				asyncs[i].update(ce);
			}
		}
		catch (Exception ex)
		{
			log.error(ex);
		}
	}

	/**
	 * 只要有一个 LateralCacheAsync有返回值就可以
	 */
	@Override
	public ICacheElement<K, V> get(K key)
	{
		for (int i = 0; i < asyncs.length; i++)
		{
			try
			{
				ICacheElement<K, V> obj = asyncs[i].get(key);

				if (obj != null)
				{
					return obj;
				}
			}
			catch (Exception ex)
			{
				log.error("Fail to get", ex);
			}
		}
		return null;
	}

	@Override
	public Map<K, ICacheElement<K, V>> getMultiple(Set<K> keys)
	{
		Map<K, ICacheElement<K, V>> elements = new HashMap<K, ICacheElement<K, V>>();

		if (keys != null && !keys.isEmpty())
		{
			for (K key : keys)
			{
				ICacheElement<K, V> element = get(key);

				if (element != null)
				{
					elements.put(key, element);
				}
			}
		}

		return elements;
	}

	@Override
	public Map<K, ICacheElement<K, V>> getMatching(String pattern)
	{
		Map<K, ICacheElement<K, V>> elements = new HashMap<K, ICacheElement<K, V>>();
		for (int i = 0; i < asyncs.length; i++)
		{
			try
			{
				elements.putAll(asyncs[i].getMatching(pattern));
			}
			catch (Exception ex)
			{
				log.error("Fail to get", ex);
			}
		}
		return elements;
	}

	@Override
	public Set<K> getKeySet() throws IOException
	{
		HashSet<K> allKeys = new HashSet<K>();
		for (int i = 0; i < asyncs.length; i++)
		{
			KitCache<K, V> kit = asyncs[i];
			if (kit != null)
			{
				Set<K> keys = kit.getKeySet();
				if (keys != null)
				{
					allKeys.addAll(keys);
				}
			}
		}
		return allKeys;
	}

	@Override
	public boolean remove(K key)
	{
		try
		{
			for (int i = 0; i < asyncs.length; i++)
			{
				asyncs[i].remove(key);
			}
		}
		catch (Exception ex)
		{
			log.error(ex);
		}
		return false;
	}

	@Override
	public void removeAll()
	{
		try
		{
			for (int i = 0; i < asyncs.length; i++)
			{
				asyncs[i].removeAll();
			}
		}
		catch (Exception ex)
		{
			log.error(ex);
		}
	}

	@Override
	public void dispose()
	{
		try
		{
			if (listener != null)
			{
				listener.dispose();
				listener = null;
			}

			for (int i = 0; i < asyncs.length; i++)
			{
				asyncs[i].dispose();
			}
		}
		catch (Exception ex)
		{
			log.error(ex);
		}
		finally
		{
			disposed = true;
		}
	}

	@Override
	public int getSize()
	{
		return 0;
	}

	@Override
	public CacheType getCacheType()
	{
		return CacheType.LATERAL_CACHE;
	}

	@Override
	public String getCacheName()
	{
		return cacheName;
	}

	@Override
	public CacheStatus getStatus()
	{
		if (disposed)
		{
			return CacheStatus.DISPOSED;
		}

		if (asyncs.length == 0 || listener != null)
		{
			return CacheStatus.ALIVE;
		}

		CacheStatus[] status = new CacheStatus[asyncs.length];
		for (int i = 0; i < asyncs.length; i++)
		{
			status[i] = asyncs[i].getStatus();
		}
		for (int i = 0; i < asyncs.length; i++)
		{
			if (status[i] == CacheStatus.ALIVE)
			{
				return CacheStatus.ALIVE;
			}
		}
		for (int i = 0; i < asyncs.length; i++)
		{
			if (status[i] == CacheStatus.ERROR)
			{
				return CacheStatus.ERROR;
			}
		}

		return CacheStatus.DISPOSED;
	}

	@Override
	public KitCacheAttributes getKitCacheAttributes()
	{
		return this.lateralCacheAttributes;
	}

	@Override
	public String toString()
	{
		return "LateralCacheAsyncFacade: " + cacheName;
	}

	@Override
	public String getEventLoggerExtraInfo()
	{
		return "Lateral Cache Async";
	}

	@Override
	public String getStats()
	{
		return getStatistics().toString();
	}

	@Override
	public IStats getStatistics()
	{
		IStats stats = new Stats();
		stats.setTypeName("Lateral Cache Async Facade");

		ArrayList<IStatElement<?>> elems = new ArrayList<IStatElement<?>>();

		if (asyncs != null)
		{
			elems.add(new StatElement<Integer>("Number of Async", Integer.valueOf(asyncs.length)));

			for (LateralCacheAsync<K, V> async : asyncs)
			{
				if (async != null)
				{
					IStats sStats = async.getStatistics();
					elems.addAll(sStats.getStatElements());
				}
			}
		}

		stats.setStatElements(elems);

		return stats;
	}
}
