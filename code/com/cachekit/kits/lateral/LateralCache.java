package com.cachekit.kits.lateral;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.CacheInfo;
import com.cachekit.core.CacheStatus;
import com.cachekit.core.DaemonCacheServiceRemote;
import com.cachekit.core.model.ICacheElement;
import com.cachekit.core.model.ICacheServiceRemote;
import com.cachekit.core.model.IDaemon;
import com.cachekit.core.stats.IStats;
import com.cachekit.core.stats.Stats;
import com.cachekit.kits.AbstractKitCacheEvent;
import com.cachekit.kits.KitCacheAttributes;
//同步线性组件发送端：当本地系统的内存组件和磁盘组件都不存在数据时，就会与请求另外一个系统的线性组件监听器通信，获取相关数据
public class LateralCache<K, V> extends AbstractKitCacheEvent<K, V>
{
	private static final Log log = LogFactory.getLog(LateralCache.class);

	private final ILateralCacheAttributes lateralCacheAttributes;

	final String cacheName;

	// LaterTCPService或者DaemonCacheServiceRemote
	// LaterCache 发生错误时，会将 LaterCache 里面的 ICacheServiceRemote 转换成 DaemonCacheServiceRemote
	// 将发生错误的相关操作存储起来，然后启动 LaterCacheMonitor 去修复
	private ICacheServiceRemote<K, V> lateralCacheService;

	// 一个检测 LateralCacheAsync 组件错误的线程，当发生异常时，开启该线程
	private LateralCacheMonitor monitor;

	public LateralCache(ILateralCacheAttributes cattr, ICacheServiceRemote<K, V> lateral, LateralCacheMonitor monitor)
	{
		this.cacheName = cattr.getCacheName();
		this.lateralCacheAttributes = cattr;
		this.lateralCacheService = lateral;
		this.monitor = monitor;
	}

	public LateralCache(ILateralCacheAttributes cattr)
	{
		this.cacheName = cattr.getCacheName();
		this.lateralCacheAttributes = cattr;
	}

	@Override
	protected void processUpdate(ICacheElement<K, V> ce) throws IOException
	{
		try
		{
			if (ce != null)
			{
				if (log.isDebugEnabled())
				{
					log.debug("Update: lateral = [" + lateralCacheService + "], " + "CacheInfo.listenerId = " + CacheInfo.listenerId);
				}
				lateralCacheService.update(ce, CacheInfo.listenerId);
			}
		}
		catch (IOException ex)
		{
			handleException(ex, "Fail to put [" + ce.getKey() + "] to " + ce.getCacheName() + "@" + lateralCacheAttributes);
		}
	}

	@Override
	protected ICacheElement<K, V> processGet(K key) throws IOException
	{
		ICacheElement<K, V> obj = null;

		if (this.lateralCacheAttributes.getPutOnlyMode())
		{
			return null;
		}
		try
		{
			obj = lateralCacheService.get(cacheName, key);
		}
		catch (Exception e)
		{
			log.error(e);
			handleException(e, "Fail to get [" + key + "] from " + lateralCacheAttributes.getCacheName() + "@" + lateralCacheAttributes);
		}
		return obj;
	}

	@Override
	protected Map<K, ICacheElement<K, V>> processGetMatching(String pattern) throws IOException
	{
		if (this.lateralCacheAttributes.getPutOnlyMode())
		{
			return Collections.emptyMap();
		}
		try
		{
			return lateralCacheService.getMatching(cacheName, pattern);
		}
		catch (IOException e)
		{
			log.error(e);
			handleException(e, "Fail to getMatching [" + pattern + "] from " + lateralCacheAttributes.getCacheName()
					+ "@" + lateralCacheAttributes);
			return Collections.emptyMap();
		}
	}

	@Override
	protected Map<K, ICacheElement<K, V>> processGetMultiple(Set<K> keys) throws IOException
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
	public Set<K> getKeySet() throws IOException
	{
		try
		{
			return lateralCacheService.getKeySet(cacheName);
		}
		catch (IOException ex)
		{
			handleException(ex, "Fail to get key set from " + lateralCacheAttributes.getCacheName() + "@" + lateralCacheAttributes);
		}
		return Collections.emptySet();
	}

	@Override
	protected boolean processRemove(K key) throws IOException
	{
		if (log.isDebugEnabled())
		{
			log.debug("Remove key:" + key);
		}

		try
		{
			lateralCacheService.remove(cacheName, key, CacheInfo.listenerId);
		}
		catch (IOException ex)
		{
			handleException(ex, "Fail to remove " + key + " from " + lateralCacheAttributes.getCacheName() + "@" + lateralCacheAttributes);
		}
		return false;
	}

	@Override
	protected void processRemoveAll() throws IOException
	{
		try
		{
			lateralCacheService.removeAll(cacheName, CacheInfo.listenerId);
		}
		catch (IOException ex)
		{
			handleException(ex, "Fail to remove all from " + lateralCacheAttributes.getCacheName() + "@" + lateralCacheAttributes);
		}
	}

	@Override
	protected void processDispose() throws IOException
	{
		try
		{
			lateralCacheService.dispose(this.lateralCacheAttributes.getCacheName());
		}
		catch (IOException ex)
		{
			log.error("Couldn't dispose", ex);
			handleException(ex, "fail to dispose " + lateralCacheAttributes.getCacheName());
		}
	}

	@Override
	public CacheStatus getStatus()
	{
		return this.lateralCacheService instanceof IDaemon ? CacheStatus.ERROR : CacheStatus.ALIVE;
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

	/**
	 * 发生异常，LaterTCPService 转换成 DaemonCacheServiceRemote，唤醒 LaterCacheMonitor 线程
	 */
	private void handleException(Exception ex, String msg) throws IOException
	{

		lateralCacheService = new DaemonCacheServiceRemote<K, V>(lateralCacheAttributes.getDaemonQueueMaxSize());
		monitor.notifyError();

		if (ex instanceof IOException)
		{
			throw (IOException) ex;
		}
		throw new IOException(ex.getMessage());
	}

	/**
	 * 供 LaterCacheMonitor 调用
	 * @param restoredLateral 新的 LaterTCPService
	 */
	public void fixCache(ICacheServiceRemote<K, V> restoredLateral)
	{
		// 如果发生错误，ICacheServiceRemote 为 DaemonCacheServiceRemote 类型时，
		// 把里面队列存的  CacheEvent 都发送出去执行，并将 ICacheServiceRemote 恢复成 LaterTCPService 类型
		if (this.lateralCacheService != null && this.lateralCacheService instanceof DaemonCacheServiceRemote)
		{
			DaemonCacheServiceRemote<K, V> daemon = (DaemonCacheServiceRemote<K, V>) this.lateralCacheService;
			this.lateralCacheService = restoredLateral;
			try
			{
				//交给新的LateralTCPService去处理
				daemon.spreadEvents(restoredLateral);
			}
			catch (Exception e)
			{
				try
				{
					handleException(e, "Problem in spreading events from DaemonCacheServiceRemote to new Lateral Service.");
				}
				catch (IOException e1)
				{
				}
			}
		}
		else
		{
			this.lateralCacheService = restoredLateral;
		}
	}

	@Override
	public String getStats()
	{
		return "";
	}

	@Override
	public KitCacheAttributes getKitCacheAttributes()
	{
		return lateralCacheAttributes;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("\n LateralCache ");
		sb.append("\n Cache Name [" + lateralCacheAttributes.getCacheName() + "]");
		sb.append("\n cattr =  [" + lateralCacheAttributes + "]");
		return sb.toString();
	}

	@Override
	public String getEventLoggerExtraInfo()
	{
		return null;
	}

	@Override
	public IStats getStatistics()
	{
		IStats stats = new Stats();
		stats.setTypeName("LateralCache");
		return stats;
	}
}
