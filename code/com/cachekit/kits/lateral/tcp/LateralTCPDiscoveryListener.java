package com.cachekit.kits.lateral.tcp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.control.ContextCacheManager;
import com.cachekit.core.model.IContextCacheManager;
import com.cachekit.kits.KitCache;
import com.cachekit.kits.KitCacheAttributes;
import com.cachekit.kits.lateral.LateralCacheAsync;
import com.cachekit.kits.lateral.LateralCacheAsyncFacade;
import com.cachekit.kits.lateral.LateralCacheAttributes;
import com.cachekit.utils.discovery.DiscoveredService;
import com.cachekit.utils.discovery.IDiscoveryListener;
//远程服务发现（服务调用方，TCP客户端），维护多个cache的LateralCacheAsync集合
public class LateralTCPDiscoveryListener implements IDiscoveryListener
{
	private static final Log log = LogFactory.getLog(LateralTCPDiscoveryListener.class);
	//key 为cacheName，value 为 LateralCacheAsyncFacade
	private final Map<String, LateralCacheAsyncFacade<?, ?>> facades = Collections
			.synchronizedMap(new HashMap<String, LateralCacheAsyncFacade<?, ?>>());

	private String factoryName;

	private IContextCacheManager cacheManager;

	protected LateralTCPDiscoveryListener(String factoryName, IContextCacheManager cacheManager)
	{
		this.factoryName = factoryName;
		this.cacheManager = cacheManager;
	}

	public synchronized boolean addAsyncFacade(String cacheName, LateralCacheAsyncFacade<?, ?> facade)
	{
		boolean isNew = !containsAsyncFacade(cacheName);

		facades.put(cacheName, facade);

		return isNew;
	}

	public boolean containsAsyncFacade(String cacheName)
	{
		return facades.containsKey(cacheName);
	}

	public <K, V> boolean containsAsync(String cacheName, LateralCacheAsync<K, V> async)
	{
		@SuppressWarnings("unchecked")
		LateralCacheAsyncFacade<K, V> facade = (LateralCacheAsyncFacade<K, V>) facades.get(async.getCacheName());
		if (facade == null)
		{
			return false;
		}

		return facade.containsAsync(async);
	}

	public <K, V> boolean containsAsync(String cacheName, String tcpServer)
	{
		@SuppressWarnings("unchecked")
		LateralCacheAsyncFacade<K, V> facade = (LateralCacheAsyncFacade<K, V>) facades.get(cacheName);
		if (facade == null)
		{
			return false;
		}

		return facade.containsAsync(tcpServer);
	}

	protected <K, V> boolean addAsync(LateralCacheAsync<K, V> async)
	{
		@SuppressWarnings("unchecked")
		LateralCacheAsyncFacade<K, V> facade = (LateralCacheAsyncFacade<K, V>) facades.get(async.getCacheName());

		if (facade != null)
		{
			boolean isNew = facade.addAsync(async);
			if (log.isDebugEnabled())
			{
				log.debug("Call addAsync, isNew = " + isNew);
			}
			return isNew;
		}
		else
		{
			return false;
		}
	}

	protected <K, V> boolean removeAsync(LateralCacheAsync<K, V> async)
	{
		@SuppressWarnings("unchecked")
		LateralCacheAsyncFacade<K, V> facade = (LateralCacheAsyncFacade<K, V>) facades.get(async.getCacheName());

		if (facade != null)
		{
			boolean removed = facade.removeAsync(async);

			return removed;
		}
		else
		{
			return false;
		}
	}

	protected <K, V> boolean removeAsync(String cacheName, String tcpServer)
	{
		@SuppressWarnings("unchecked")
		LateralCacheAsyncFacade<K, V> facade = (LateralCacheAsyncFacade<K, V>) facades.get(cacheName);

		if (facade != null)
		{
			boolean removed = facade.removeAsync(tcpServer);

			return removed;
		}
		else
		{
			return false;
		}
	}

	@Override
	public void addDiscoveredService(DiscoveredService service)
	{
		ArrayList<String> regions = service.getCacheNames();
		//服务端ip和端口号
		String serverAndPort = service.getServiceAddress() + ":" + service.getServicePort();

		if (regions != null)
		{
			for (String cacheName : regions)
			{
				KitCache<?, ?> kit = cacheManager.getKitCache(factoryName, cacheName);

				if (kit != null)
				{
					KitCacheAttributes attr = kit.getKitCacheAttributes();
					if (attr instanceof ITCPLateralCacheAttributes)
					{
						ITCPLateralCacheAttributes lca = (ITCPLateralCacheAttributes) attr;

						if (lca.getTransmissionType() == LateralCacheAttributes.Type.TCP && !containsAsync(cacheName, serverAndPort))
						{
							ContextCacheManager ccm = (ContextCacheManager) cacheManager;

							LateralTCPCacheFactory factory = (LateralTCPCacheFactory) ccm.registryFacGet(factoryName);
							lca.setTcpServer(serverAndPort);

							LateralCacheAsyncFacade<?, ?> facade = (LateralCacheAsyncFacade<?, ?>) kit;

							LateralCacheAsync<?, ?> async = factory.createCacheAsync(lca, facade.getCacheEventLogger(), facade.getElementSerializer());
							boolean result = addAsync(async);

							if (log.isDebugEnabled())
							{
								log.debug("Call addAsync, result = " + result);
							}
						}
					}
				}
			}
		}
		else
		{
			log.warn("No cache names found in message " + service);
		}
	}

	@Override
	public void removeDiscoveredService(DiscoveredService service)
	{
		ArrayList<String> regions = service.getCacheNames();
		String serverAndPort = service.getServiceAddress() + ":" + service.getServicePort();

		if (regions != null)
		{
			for (String cacheName : regions)
			{
				KitCache<?, ?> kit = cacheManager.getKitCache(factoryName, cacheName);

				if (kit != null)
				{
					KitCacheAttributes attr = kit.getKitCacheAttributes();
					if (attr instanceof ITCPLateralCacheAttributes)
					{
						ITCPLateralCacheAttributes lca = (ITCPLateralCacheAttributes) attr;

						if (lca.getTransmissionType() == LateralCacheAttributes.Type.TCP && containsAsync(cacheName, serverAndPort))
						{
							boolean result = removeAsync(cacheName, serverAndPort);

							if (log.isDebugEnabled())
							{
								log.debug("Call removeAsync, result = " + result);
							}

						}
					}

				}
			}
		}
		else
		{
			log.warn("No cache names found in message " + service);
		}
	}
}
