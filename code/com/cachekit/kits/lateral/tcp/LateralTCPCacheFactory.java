package com.cachekit.kits.lateral.tcp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.CacheWatchRepairable;
import com.cachekit.core.DaemonCacheServiceRemote;
import com.cachekit.core.DaemonCacheWatch;
import com.cachekit.core.logger.ICacheEventWrapper;
import com.cachekit.core.model.ICache;
import com.cachekit.core.model.ICacheServiceRemote;
import com.cachekit.core.model.IContextCacheManager;
import com.cachekit.core.model.IElementSerializer;
import com.cachekit.core.model.IShutdownObserver;
import com.cachekit.kits.AbstractKitCacheFactory;
import com.cachekit.kits.KitCacheAttributes;
import com.cachekit.kits.lateral.ILateralCacheListener;
import com.cachekit.kits.lateral.LateralCache;
import com.cachekit.kits.lateral.LateralCacheAsync;
import com.cachekit.kits.lateral.LateralCacheAsyncFacade;
import com.cachekit.kits.lateral.LateralCacheMonitor;
import com.cachekit.utils.discovery.UDPDiscoveryManager;
import com.cachekit.utils.discovery.UDPDiscoveryService;

public class LateralTCPCacheFactory extends AbstractKitCacheFactory
{
	private static final Log log = LogFactory.getLog(LateralTCPCacheFactory.class);

	private ConcurrentHashMap<String, ICacheServiceRemote<?, ?>> serviceInstance;

	private ReentrantLock lock;

	private ConcurrentHashMap<String, LateralTCPDiscoveryListener> listenerInstance;

	private LateralCacheMonitor monitor;

	private CacheWatchRepairable lateralWatch;

	@Override
	public <K, V> LateralCacheAsyncFacade<K, V> createCache(
			KitCacheAttributes attr,
			IContextCacheManager cacheMgr,
			ICacheEventWrapper cacheEventWrapper,
			IElementSerializer elementSerializer)
	{
		ITCPLateralCacheAttributes cacheattr = (ITCPLateralCacheAttributes) attr;
		ArrayList<ICache<K, V>> asyncs = new ArrayList<ICache<K, V>>();

		if (cacheattr.getTcpServers() != null)
		{
			StringTokenizer it = new StringTokenizer(cacheattr.getTcpServers(), ",");
			if (log.isDebugEnabled())
			{
				log.debug("Configure for [" + it.countTokens() + "]  servers.");
			}
			while (it.hasMoreElements())
			{
				String server = (String) it.nextElement();
				if (log.isDebugEnabled())
				{
					log.debug("Tcp server = " + server);
				}
				ITCPLateralCacheAttributes lateralCacheAttr = (ITCPLateralCacheAttributes) cacheattr.clone();
				lateralCacheAttr.setTcpServer(server);//注意

				LateralCacheAsync<K, V> lateralCacheAsync = createCacheAsync(lateralCacheAttr, cacheEventWrapper,
						elementSerializer);

				addListenerIfNeeded(lateralCacheAttr, cacheMgr);
				monitor.addCache(lateralCacheAsync);
				asyncs.add(lateralCacheAsync);
			}

		}

		ILateralCacheListener<K, V> listener = createListener(cacheattr, cacheMgr);

		@SuppressWarnings("unchecked")
		LateralCacheAsync<K, V>[] asyncArray = asyncs.toArray(new LateralCacheAsync[0]);
		LateralCacheAsyncFacade<K, V> lcaf = new LateralCacheAsyncFacade<K, V>(listener, asyncArray, cacheattr);

		createDiscoveryService(cacheattr, lcaf, cacheMgr, cacheEventWrapper, elementSerializer);

		return lcaf;

	}

	protected <K, V> LateralCacheAsync<K, V> createCacheAsync(
			ITCPLateralCacheAttributes attr,
			ICacheEventWrapper cacheEventWrapper,
			IElementSerializer elementSerializer)
	{
		ICacheServiceRemote<K, V> lateralService = getCacheServiceRemoteInstance(attr);

		LateralCache<K, V> cache = new LateralCache<K, V>(attr, lateralService, this.monitor);
		cache.setCacheEventLogger(cacheEventWrapper);
		cache.setElementSerializer(elementSerializer);

		if (log.isDebugEnabled())
		{
			log.debug("Create cache for async, cache [" + cache + "]");
		}

		LateralCacheAsync<K, V> lateralAsync = new LateralCacheAsync<K, V>(cache);
		lateralAsync.setCacheEventLogger(cacheEventWrapper);
		lateralAsync.setElementSerializer(elementSerializer);

		if (log.isInfoEnabled())
		{
			log.info("Create LateralCacheAsync for [" + attr + "] LateralCacheAsync = [" + lateralAsync + "]");
		}

		return lateralAsync;
	}

	@Override
	public void initialize()
	{
		this.serviceInstance = new ConcurrentHashMap<String, ICacheServiceRemote<?, ?>>();
		this.lock = new ReentrantLock();
		this.listenerInstance = new ConcurrentHashMap<String, LateralTCPDiscoveryListener>();

		this.monitor = new LateralCacheMonitor(this);
		this.monitor.setDaemon(true);
		this.monitor.start();

		this.lateralWatch = new CacheWatchRepairable();
		this.lateralWatch.setCacheWatch(new DaemonCacheWatch());
	}

	@Override
	public void dispose()
	{
		for (ICacheServiceRemote<?, ?> service : this.serviceInstance.values())
		{
			try
			{
				service.dispose("");
			}
			catch (IOException e)
			{
				log.error("Could not dispose service " + service, e);
			}
		}

		this.serviceInstance.clear();

		this.listenerInstance.clear();

		if (this.monitor != null)
		{
			this.monitor.notifyShutdown();
			try
			{
				this.monitor.join(5000);
			}
			catch (InterruptedException e)
			{

			}
			this.monitor = null;
		}
	}

	@SuppressWarnings("unchecked")
	public <K, V> ICacheServiceRemote<K, V> getCacheServiceRemoteInstance(ITCPLateralCacheAttributes attr)
	{
		String key = attr.getTcpServer();

		ICacheServiceRemote<K, V> service = (ICacheServiceRemote<K, V>) serviceInstance.get(key);

		if (service == null || service instanceof DaemonCacheServiceRemote)
		{
			lock.lock();

			try
			{
				service = (ICacheServiceRemote<K, V>) serviceInstance.get(key);

				if (service instanceof DaemonCacheServiceRemote)
				{
					service = null;
				}
				if (service == null)
				{
					try
					{
						if (log.isInfoEnabled())
						{
							log.info("Create TCP service, attr = " + attr);
						}

						service = new LateralTCPService<K, V>(attr);
					}
					catch (IOException ex)
					{
						log.error("Failure, lateral instance will use daemon service", ex);

						service = new DaemonCacheServiceRemote<K, V>(attr.getDaemonQueueMaxSize());

						monitor.notifyError();
					}

					serviceInstance.put(key, service);
				}
			}
			finally
			{
				lock.unlock();
			}
		}

		return service;
	}

	private LateralTCPDiscoveryListener getDiscoveryListener(ITCPLateralCacheAttributes attr, IContextCacheManager cacheManager)
	{
		String key = attr.getUdpDiscoveryAddr() + ":" + attr.getUdpDiscoveryPort();
		LateralTCPDiscoveryListener ins = null;

		LateralTCPDiscoveryListener newListener = new LateralTCPDiscoveryListener(this.getName(), cacheManager);
		ins = listenerInstance.putIfAbsent(key, newListener);

		if (ins == null)
		{
			ins = newListener;

			if (log.isInfoEnabled())
			{
				log.info("Create new discovery listener for " + key + " cacheName for request " + attr.getCacheName());
			}
		}

		return ins;
	}

	private void addListenerIfNeeded(ITCPLateralCacheAttributes attr, IContextCacheManager cacheMgr)
	{
		if (attr.isReceive())
		{
			try
			{
				addLateralCacheListener(attr.getCacheName(), LateralTCPReceiver.getInstance(attr, cacheMgr));
			}
			catch (IOException ioe)
			{
				log.error("Problem creating lateral listener", ioe);
			}
		}
		else
		{
			if (log.isDebugEnabled())
			{
				log.debug("Not creating a listener since not receiving.");
			}
		}
	}

	private <K, V> void addLateralCacheListener(String cacheName, ILateralCacheListener<K, V> listener) throws IOException
	{
		synchronized (this.lateralWatch)
		{
			lateralWatch.addCacheListener(cacheName, listener);
		}
	}

	private <K, V> ILateralCacheListener<K, V> createListener(ITCPLateralCacheAttributes attr, IContextCacheManager cacheMgr)
	{
		ILateralCacheListener<K, V> listener = null;

		if (attr.isReceive())
		{
			if (log.isInfoEnabled())
			{
				log.info("Get listener for " + attr);
			}

			listener = LateralTCPReceiver.getInstance(attr, cacheMgr);

			cacheMgr.registerShutdownObserver((IShutdownObserver) listener);
		}

		return listener;
	}

	private synchronized <K, V> UDPDiscoveryService createDiscoveryService(
			ITCPLateralCacheAttributes attr,
			LateralCacheAsyncFacade<K, V> lcaf,
			IContextCacheManager cacheMgr,
			ICacheEventWrapper cacheEventWrapper,
			IElementSerializer elementSerializer)
	{
		UDPDiscoveryService discovery = null;

		if (attr.isUdpDiscoveryEnabled())
		{
			LateralTCPDiscoveryListener discoveryListener = getDiscoveryListener(attr, cacheMgr);
			discoveryListener.addAsyncFacade(attr.getCacheName(), lcaf);

			discovery = UDPDiscoveryManager.getInstance().getService(attr.getUdpDiscoveryAddr(),
					attr.getUdpDiscoveryPort(), attr.getTcpListenerPort(), cacheMgr);

			discovery.addParticipatingCacheName(attr.getCacheName());
			discovery.addDiscoveryListener(discoveryListener);

			if (log.isInfoEnabled())
			{
				log.info("Register TCP lateral cache [" + attr.getCacheName() + "] with UDPDiscoveryService.");
			}
		}
		return discovery;
	}
}
