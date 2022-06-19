package com.cachekit.utils.discovery;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.model.IRequireScheduler;
import com.cachekit.core.model.IShutdownObserver;
import com.cachekit.utils.net.HostNameUtil;
//UDP组播实现服务注册与服务发现，然后通过 IDiscoveryListener 建立TCP连接，实现服务调用
public class UDPDiscoveryService implements IShutdownObserver, IRequireScheduler
{
	private static final Log log = LogFactory.getLog(UDPDiscoveryService.class);

	private Thread udpReceiverThread;
	//runnable
	private UDPDiscoveryReceiver receiver;
	//runnable
	private UDPDiscoverySenderThread sender = null;

	private UDPDiscoveryAttributes udpDiscoveryAttributes = null;

	private boolean shutdown = false;
	//服务方集合
	private Set<DiscoveredService> discoveredServices = new CopyOnWriteArraySet<DiscoveredService>();

	private final Set<String> cacheNames = new CopyOnWriteArraySet<String>();
	//LateralTCPDiscoveryListener
	private final Set<IDiscoveryListener> discoveryListeners = new CopyOnWriteArraySet<IDiscoveryListener>();

	public UDPDiscoveryService(UDPDiscoveryAttributes attributes)
	{
		udpDiscoveryAttributes = (UDPDiscoveryAttributes) attributes.clone();

		try
		{
			//设置发送端地址(自身ip)
			udpDiscoveryAttributes.setServiceAddress(HostNameUtil.getLocalHostAddress());
		}
		catch (UnknownHostException e)
		{
			log.error("Couldn't get localhost address", e);
		}

		try
		{
			//服务接受器
			receiver = new UDPDiscoveryReceiver(
					this,
					getUdpDiscoveryAttributes().getUdpDiscoveryAddr(),
					getUdpDiscoveryAttributes().getUdpDiscoveryPort()
			);
		}
		catch (IOException e)
		{
			log.error(
					"Problem in creating UDPDiscoveryReceiver, address ["
							+ getUdpDiscoveryAttributes().getUdpDiscoveryAddr() + "] port ["
							+ getUdpDiscoveryAttributes().getUdpDiscoveryPort() + "] not able to find any other caches",
					e);
		}
		//创建发送线程，初始化时会向组播地址发送一次requestBroadcast请求
		sender = new UDPDiscoverySenderThread(getUdpDiscoveryAttributes(), getCacheNames());
	}

	/**
	 * 将 sender线程设置为周期性执行,每15s向组播地址发送一次 PASSIVE 广播
	 * @param scheduledExecutor ScheduledThreadPoolExecutor
	 */
	@Override
	public void setScheduledExecutorService(ScheduledExecutorService scheduledExecutor)
	{
		if (sender != null)
		{
			scheduledExecutor.scheduleAtFixedRate(sender, 0, 15, TimeUnit.SECONDS);
		}

		UDPCleanupRunner cleanup = new UDPCleanupRunner(this);
		scheduledExecutor.scheduleAtFixedRate(cleanup, 0, getUdpDiscoveryAttributes().getMaxIdleTimeSecond(), TimeUnit.SECONDS);
	}

	/**
	 * UDPDiscoveryReceiver 收到其他的 REQUEST 请求报文，要求 sender 向组播地址发送 PASSIVE 报文
	 */
	protected void serviceRequestBroadcast()
	{
		UDPDiscoverySender sender = null;
		try
		{
			//根据目标ip和目标port创建sender
			sender = new UDPDiscoverySender(getUdpDiscoveryAttributes().getUdpDiscoveryAddr(), getUdpDiscoveryAttributes().getUdpDiscoveryPort());

			sender.passiveBroadcast(getUdpDiscoveryAttributes().getServiceAddress(), getUdpDiscoveryAttributes().getServicePort(), this.getCacheNames());

			if (log.isDebugEnabled())
			{
				log.debug("Call sender to issue a passive broadcast");
			}
		}
		catch (Exception e)
		{
			log.error("Problem in calling the UDP Discovery Sender. address ["
					+ getUdpDiscoveryAttributes().getUdpDiscoveryAddr() + "] port ["
					+ getUdpDiscoveryAttributes().getUdpDiscoveryPort() + "]", e);
		}
		finally
		{
			try
			{
				if (sender != null)
				{
					sender.destroy();
				}
			}
			catch (Exception e)
			{
				log.error("Problem in closing Passive Broadcast sender, while servicing a request broadcast.", e);
			}
		}
	}

	public void addParticipatingCacheName(String cacheName)
	{
		cacheNames.add(cacheName);
		sender.setCacheNames(getCacheNames());
	}

	public void removeDiscoveredService(DiscoveredService service)
	{
		boolean contained = getDiscoveredServices().remove(service);

		if (contained)
		{
			if (log.isInfoEnabled())
			{
				log.info("Remove " + service);
			}
		}

		for (IDiscoveryListener listener : getDiscoveryListeners())
		{
			listener.removeDiscoveredService(service);
		}
	}

	protected void addOrUpdateService(DiscoveredService discoveredService)
	{
		synchronized (getDiscoveredServices())
		{
			if (!getDiscoveredServices().contains(discoveredService))
			{
				if (log.isInfoEnabled())
				{
					log.info("Set does not contain service. cachekit discovered " + discoveredService);
				}
				if (log.isDebugEnabled())
				{
					log.debug("Add service in the set " + discoveredService);
				}
				getDiscoveredServices().add(discoveredService);
			}
			else
			{
				if (log.isDebugEnabled())
				{
					log.debug("Set contains service.");
				}
				if (log.isDebugEnabled())
				{
					log.debug("Update service in the set " + discoveredService);
				}

				DiscoveredService oldService = null;
				for (DiscoveredService service : getDiscoveredServices())
				{
					if (discoveredService.equals(service))
					{
						oldService = service;
						break;
					}
				}
				if (oldService != null)
				{
					if (!oldService.getCacheNames().equals(discoveredService.getCacheNames()))
					{
						if (log.isInfoEnabled())
						{
							log.info("List of cache names changed for service: " + discoveredService);
						}
					}
				}

				getDiscoveredServices().remove(discoveredService);
				getDiscoveredServices().add(discoveredService);
			}
		}
		for (IDiscoveryListener listener : getDiscoveryListeners())
		{
			listener.addDiscoveredService(discoveredService);
		}
	}

	protected ArrayList<String> getCacheNames()
	{
		ArrayList<String> names = new ArrayList<String>();
		names.addAll(cacheNames);
		return names;
	}

	public void setUdpDiscoveryAttributes(UDPDiscoveryAttributes attr)
	{
		this.udpDiscoveryAttributes = attr;
	}

	public UDPDiscoveryAttributes getUdpDiscoveryAttributes()
	{
		return this.udpDiscoveryAttributes;
	}

	public void startup()
	{
		udpReceiverThread = new Thread(receiver);
		udpReceiverThread.setDaemon(true);
		udpReceiverThread.start();
	}

	@Override
	public void shutdown()
	{
		if (!shutdown)
		{
			shutdown = true;

			if (log.isInfoEnabled())
			{
				log.info("Shut down UDP discovery service receiver.");
			}

			try
			{
				receiver.shutdown();
				udpReceiverThread.interrupt();
			}
			catch (Exception e)
			{
				log.error("Problem occur in interrupting UDP receiver thread.");
			}

			if (log.isInfoEnabled())
			{
				log.info("Shut down UDP discovery service sender.");
			}

			try
			{
				sender.shutdown();
			}
			catch (Exception e)
			{
				log.error("Problem occur in removing broadcast via UDP sender.");
			}
		}
		else
		{
			if (log.isDebugEnabled())
			{
				log.debug("Shut down already called.");
			}
		}
	}

	public synchronized Set<DiscoveredService> getDiscoveredServices()
	{
		return discoveredServices;
	}

	private Set<IDiscoveryListener> getDiscoveryListeners()
	{
		return discoveryListeners;
	}

	public Set<IDiscoveryListener> getCopyOfDiscoveryListeners()
	{
		Set<IDiscoveryListener> copy = new HashSet<IDiscoveryListener>();
		copy.addAll(getDiscoveryListeners());
		return copy;
	}

	public boolean addDiscoveryListener(IDiscoveryListener listener)
	{
		return getDiscoveryListeners().add(listener);
	}

	public boolean removeDiscoveryListener(IDiscoveryListener listener)
	{
		return getDiscoveryListeners().remove(listener);
	}
}
