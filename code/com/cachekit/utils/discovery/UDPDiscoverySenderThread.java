package com.cachekit.utils.discovery;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
// 初始化时，创建 UDPDiscoverySender 发送一次 requestBroadcast
// run方法：创建 UDPDiscoverySender 发送一次 passiveBroadcast
// 关闭该线程时：创建 UDPDiscoverySender 发送一次 removeBroadcast
public class UDPDiscoverySenderThread implements Runnable
{
	private static final Log log = LogFactory.getLog(UDPDiscoverySenderThread.class);

	private final UDPDiscoveryAttributes attributes;

	private ArrayList<String> cacheNames = new ArrayList<String>();

	protected void setCacheNames(ArrayList<String> cacheNames)
	{
		this.cacheNames = cacheNames;
	}

	protected ArrayList<String> getCacheNames()
	{
		return cacheNames;
	}

	/**
	 * 初始化时，创建一个 UDPDiscoverySender 向组播地址发送一个 REQUEST请求，发送完关闭 UDPDiscoverySender
	 */
	public UDPDiscoverySenderThread(UDPDiscoveryAttributes attributes, ArrayList<String> cacheNames)
	{
		this.attributes = attributes;

		this.cacheNames = cacheNames;

		if (log.isDebugEnabled())
		{
			log.debug("Create sender thread for discoveryAddress = [" + attributes.getUdpDiscoveryAddr()
					+ "] and discoveryPort = [" + attributes.getUdpDiscoveryPort() + "] myHostName = ["
					+ attributes.getServiceAddress() + "] and port = [" + attributes.getServicePort() + "]");
		}

		UDPDiscoverySender sender = null;
		try
		{
			sender = new UDPDiscoverySender(attributes.getUdpDiscoveryAddr(), attributes.getUdpDiscoveryPort());
			//向服务组发送一个请求广播信息
			sender.requestBroadcast();

			if (log.isDebugEnabled())
			{
				log.debug("Sent a request broadcast to the group");
			}
		}
		catch (Exception e)
		{
			log.error("Problem in sending a Request Broadcast", e);
		}
		finally
		{
			try
			{
				if (sender != null)
				{
					//关闭 sender
					sender.destroy();
				}
			}
			catch (Exception e)
			{
				log.error("Problem in closing Request Broadcast sender", e);
			}
		}
	}

	@Override
	public void run()
	{
		UDPDiscoverySender sender = null;
		try
		{
			sender = new UDPDiscoverySender(attributes.getUdpDiscoveryAddr(), attributes.getUdpDiscoveryPort());

			sender.passiveBroadcast(attributes.getServiceAddress(), attributes.getServicePort(), cacheNames);

			if (log.isDebugEnabled())
			{
				log.debug("Call sender to issue a passive broadcast");
			}

		}
		catch (Exception e)
		{
			log.error("Problem in calling the UDP Discovery Sender [" + attributes.getUdpDiscoveryAddr() + ":"
					+ attributes.getUdpDiscoveryPort() + "]", e);
		}
		finally
		{
			if (sender != null)
			{
				try
				{
					sender.destroy();
				}
				catch (Exception e)
				{
					log.error("Problem in closing Passive Broadcast sender", e);
				}
			}
		}
	}

	/**
	 * 关闭时，创建一个 UDPDiscoverySender 向组播地址发送一个 REMOVE请求，发送完关闭 UDPDiscoverySender
	 */
	protected void shutdown()
	{
		UDPDiscoverySender sender = null;
		try
		{
			sender = new UDPDiscoverySender(attributes.getUdpDiscoveryAddr(), attributes.getUdpDiscoveryPort());

			sender.removeBroadcast(attributes.getServiceAddress(), attributes.getServicePort(), cacheNames);

			if (log.isDebugEnabled())
			{
				log.debug("Call sender to issue a remove broadcast in shudown.");
			}
		}
		catch (Exception e)
		{
			log.error("Problem in calling the UDP Discovery Sender", e);
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
				log.error("Problem in closing Remote Broadcast sender", e);
			}
		}
	}
}
