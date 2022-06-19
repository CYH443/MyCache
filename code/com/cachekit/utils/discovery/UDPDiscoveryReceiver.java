package com.cachekit.utils.discovery;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.CacheInfo;
import com.cachekit.core.model.IShutdownObserver;
import com.cachekit.io.IOClassLoaderWarpper;
import com.cachekit.utils.discovery.UDPDiscoveryMessage.BroadcastType;
import com.cachekit.utils.threadpool.CacheKitThreadFactory;
//该线程负责接收 UDPDiscoveryMessage 对象，封装成一个 MessageHandler 任务，交给线程池处理
public class UDPDiscoveryReceiver implements Runnable, IShutdownObserver
{
	private static final Log log = LogFactory.getLog(UDPDiscoveryReceiver.class);

	private final byte[] buffer = new byte[65536];

	private MulticastSocket socket;
	//组播ip
	private String multicastAddressString = "";
	//监听端口号
	private int multicastPort = 0;
	//线程池最大线程数
	private static final int MAX_POOL_SIZE = 2;

	private ThreadPoolExecutor pooledExecutor = null;

	private UDPDiscoveryService service = null;

	private boolean shutdown = false;

	private int cnt = 0;

	/**
	 * @param multicastAddressString 组播ip
	 * @param multicastPort 监听端口号
	 */
	public UDPDiscoveryReceiver(UDPDiscoveryService service, String multicastAddressString, int multicastPort) throws IOException
	{
		this.service = service;
		this.multicastAddressString = multicastAddressString;
		this.multicastPort = multicastPort;

		pooledExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_POOL_SIZE,
				new CacheKitThreadFactory("CacheKit-UDPDiscoveryReceiver-", Thread.MIN_PRIORITY));

		//线程池饱和，丢弃工作队列中最老的任务，重新接纳新的任务
		pooledExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardOldestPolicy());

		if (log.isInfoEnabled())
		{
			log.info("Construct listener, [" + this.multicastAddressString + ":" + this.multicastPort + "]");
		}

		try
		{
			createSocket(this.multicastAddressString, this.multicastPort);
		}
		catch (IOException ioe)
		{
			throw ioe;
		}
	}

	private void createSocket(String multicastAddressString, int multicastPort) throws IOException
	{
		try
		{
			socket = new MulticastSocket(multicastPort);
			if (log.isInfoEnabled())
			{
				log.info("Join group: [" + InetAddress.getByName(multicastAddressString) + "]");
			}
			socket.joinGroup(InetAddress.getByName(multicastAddressString));
		}
		catch (IOException e)
		{
			log.error("Could not bind to multicast address [" + InetAddress.getByName(multicastAddressString) + ":"
					+ multicastPort + "]", e);
			throw e;
		}
	}

	public Object waitForMessage() throws IOException
	{
		final DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
		ObjectInputStream objectStream = null;
		Object obj = null;
		try
		{
			if (log.isDebugEnabled())
			{
				log.debug("Wait for message.");
			}

			socket.receive(packet);

			if (log.isDebugEnabled())
			{
				log.debug("Receive packet from address [" + packet.getSocketAddress() + "]");
			}

			final ByteArrayInputStream byteStream = new ByteArrayInputStream(buffer, 0, packet.getLength());
			objectStream = new IOClassLoaderWarpper(byteStream, null);
			obj = objectStream.readObject();

			if (obj instanceof UDPDiscoveryMessage)
			{
				UDPDiscoveryMessage msg = (UDPDiscoveryMessage) obj;
				//设置发送发ip
				msg.setHost(packet.getAddress().getHostAddress());

				if (log.isDebugEnabled())
				{
					log.debug("Read object from address [" + packet.getSocketAddress() + "], object=[" + obj + "]");
				}
			}
		}
		catch (Exception e)
		{
			log.error("Error in receiving multicast packet", e);
		}
		finally
		{
			if (objectStream != null)
			{
				try
				{
					objectStream.close();
				}
				catch (IOException e)
				{
					log.error("Error in closing object stream", e);
				}
			}
		}
		return obj;
	}

	@Override
	public void run()
	{
		try
		{
			while (!shutdown)
			{
				Object obj = waitForMessage();

				cnt++;

				if (log.isDebugEnabled())
				{
					log.debug(getCnt() + " messages received.");
				}

				UDPDiscoveryMessage message = null;

				try
				{
					message = (UDPDiscoveryMessage) obj;
					if (message != null)
					{
						MessageHandler handler = new MessageHandler(message);

						pooledExecutor.execute(handler);

						if (log.isDebugEnabled())
						{
							log.debug("Passed handler to executor.");
						}
					}
					else
					{
						log.warn("Message is null");
					}
				}
				catch (ClassCastException cce)
				{
					log.warn("Receive unknown message type " + cce.getMessage());
				}
			}
		}
		catch (Exception e)
		{
			log.error("Unexpected exception in UDP receiver.", e);
			try
			{
				Thread.sleep(100);
			}
			catch (Exception e2)
			{
				log.error("Exception in sleeping", e2);
			}
		}
	}

	public void setCnt(int cnt)
	{
		this.cnt = cnt;
	}

	public int getCnt()
	{
		return cnt;
	}

	public class MessageHandler implements Runnable
	{
		private UDPDiscoveryMessage message = null;

		public MessageHandler(UDPDiscoveryMessage message)
		{
			this.message = message;
		}

		@Override
		public void run()
		{
			//忽略自身的广播消息
			if (message.getRequesterId() == CacheInfo.listenerId)
			{
				if (log.isDebugEnabled())
				{
					log.debug("Ignore message sent from self");
				}
			}
			else
			{
				if (log.isDebugEnabled())
				{
					log.debug("Process message sent from another");
					log.debug("Message = " + message);
				}
				//忽略无效的广播信息
				if (message.getHost() == null || message.getCacheNames() == null || message.getCacheNames().isEmpty())
				{
					if (log.isDebugEnabled())
					{
						log.debug("Ignore invalid message: " + message);
					}
				}
				else
				{
					processMessage();
				}
			}
		}

		private void processMessage()
		{
			DiscoveredService discoveredService = new DiscoveredService();
			//存储发送方服务的ip和port
			discoveredService.setServiceAddress(message.getHost());
			discoveredService.setCacheNames(message.getCacheNames());
			discoveredService.setServicePort(message.getPort());
			discoveredService.setLastHearTime(System.currentTimeMillis());

			if (message.getMessageType() == BroadcastType.REQUEST)
			{
				//REQUEST报文请求: 要求服务方发送 PASSIVE 广播报文
				if (log.isDebugEnabled())
				{
					log.debug("Message is a Request Broadcast, will have the service handle it.");
				}
				service.serviceRequestBroadcast();
				return;
			}
			else if (message.getMessageType() == BroadcastType.REMOVE)
			{
				//REMOVE报文: 服务端下线，要求调用方删除服务列表
				if (log.isInfoEnabled())
				{
					log.info("Remove service from set " + discoveredService);
				}
				service.removeDiscoveredService(discoveredService);
			}
			else
			{
				//PASSIVE报文: 服务端发送的报文，调用方应将该服务信息保存在服务列表
				service.addOrUpdateService(discoveredService);
			}
		}
	}

	@Override
	public void shutdown()
	{
		try
		{
			shutdown = true;
			socket.leaveGroup(InetAddress.getByName(multicastAddressString));
			socket.close();
			pooledExecutor.shutdownNow();
		}
		catch (IOException e)
		{
			log.error("Problem occur in closing socket");
		}
	}
}
