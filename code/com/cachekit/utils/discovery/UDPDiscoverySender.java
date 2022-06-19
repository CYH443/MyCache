package com.cachekit.utils.discovery;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.CacheInfo;
import com.cachekit.utils.discovery.UDPDiscoveryMessage.BroadcastType;
import com.cachekit.utils.serialization.StandardSerializer;

/**
 * UDP组播发送端
 */
public class UDPDiscoverySender
{
	private static final Log log = LogFactory.getLog(UDPDiscoverySender.class);

	private MulticastSocket localSocket;
	//组播ip（目标ip）
	private InetAddress multicastAddress;
	//目标端口（发送端口相同）
	private final int multicastPort;

	private final StandardSerializer serializer = new StandardSerializer();

	/**
	 * @param host 目标IP
	 * @param port 目标Port
	 */
	public UDPDiscoverySender(String host, int port) throws IOException
	{
		try
		{
			localSocket = new MulticastSocket(port);

			multicastAddress = InetAddress.getByName(host);

			multicastPort = port;
		}
		catch (IOException e)
		{
			log.error("Could not bind to multicast address [" + host + "]", e);

			throw e;
		}
	}

	public void destroy()
	{
		try
		{
			if (this.localSocket != null && !this.localSocket.isClosed())
			{
				this.localSocket.close();
			}
		}
		catch (Exception e)
		{
			log.error("Problem occur in destrying sender", e);
		}
	}

	@Override
	protected void finalize() throws Throwable
	{
		super.finalize();
		destroy();
	}

	public void send(UDPDiscoveryMessage message) throws IOException
	{
		if (this.localSocket == null)
		{
			throw new IOException("Socket is null, cannot send message.");
		}

		if (this.localSocket.isClosed())
		{
			throw new IOException("Socket is closed, cannot send message.");
		}

		if (log.isDebugEnabled())
		{
			log.debug("Send UDPDiscoveryMessage, address [" + multicastAddress + "], port [" + multicastPort
					+ "], message = " + message);
		}

		try
		{
			final byte[] bytes = serializer.serialize(message);

			final DatagramPacket packet = new DatagramPacket(bytes, bytes.length, multicastAddress, multicastPort);

			if (log.isDebugEnabled())
			{
				log.debug("Send DatagramPacket. bytes.length [" + bytes.length + "] to " + multicastAddress + ":" + multicastPort);
			}

			localSocket.send(packet);
		}
		catch (IOException e)
		{
			log.error("Error sending message", e);
			throw e;
		}
	}

	public void requestBroadcast() throws IOException
	{
		if (log.isDebugEnabled())
		{
			log.debug("Send requestBroadcast ");
		}
		//考虑UDPDiscoveryMessage中的地址和端口号是否正确
		UDPDiscoveryMessage message = new UDPDiscoveryMessage();
		message.setRequesterId(CacheInfo.listenerId);
		message.setMessageType(BroadcastType.REQUEST);
		send(message);
	}

	/**
	 * @param host 发送方自身ip
	 * @param port 发送方自身port
	 */
	public void passiveBroadcast(String host, int port, ArrayList<String> cacheNames) throws IOException
	{
		passiveBroadcast(host, port, cacheNames, CacheInfo.listenerId);
	}

	protected void passiveBroadcast(String host, int port, ArrayList<String> cacheNames, long listenerId) throws IOException
	{
		if (log.isDebugEnabled())
		{
			log.debug("Send passiveBroadcast ");
		}

		UDPDiscoveryMessage message = new UDPDiscoveryMessage();
		message.setHost(host);
		message.setPort(port);
		message.setCacheNames(cacheNames);
		message.setRequesterId(listenerId);
		message.setMessageType(BroadcastType.PASSIVE);
		send(message);
	}

	/**
	 * @param host 发送方自身ip
	 * @param port 发送方自身port
	 */
	public void removeBroadcast(String host, int port, ArrayList<String> cacheNames) throws IOException
	{
		removeBroadcast(host, port, cacheNames, CacheInfo.listenerId);
	}

	protected void removeBroadcast(String host, int port, ArrayList<String> cacheNames, long listenerId) throws IOException
	{
		if (log.isDebugEnabled())
		{
			log.debug("Send removeBroadcast ");
		}

		UDPDiscoveryMessage message = new UDPDiscoveryMessage();
		message.setHost(host);
		message.setPort(port);
		message.setCacheNames(cacheNames);
		message.setRequesterId(listenerId);
		message.setMessageType(BroadcastType.REMOVE);
		send(message);
	}
}

class MyByteArrayOutputStream extends ByteArrayOutputStream
{
	public byte[] getBytes()
	{
		return buf;
	}
}
