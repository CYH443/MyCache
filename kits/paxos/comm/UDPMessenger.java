package com.cachekit.kits.paxos.comm;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.cachekit.kits.paxos.PaxosUtils;
//UDP信使，每个节点都有一个
public class UDPMessenger implements CommLayer
{
	public static final int BUFFER_SIZE = 128 * 1024;
	//Tick消息周期
	public static final int UPDATE_PERIOD = 100;
	//UDP Socket
	private final DatagramSocket socket;
	private final DatagramPacket receivePacket;

	private final ReceivingThread receivingThread;
	//内部时钟线程
	private final TickingThread tickingThread;
	//调度线程
	private final DispatchingThread dispatchThread;
	// PaxosGroup
	private MessageListener listener;
	private boolean running = true;
	//存放字节数组
	private BlockingQueue<byte[]> msgQueue = new LinkedBlockingQueue<byte[]>();

	public UDPMessenger() throws SocketException, UnknownHostException
	{
		this(2440);
	}

	public UDPMessenger(int port) throws SocketException, UnknownHostException
	{
		//udp监听端口2440
		socket = new DatagramSocket(port);
		//使得服务关掉重启时立马可使用该端口
		socket.setReuseAddress(true);
		receivePacket = new DatagramPacket(new byte[BUFFER_SIZE], BUFFER_SIZE, socket.getLocalAddress(), port);

		this.receivingThread = new ReceivingThread();
		this.tickingThread = new TickingThread();
		this.dispatchThread = new DispatchingThread();
		this.receivingThread.start();
		this.tickingThread.start();
		this.dispatchThread.start();
	}

	public void setListener(MessageListener listener)
	{
		this.listener = listener;
	}

	public void sendTo(List<Member> members, byte[] message)
	{
		DatagramPacket packet = new DatagramPacket(message, message.length);
		for (Member member : members)
		{
			try
			{
				packet.setAddress(member.getAddress());
				packet.setPort(member.getPort());
				synchronized (this)
				{
					socket.send(packet);
				}
			}
			catch (IOException e)
			{
				if (running)
				{
					e.printStackTrace();
				}
			}
		}
	}

	public void sendTo(Member member, byte[] message)
	{
		DatagramPacket packet = new DatagramPacket(message, message.length);
		packet.setAddress(member.getAddress());
		packet.setPort(member.getPort());

		try
		{
			synchronized (this)
			{
				socket.send(packet);
			}
		}
		catch (IOException e)
		{
			if (running)
			{
				e.printStackTrace();
			}
		}
	}

	public void close()
	{
		this.running = false;
		this.socket.close();
		this.dispatchThread.interrupt();
	}

	/**
	 * 不断监听端口，收到消息，将字节数组放到阻塞队列中
	 */
	private class ReceivingThread extends Thread
	{
		@Override
		public void run()
		{
			while (running)
			{
				try
				{
					socket.receive(receivePacket);

					if (receivePacket.getLength() > BUFFER_SIZE)
					{
						throw new IOException("Message too big " + receivePacket.getLength());
					}
					msgQueue.put(receivePacket.getData().clone());
				}
				catch (IOException e)
				{
					if (running)
					{
						e.printStackTrace();
					}
				}
				catch (InterruptedException e)
				{
					if (running)
					{
						e.printStackTrace();
					}
				}
			}
		}
	}

	private class DispatchingThread extends Thread
	{
		@Override
		public void run()
		{
			try
			{
				while (running)
				{
					byte[] msg = msgQueue.take();
					if (running)
					{
						dispatch(msg);
					}
				}
			}
			catch (InterruptedException e)
			{
				if (running)
				{
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 100ms 执行一次
	 */
	private class TickingThread extends Thread
	{
		@Override
		public void run()
		{
			try
			{
				while (running)
				{
					dispatch(PaxosUtils.serialize(new Tick(System.currentTimeMillis())));
					sleep(UPDATE_PERIOD);
				}
			}
			catch (Exception e)
			{
				if (running)
				{
					e.printStackTrace();
				}
			}
		}
	}

	private synchronized void dispatch(byte[] msg)
	{
		if (listener != null)
		{
			listener.receive(msg);
		}
	}
}
