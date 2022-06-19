package com.cachekit.kits.lateral.tcp;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.access.exception.CacheException;
import com.cachekit.core.CacheInfo;
import com.cachekit.core.control.ContextCache;
import com.cachekit.core.control.ContextCacheManager;
import com.cachekit.core.model.ICacheElement;
import com.cachekit.core.model.IContextCacheManager;
import com.cachekit.core.model.IShutdownObserver;
import com.cachekit.io.IOClassLoaderWarpper;
import com.cachekit.kits.lateral.ILateralCacheListener;
import com.cachekit.kits.lateral.LateralElementDescriptor;
import com.cachekit.utils.threadpool.CacheKitThreadFactory;

/**
 * 线性组件监听器
 * 有一个独立的守护线程 serverSocket 负责监听某个端口TCP socket客户端的连接，每接受一个客户端 socket，就将其封装成一个 ConnectionHandler任务，放到线程池中运行
 * 线程池并发执行多个socket客户端的操作请求
 * 执行该runnable时，从socket中取出LateralElementDescriptor，调用contextCache执行update、get等操作
 */
public class LateralTCPReceiver<K, V> implements ILateralCacheListener<K, V>, IShutdownObserver
{
	private static final Log log = LogFactory.getLog(LateralTCPReceiver.class);

	private static final int acceptTimeOut = 1000;

	private transient IContextCacheManager cacheManager;
	//key 为端口号，value 为LateralTCPReceiver实例
	private static final HashMap<String, ILateralCacheListener<?, ?>> instances = new HashMap<String, ILateralCacheListener<?, ?>>();

	private ListenerReceiverThread receiver;

	private ITCPLateralCacheAttributes tcpLateralCacheAttributes;

	private int port;
	//CachedThreadPool
	private ExecutorService pooledExecutor;

	private int putCnt = 0;

	private int removeCnt = 0;

	private int getCnt = 0;

	private long listenerId = CacheInfo.listenerId;

	private AtomicBoolean shutdown;

	private AtomicBoolean terminated;

	/**
	 * 从 instances 集合获取 LateralTCPReceiver，没有则创建新的，并存入 instances 集合
	 */
	public synchronized static <K, V> LateralTCPReceiver<K, V> getInstance(ITCPLateralCacheAttributes attr, IContextCacheManager cacheMgr)
	{
		@SuppressWarnings("unchecked")
		LateralTCPReceiver<K, V> receiver = (LateralTCPReceiver<K, V>) instances.get(String.valueOf(attr.getTcpListenerPort()));

		if (receiver == null)
		{
			receiver = new LateralTCPReceiver<K, V>(attr);

			receiver.init();
			receiver.setCacheManager(cacheMgr);

			instances.put(String.valueOf(attr.getTcpListenerPort()), receiver);

			if (log.isInfoEnabled())
			{
				log.info("Create new listener " + attr.getTcpListenerPort());
			}
		}

		return receiver;
	}

	protected LateralTCPReceiver(ITCPLateralCacheAttributes attr)
	{
		this.setTcpLateralCacheAttributes(attr);
	}

	/**
	 * 初始化线程池和接收线程
	 */
	@Override
	public synchronized void init()
	{
		try
		{
			this.port = getTcpLateralCacheAttributes().getTcpListenerPort();

			pooledExecutor = Executors.newCachedThreadPool(new CacheKitThreadFactory("CacheKit-LateralTCPReceiver-"));
			terminated = new AtomicBoolean(false);
			shutdown = new AtomicBoolean(false);

			log.info("Listen on port " + port);

			ServerSocket serverSocket = new ServerSocket(port);
			serverSocket.setSoTimeout(acceptTimeOut);

			receiver = new ListenerReceiverThread(serverSocket);
			receiver.setDaemon(true);
			receiver.start();
		}
		catch (IOException ex)
		{
			throw new IllegalStateException(ex);
		}
	}

	@Override
	public void setListenerId(long id) throws IOException
	{
		this.listenerId = id;
		if (log.isDebugEnabled())
		{
			log.debug("Set listenerId = " + id);
		}
	}

	@Override
	public long getListenerId() throws IOException
	{
		return this.listenerId;
	}

	@Override
	public void handlePut(ICacheElement<K, V> element) throws IOException
	{
		putCnt++;
		if (log.isInfoEnabled())
		{
			if (getPutCnt() % 100 == 0)
			{
				log.info("Put count (port " + getTcpLateralCacheAttributes().getTcpListenerPort() + ") = " + getPutCnt());
			}
		}

		if (log.isDebugEnabled())
		{
			log.debug("handlePut cacheName=" + element.getCacheName() + ", key=" + element.getKey());
		}

		getCache(element.getCacheName()).localUpdate(element);
	}

	@Override
	public void handleRemove(String cacheName, K key) throws IOException
	{
		removeCnt++;
		if (log.isInfoEnabled())
		{
			if (getRemoveCnt() % 100 == 0)
			{
				log.info("Remove Count = " + getRemoveCnt());
			}
		}

		if (log.isDebugEnabled())
		{
			log.debug("handleRemove cacheName=" + cacheName + ", key=" + key);
		}

		getCache(cacheName).localRemove(key);
	}

	@Override
	public void handleRemoveAll(String cacheName) throws IOException
	{
		if (log.isDebugEnabled())
		{
			log.debug("handleRemoveAll cacheName=" + cacheName);
		}

		getCache(cacheName).localRemoveAll();
	}

	public ICacheElement<K, V> handleGet(String cacheName, K key) throws IOException
	{
		getCnt++;
		if (log.isInfoEnabled())
		{
			if (getGetCnt() % 100 == 0)
			{
				log.info("Get Count (port " + getTcpLateralCacheAttributes().getTcpListenerPort() + ") = " + getGetCnt());
			}
		}

		if (log.isDebugEnabled())
		{
			log.debug("handleGet cacheName=" + cacheName + ", key = " + key);
		}
		//从内存组件和磁盘组件中读取
		return getCache(cacheName).localGet(key);
	}

	public Map<K, ICacheElement<K, V>> handleGetMatching(String cacheName, String pattern) throws IOException
	{
		getCnt++;
		if (log.isInfoEnabled())
		{
			if (getGetCnt() % 100 == 0)
			{
				log.info("getMatching Count (port " + getTcpLateralCacheAttributes().getTcpListenerPort() + ") = " + getGetCnt());
			}
		}

		if (log.isDebugEnabled())
		{
			log.debug("handleGetMatching cacheName=" + cacheName + ", pattern = " + pattern);
		}

		return getCache(cacheName).localGetMatching(pattern);
	}

	public Set<K> handleGetKeySet(String cacheName) throws IOException
	{
		return getCache(cacheName).getKeySet(true);
	}

	@Override
	public void handleDispose(String cacheName) throws IOException
	{
		if (log.isInfoEnabled())
		{
			log.info("handleDispose cacheName=" + cacheName + " | Ignoring message.  Do not dispose from remote.");
		}

		terminated.set(true);
	}

	@Override
	public synchronized void dispose()
	{
		terminated.set(true);
		notify();

		pooledExecutor.shutdownNow();
	}

	protected ContextCache<K, V> getCache(String name)
	{
		if (getCacheManager() == null)
		{
			try
			{
				setCacheManager(ContextCacheManager.getInstance());
			}
			catch (CacheException e)
			{
				throw new RuntimeException("Could not retrieve cache manager instance", e);
			}

			if (log.isDebugEnabled())
			{
				log.debug("cacheMgr = " + getCacheManager());
			}
		}

		return getCacheManager().getCache(name);
	}

	public int getPutCnt()
	{
		return putCnt;
	}

	public int getGetCnt()
	{
		return getCnt;
	}

	public int getRemoveCnt()
	{
		return removeCnt;
	}

	@Override
	public void setCacheManager(IContextCacheManager cacheMgr)
	{
		this.cacheManager = cacheMgr;
	}

	@Override
	public IContextCacheManager getCacheManager()
	{
		return cacheManager;
	}

	public void setTcpLateralCacheAttributes(ITCPLateralCacheAttributes tcpLateralCacheAttributes)
	{
		this.tcpLateralCacheAttributes = tcpLateralCacheAttributes;
	}

	public ITCPLateralCacheAttributes getTcpLateralCacheAttributes()
	{
		return tcpLateralCacheAttributes;
	}

	/**
	 * 不断地等待客户端Socket的连接，每接收一个socket，就将其封装成一个ConnectionHandler任务，交给线程池处理
	 */
	public class ListenerReceiverThread extends Thread
	{
		private final ServerSocket serverSocket;

		public ListenerReceiverThread(ServerSocket serverSocket)
		{
			super();
			this.serverSocket = serverSocket;
		}

		@Override
		public void run()
		{
			try
			{
				ConnectionHandler handler;
				outer: while (true)
				{
					if (log.isDebugEnabled())
					{
						log.debug("Wait for clients to connect ");
					}

					Socket socket = null;
					inner: while (true)
					{
						if (terminated.get())
						{
							if (log.isDebugEnabled())
							{
								log.debug("Thread terminated, exiting gracefully");
							}
							break outer;
						}

						try
						{
							socket = serverSocket.accept();
							break inner;
						}
						catch (SocketTimeoutException e)
						{
							continue inner;
						}
					}

					if (socket != null && log.isDebugEnabled())
					{
						InetAddress inetAddress = socket.getInetAddress();
						log.debug("connect to client at " + inetAddress);
					}

					handler = new ConnectionHandler(socket);
					pooledExecutor.execute(handler);
				}
			}
			catch (IOException e)
			{
				log.error("Exception caught in TCP listener", e);
			}
			finally
			{
				if (serverSocket != null)
				{
					try
					{
						serverSocket.close();
					}
					catch (IOException e)
					{
						log.error("Exception caught in closing socket", e);
					}
				}
			}
		}
	}

	/**
	 *  不断的从socket的对象输入流中读取LateralElementDescriptor进行处理
	 *  需要判断LateralElementDescriptor是来自自己发的还是别人发的
	 */
	public class ConnectionHandler implements Runnable
	{
		private final Socket socket;

		public ConnectionHandler(Socket socket)
		{
			this.socket = socket;
		}

		//从socket中获取输入流，从输入流读取LateralElementDescriptor对象，然后调用handle()方法处理
		@SuppressWarnings("unchecked")
		@Override
		public void run()
		{
			ObjectInputStream ois;

			try
			{
				ois = new IOClassLoaderWarpper(socket.getInputStream(), null);
			}
			catch (Exception e)
			{
				log.error("Could not open ObjectInputStream on " + socket, e);
				return;
			}

			LateralElementDescriptor<K, V> led;

			try
			{
				while (true)
				{
					led = (LateralElementDescriptor<K, V>) ois.readObject();

					if (led == null)
					{
						log.debug("LateralElementDescriptor is null");
						continue;
					}
					if (led.requesterId == getListenerId())
					{
						log.debug("from self");
					}
					else
					{
						if (log.isDebugEnabled())
						{
							log.debug("Receive LateralElementDescriptor from another" + "led = " + led
									+ ", led.command = " + led.command + ", led.ce = " + led.ce);
						}

						handle(led);
					}
				}
			}
			catch (EOFException e)
			{
				log.info("java.io.EOFException, close connection." + e.getMessage());
			}
			catch (SocketException e)
			{
				log.info("java.net.SocketException, close connection." + e.getMessage());
			}
			catch (Exception e)
			{
				log.error("Unexpected exception.", e);
			}

			try
			{
				ois.close();
			}
			catch (IOException e)
			{
				log.error("Could not close object input stream.", e);
			}
		}

		private void handle(LateralElementDescriptor<K, V> led) throws IOException
		{
			String cacheName = led.ce.getCacheName();
			K key = led.ce.getKey();
			Serializable obj = null;

			switch (led.command)
			{
				case UPDATE:
					handlePut(led.ce);
					break;

				case REMOVE:
					if (led.valHashCode != -1)
					{
						if (getTcpLateralCacheAttributes().isFilterRemoveByHashCode())
						{
							ICacheElement<K, V> ce = getCache(cacheName).localGet(key);
							if (ce != null)
							{
								if (ce.getVal().hashCode() == led.valHashCode)
								{
									if (log.isDebugEnabled())
									{
										log.debug("Filtering detected identical hashCode [" + led.valHashCode + "], not allow a remove for led " + led);
									}
									return;
								}

							}
						}
					}
					handleRemove(cacheName, key);
					break;

				case REMOVEALL:
					handleRemoveAll(cacheName);
					break;

				case GET:
					obj = handleGet(cacheName, key);
					break;

				case GET_MATCHING:
					obj = (Serializable) handleGetMatching(cacheName, (String) key);
					break;

				case GET_KEYSET:
					obj = (Serializable) handleGetKeySet(cacheName);
					break;

				default:
					break;
			}

			if (obj != null)
			{
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				oos.writeObject(obj);
				oos.flush();
			}
		}
	}

	/**
	 * 关闭 serverSocket 接收线程
	 */
	@Override
	public void shutdown()
	{
		if (shutdown.compareAndSet(false, true))
		{
			if (log.isInfoEnabled())
			{
				log.info("Shutdown TCP Lateral receiver.");
			}

			receiver.interrupt();
		}
		else
		{
			if (log.isDebugEnabled())
			{
				log.debug("Shutdown already called.");
			}
		}
	}
}
