package com.cachekit.kits.disk.indexed;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.CacheConstants;
import com.cachekit.core.control.group.GroupAttrName;
import com.cachekit.core.control.group.GroupId;
import com.cachekit.core.logger.ICacheEvent;
import com.cachekit.core.logger.ICacheEventWrapper;
import com.cachekit.core.model.ICacheElement;
import com.cachekit.core.model.IElementSerializer;
import com.cachekit.core.stats.IStatElement;
import com.cachekit.core.stats.IStats;
import com.cachekit.core.stats.StatElement;
import com.cachekit.core.stats.Stats;
import com.cachekit.kits.KitCacheAttributes;
import com.cachekit.kits.disk.AbstractDiskCache;
import com.cachekit.kits.disk.IDiskCacheAttributes.DiskLimitType;
import com.cachekit.utils.struct.AbstractLRUMap;
import com.cachekit.utils.struct.LRUMap;
import com.cachekit.utils.timing.ElapsedTimer;

public class IndexedDiskCache<K, V> extends AbstractDiskCache<K, V>
{
	private static final Log log = LogFactory.getLog(IndexedDiskCache.class);

	protected final String cacheLogger;
	//文件前缀名
	private final String fileName;

	private IndexedDisk dataFile;

	private IndexedDisk keyFile;

	private Map<K, IndexedDiskElementDescriptor> keyHash;

	private final int maxKeySize;
	//存储KeyFile和DataFile的文件夹
	private File cacheFileDir;

	private boolean doRecycle = true;

	private boolean isRealTimeOptimizationEnabled = true;

	private boolean isShutdownOptimizationEnabled = true;

	private boolean isOptimizing = false;

	private int timesOptimized = 0;

	private volatile Thread currentOptimizationThread;

	private int removeCount = 0;

	private boolean queueInput = false;
	//待插入的元素，暂时存放在队列中
	private final ConcurrentSkipListSet<IndexedDiskElementDescriptor> queuedPutList = new ConcurrentSkipListSet<IndexedDiskElementDescriptor>(new PositionComparator());
	//回收集合
	private ConcurrentSkipListSet<IndexedDiskElementDescriptor> recycle;

	private final IndexedDiskCacheAttributes cattr;

	private int recycleCnt = 0;

	private int startupSize = 0;
	//可回收利用的空间
	private AtomicLong bytesFree = new AtomicLong(0);

	private DiskLimitType diskLimitType = DiskLimitType.COUNT;

	private AtomicInteger hitCount = new AtomicInteger(0);

	protected ReentrantReadWriteLock storageLock = new ReentrantReadWriteLock();

	public IndexedDiskCache(IndexedDiskCacheAttributes cacheAttributes)
	{
		this(cacheAttributes, null);
	}

	public IndexedDiskCache(IndexedDiskCacheAttributes cattr, IElementSerializer elementSerializer)
	{
		//初始化事件队列和 purgatory 集合
		super(cattr);

		setElementSerializer(elementSerializer);

		this.cattr = cattr;
		this.maxKeySize = cattr.getMaxKeySize();
		this.isRealTimeOptimizationEnabled = cattr.getOptimizeAtRemoveCount() > 0;
		this.isShutdownOptimizationEnabled = cattr.isOptimizeOnShutdown();
		this.cacheLogger = "CacheName [" + getCacheName() + "] ";
		this.diskLimitType = cattr.getDiskLimitType();
		this.fileName = getCacheName().replaceAll("[^a-zA-Z0-9-_\\.]", "_");

		try
		{
		    //初始化文件夹路径
			initializeFileSystem(cattr);
            //初始化 key 和 data 文件路径
			initializeKeysAndData(cattr);

			initializeRecycleBin();

			setAlive(true);

			if (log.isInfoEnabled())
			{
				log.info(cacheLogger + "Indexed Disk Cache is alive.");
			}

			if (isRealTimeOptimizationEnabled && keyHash.size() > 0)
			{
				doOptimizeRealTime();
			}
		}
		catch (IOException e)
		{
			log.error(cacheLogger + "fail to initialize for fileName: " + fileName + " and directory: "
					+ this.cacheFileDir.getAbsolutePath(), e);
		}
	}

	private void initializeFileSystem(IndexedDiskCacheAttributes cattr)
	{
		this.cacheFileDir = cattr.getDiskPath();

		if (log.isInfoEnabled())
		{
			log.info(cacheLogger + "cache file root directory: " + cacheFileDir);
		}
	}

	private void initializeKeysAndData(IndexedDiskCacheAttributes cattr) throws IOException
	{
		this.dataFile = new IndexedDisk(new File(cacheFileDir, fileName + ".data"), getElementSerializer());
		this.keyFile = new IndexedDisk(new File(cacheFileDir, fileName + ".key"), getElementSerializer());
		//默认是false
		if (cattr.isClearDiskOnStartup())
		{
			if (log.isInfoEnabled())
			{
				log.info(cacheLogger + "ClearDiskOnStartup is set to true. Ingnore any persisted data.");
			}
			//初始化 keyHash 集合并清空 data 磁盘文件
			initializeEmptyStore();
		}
		else if (keyFile.length() > 0)
		{
			//将keyFile文件的内容加载到keyHash，然后检查keyHash与DataFile是否一致，不一致就清空重新存储
			initializeStoreFromPersistedData();
		}
		//keyFile.length()==0
		else
		{
			initializeEmptyStore();
		}
	}

	/**
	 * 初始化 keyHash 集合并清空 data 磁盘文件
	 */
	private void initializeEmptyStore() throws IOException
	{
		//初始化 keyHash 集合
		initializeKeyMap();

		if (dataFile.length() > 0)
		{
			dataFile.reset();
		}
	}

    /**
     * 检查磁盘是否已经存储了key，没有则清空从零开始存储，
	 * 有的话检查key与data是否一致，不一致清空从零开始，一致则接着keyHash的位置存储
     */
	private void initializeStoreFromPersistedData() throws IOException
	{
		loadKeys();

		if (keyHash.isEmpty())
		{
			dataFile.reset();
		}
		else
		{
		    //检查磁盘文件key所对应的文件大小 和 dataFile实际文件大小是否一致，不检查是否有重复
			boolean isOk = checkKeyDataConsistency(false);

			if (!isOk)
			{
				//磁盘数据不一致(数据损坏)，全部清除
				keyHash.clear();
				keyFile.reset();
				dataFile.reset();

				log.warn(cacheLogger + "corruption detected.  reset data and keys files.");
			}
			else
			{
				synchronized (this)
				{
					startupSize = keyHash.size();
				}
			}
		}
	}

    /**
     * 将磁盘文件中的 key 加载到内存 keyHash 中
     */
	protected void loadKeys()
	{
		if (log.isDebugEnabled())
		{
			log.debug(cacheLogger + "load keys for " + keyFile.toString());
		}

		storageLock.writeLock().lock();

		try
		{
			initializeKeyMap();

			HashMap<K, IndexedDiskElementDescriptor> keys = keyFile.readObject(
					new IndexedDiskElementDescriptor(0, (int) keyFile.length() - IndexedDisk.HEADER_SIZE_BYTES));

			if (keys != null)
			{
				if (log.isDebugEnabled())
				{
					log.debug(cacheLogger + "found " + keys.size() + " in keys file.");
				}

				keyHash.putAll(keys);

				if (log.isInfoEnabled())
				{
					log.info(cacheLogger + "load keys from [" + fileName + "], key count: " + keyHash.size()
							+ "; up to " + maxKeySize + " will be available.");
				}
			}

			if (log.isDebugEnabled())
			{
				dump(false);
			}
		}
		catch (Exception e)
		{
			log.error(cacheLogger + "error occur in loading keys for file " + fileName, e);
		}
		finally
		{
			storageLock.writeLock().unlock();
		}
	}

	private boolean checkKeyDataConsistency(boolean checkForDedOverlaps)
	{
		ElapsedTimer timer = new ElapsedTimer();

		boolean isOk = true;
		long fileLength = 0;
		try
		{
			fileLength = dataFile.length();

			//检查 keyHash 中描述符的所对应的数据大小是否超过实际数据文件大小
			for (Map.Entry<K, IndexedDiskElementDescriptor> e : keyHash.entrySet())
			{
				IndexedDiskElementDescriptor ded = e.getValue();

				isOk = ded.pos + IndexedDisk.HEADER_SIZE_BYTES + ded.len <= fileLength;

				if (!isOk)
				{
					log.warn(cacheLogger + "the dataFile is corrupted!" + "\n dataFile.length() = " + fileLength + "\n ded.pos = " + ded.pos);
					break;
				}
			}
			//检查keyHash中的描述符的位置是否重复
			if (isOk && checkForDedOverlaps)
			{
				IndexedDiskElementDescriptor[] deds = createPositionSortedDescriptorList();
				isOk = checkForDedOverlaps(deds);
			}
		}
		catch (IOException e)
		{
			log.error(e);
			isOk = false;
		}

		if (log.isInfoEnabled())
		{
			log.info(cacheLogger + "finish inital consistency check, isOk = " + isOk + " in " + timer.getElapsedTimeString());
		}

		return isOk;
	}

	/**
	 * 检查是否有重复的 IndexedDiskElementDescriptor 元素
	 */
	protected boolean checkForDedOverlaps(IndexedDiskElementDescriptor[] sortedDescriptors)
	{
		long start = System.currentTimeMillis();
		boolean isOk = true;
		long expectedNextPos = 0;
		for (int i = 0; i < sortedDescriptors.length; i++)
		{
			IndexedDiskElementDescriptor ded = sortedDescriptors[i];
			if (expectedNextPos > ded.pos)
			{
				log.error(cacheLogger + "corrupt file: overlapping deds " + ded);
				isOk = false;
				break;
			}
			else
			{
				expectedNextPos = ded.pos + IndexedDisk.HEADER_SIZE_BYTES + ded.len;
			}
		}
		long end = System.currentTimeMillis();
		if (log.isDebugEnabled())
		{
			log.debug(cacheLogger + "check for DiskElementDescriptor overlaps took " + (end - start) + " ms.");
		}

		return isOk;
	}

    /**
     * 将 keyHash 作为一个整体存入到磁盘
     */
	protected void saveKeys()
	{
		try
		{
			if (log.isInfoEnabled())
			{
				log.info(cacheLogger + "save keys to: " + fileName + ", key count: " + keyHash.size());
			}

			keyFile.reset();

			HashMap<K, IndexedDiskElementDescriptor> keys = new HashMap<K, IndexedDiskElementDescriptor>();
			keys.putAll(keyHash);

			if (keys.size() > 0)
			{
				keyFile.writeObject(keys, 0);
			}

			if (log.isInfoEnabled())
			{
				log.info(cacheLogger + "finish saving keys.");
			}
		}
		catch (IOException e)
		{
			log.error(cacheLogger + "error occur in storing keys.", e);
		}
	}

	/**
	 * 直接往磁盘进行更新
	 */
	@Override
	protected void processUpdate(ICacheElement<K, V> ce)
	{
		if (!isAlive())
		{
			log.error(cacheLogger + "no alive, abort to put of key = " + ce.getKey());
			return;
		}

		if (log.isDebugEnabled())
		{
			log.debug(cacheLogger + "store element on disk, key: " + ce.getKey());
		}

		IndexedDiskElementDescriptor ded = null;

		IndexedDiskElementDescriptor old = null;

		try
		{
			byte[] data = getElementSerializer().serialize(ce);

			storageLock.writeLock().lock();
			try
			{
				old = keyHash.get(ce.getKey());

				if (old != null && data.length <= old.len)
				{
					// 如果存在相同的key并且旧的数据长度大于新数据长度，则在旧的位置直接覆盖旧元素
					ded = old;
					ded.len = data.length;
				}
				else
				{
					//在data文件末尾插入
					ded = new IndexedDiskElementDescriptor(dataFile.length(), data.length);

					if (doRecycle)
					{
						// 从垃圾列表中找到 数据大小 >= 插入数据大小 的第一个元素
						IndexedDiskElementDescriptor rep = recycle.ceiling(ded);
						if (rep != null)
						{
							recycle.remove(rep);
							ded = rep;
							ded.len = data.length;
							recycleCnt++;
							this.adjustBytesFree(ded, false);
							if (log.isDebugEnabled())
							{
								log.debug(cacheLogger + "use recycled ded " + ded.pos + " rep.len = " + rep.len + " ded.len = " + ded.len);
							}
						}
					}

					keyHash.put(ce.getKey(), ded);

					if (queueInput)
					{
						queuedPutList.add(ded);
						if (log.isDebugEnabled())
						{
							log.debug(cacheLogger + "add to queued put list." + queuedPutList.size());
						}
					}

					if (old != null)
					{
						addToRecycleBin(old);
					}
				}
				//没有更新byteFree大小？？？？？
				dataFile.write(ded, data);
			}
			finally
			{
				storageLock.writeLock().unlock();
			}

			if (log.isDebugEnabled())
			{
				log.debug(cacheLogger + "put to file: " + fileName + ", key: " + ce.getKey() + ", position: " + ded.pos
						+ ", size: " + ded.len);
			}
		}
		catch (IOException e)
		{
			log.error(cacheLogger + "fail to update element, key: " + ce.getKey() + " old: " + old, e);
		}
	}

	/**
	 * 从磁盘读取元素
	 */
	@Override
	protected ICacheElement<K, V> processGet(K key)
	{
		if (!isAlive())
		{
			log.error(cacheLogger + "no alive so returning null for key = " + key);
			return null;
		}

		if (log.isDebugEnabled())
		{
			log.debug(cacheLogger + "try to get from disk: " + key);
		}

		ICacheElement<K, V> object = null;
		try
		{
			storageLock.readLock().lock();
			try
			{
				//从磁盘 data 文件中读取数据
				object = readElement(key);
			}
			finally
			{
				storageLock.readLock().unlock();
			}

			if (object != null)
			{
				hitCount.incrementAndGet();
			}
		}
		catch (IOException ioe)
		{
			log.error(cacheLogger + "fail to get from disk, key = " + key, ioe);
			reset();
		}
		return object;
	}

	@Override
	public Map<K, ICacheElement<K, V>> processGetMatching(String pattern)
	{
		Map<K, ICacheElement<K, V>> elements = new HashMap<K, ICacheElement<K, V>>();
		Set<K> keyArray = null;
		storageLock.readLock().lock();
		try
		{
			keyArray = new HashSet<K>(keyHash.keySet());
		}
		finally
		{
			storageLock.readLock().unlock();
		}

		Set<K> matchingKeys = getKeyMatcher().getMatchingKeysFromArray(pattern, keyArray);

		for (K key : matchingKeys)
		{
			ICacheElement<K, V> element = processGet(key);
			if (element != null)
			{
				elements.put(key, element);
			}
		}
		return elements;
	}

    /**
     * 从磁盘data文件中读数据
     */
	private ICacheElement<K, V> readElement(K key) throws IOException
	{
		ICacheElement<K, V> object = null;

		IndexedDiskElementDescriptor ded = keyHash.get(key);

		if (ded != null)
		{
			if (log.isDebugEnabled())
			{
				log.debug(cacheLogger + "find on disk, key: " + key);
			}
			try
			{
				ICacheElement<K, V> readObject = dataFile.readObject(ded);
				object = readObject;
			}
			catch (IOException e)
			{
				log.error(cacheLogger + "IO Exception, error occur in reading object from file", e);
				throw e;
			}
			catch (Exception e)
			{
				log.error(cacheLogger + "error occur in reading object from file", e);
				throw new IOException(cacheLogger + "error occur in reading object from disk. " + e.getMessage());
			}
		}

		return object;
	}

	@Override
	public Set<K> getKeySet() throws IOException
	{
		HashSet<K> keys = new HashSet<K>();

		storageLock.readLock().lock();

		try
		{
			keys.addAll(this.keyHash.keySet());
		}
		finally
		{
			storageLock.readLock().unlock();
		}

		return keys;
	}

	@Override
	protected boolean processRemove(K key)
	{
		if (!isAlive())
		{
			log.error(cacheLogger + "no alive so returning false for key = " + key);
			return false;
		}

		if (key == null)
		{
			return false;
		}

		boolean reset = false;
		boolean removed = false;
		try
		{
			storageLock.writeLock().lock();

			if (key instanceof String && key.toString().endsWith(CacheConstants.NAME_COMPONENT_DELIMITER))
			{
				removed = performPartialKeyRemoval((String) key);
			}
			else if (key instanceof GroupAttrName && ((GroupAttrName<?>) key).attrName == null)
			{
				removed = performGroupRemoval(((GroupAttrName<?>) key).groupId);
			}
			else
			{
				removed = performSingleKeyRemoval(key);
			}
		}
		finally
		{
			storageLock.writeLock().unlock();
		}

		if (reset)
		{
			reset();
		}

		if (removed)
		{
			//removeCount次数达到阈值，进行磁盘文件优化
			doOptimizeRealTime();
		}

		return removed;
	}

	private boolean performPartialKeyRemoval(String key)
	{
		boolean removed = false;

		List<K> itemsToRemove = new LinkedList<K>();

		for (K k : keyHash.keySet())
		{
			if (k instanceof String && k.toString().startsWith(key))
			{
				itemsToRemove.add(k);
			}
		}

		for (K fullKey : itemsToRemove)
		{
			performSingleKeyRemoval(fullKey);
			removed = true;
		}

		return removed;
	}

	private boolean performGroupRemoval(GroupId key)
	{
		boolean removed = false;

		List<K> itemsToRemove = new LinkedList<K>();

		for (K k : keyHash.keySet())
		{
			if (k instanceof GroupAttrName && ((GroupAttrName<?>) k).groupId.equals(key))
			{
				itemsToRemove.add(k);
			}
		}

		for (K fullKey : itemsToRemove)
		{
			performSingleKeyRemoval(fullKey);
			removed = true;
		}

		return removed;
	}

	/**
	 * 从KeyHash中删除，然后加入可重用链表，更新byteFree大小
	 */
	private boolean performSingleKeyRemoval(K key)
	{
		boolean removed;
		IndexedDiskElementDescriptor ded = keyHash.remove(key);
		removed = ded != null;
		addToRecycleBin(ded);

		if (log.isDebugEnabled())
		{
			log.debug(cacheLogger + "disk removal: removed from key hash, key [" + key + "] removed = " + removed);
		}
		return removed;
	}

	@Override
	public void processRemoveAll()
	{
		ICacheEvent<String> cacheEvent = createICacheEvent(getCacheName(), "all", ICacheEventWrapper.REMOVEALL_EVENT);
		try
		{
			reset();
		}
		finally
		{
			cacheEventLogger(cacheEvent);
		}
	}

	/**
	 * 删除 key 和 data 文件，并重新创建keyFile和DataFile，初始化 KeyHash 和 recycle
	 */
	private void reset()
	{
		if (log.isWarnEnabled())
		{
			log.warn(cacheLogger + "reset cache");
		}

		try
		{
			storageLock.writeLock().lock();

			if (dataFile != null)
			{
				//关闭 FileChannel 通道
				dataFile.close();
			}
			File dataFileTemp = new File(cacheFileDir, fileName + ".data");
			//删除文件，删除前要关闭该文件的所有流和通道
			boolean result = dataFileTemp.delete();
			if (!result && log.isDebugEnabled())
			{
				log.debug("Could not delete file " + dataFileTemp);
			}

			if (keyFile != null)
			{
				//关闭 key的FileChannel 通道
				keyFile.close();
			}
			File keyFileTemp = new File(cacheFileDir, fileName + ".key");
			result = keyFileTemp.delete();
			if (!result && log.isDebugEnabled())
			{
				log.debug("Could not delete file " + keyFileTemp);
			}

			dataFile = new IndexedDisk(new File(cacheFileDir, fileName + ".data"), getElementSerializer());
			keyFile = new IndexedDisk(new File(cacheFileDir, fileName + ".key"), getElementSerializer());

			initializeRecycleBin();

			initializeKeyMap();
		}
		catch (IOException e)
		{
			log.error(cacheLogger + "fail to reset state", e);
		}
		finally
		{
			storageLock.writeLock().unlock();
		}
	}

	private void initializeRecycleBin()
	{
		recycle = new ConcurrentSkipListSet<IndexedDiskElementDescriptor>();
	}

    /**
     * 初始化 key 集合
     */
	private void initializeKeyMap()
	{
		keyHash = null;
		if (maxKeySize >= 0)
		{
			if (this.diskLimitType == DiskLimitType.COUNT)
			{
				keyHash = new LRUMapCountLimited(maxKeySize);
			}
			else
			{
				keyHash = new LRUMapSizeLimited(maxKeySize);
			}

			if (log.isInfoEnabled())
			{
				log.info(cacheLogger + "set maxKeySize to: '" + maxKeySize + "'");
			}
		}
		else
		{
			keyHash = new HashMap<K, IndexedDiskElementDescriptor>();
			if (log.isInfoEnabled())
			{
				log.info(cacheLogger + "set maxKeySize to unlimited'");
			}
		}
	}

	@Override
	public void processDispose()
	{
		ICacheEvent<String> cacheEvent = createICacheEvent(getCacheName(), "none", ICacheEventWrapper.DISPOSE_EVENT);
		try
		{
			Runnable runner = new Runnable() {
				@Override
				public void run()
				{
					disposeInternal();
				}
			};
			Thread thread = new Thread(runner, "IndexedDiskCache-DisposalThread");
			thread.start();
			try
			{
				thread.join(60 * 1000);
			}
			catch (InterruptedException ex)
			{
				log.error(cacheLogger + "interrupted while waiting for disposal thread to finish.", ex);
			}
		}
		finally
		{
			cacheEventLogger(cacheEvent);
		}
	}

	protected void disposeInternal()
	{
		if (!isAlive())
		{
			log.error(cacheLogger + "no alive and dispose was called, filename: " + fileName);
			return;
		}

		setAlive(false);

		Thread optimizationThread = currentOptimizationThread;
		if (isRealTimeOptimizationEnabled && optimizationThread != null)
		{
			if (log.isDebugEnabled())
			{
				log.debug(cacheLogger + "in dispose, optimization already " + "in progress; waiting for completion.");
			}
			try
			{
				optimizationThread.join();
			}
			catch (InterruptedException e)
			{
				log.error(cacheLogger + "unable to join current optimization thread.", e);
			}
		}
		else if (isShutdownOptimizationEnabled && this.getBytesFree() > 0)
		{
			optimizeFile();
		}

		saveKeys();

		try
		{
			if (log.isDebugEnabled())
			{
				log.debug(cacheLogger + "close files, base filename: " + fileName);
			}
			dataFile.close();
			dataFile = null;
			keyFile.close();
			keyFile = null;
		}
		catch (IOException e)
		{
			log.error(cacheLogger + "fail to close files in dispose, filename: " + fileName, e);
		}

		if (log.isInfoEnabled())
		{
			log.info(cacheLogger + "shutdown complete.");
		}
	}

	/**
	 * 更新空闲空间大小，加入可重用队列
	 */
	protected void addToRecycleBin(IndexedDiskElementDescriptor ded)
	{
		if (ded != null)
		{
			storageLock.readLock().lock();
			try
			{
				//更新bytesFree（AtomicLong ）变量
				this.adjustBytesFree(ded, true);

				if (doRecycle)
				{
					//加入可重用列表
					recycle.add(ded);
					if (log.isDebugEnabled())
					{
						log.debug(cacheLogger + "recycled ded" + ded);
					}
				}
			}
			finally
			{
				storageLock.readLock().unlock();
			}
		}
	}

	/**
	 * 创建 currentOptimizationThread，执行 optimizeFile()方法
	 */
	protected void doOptimizeRealTime()
	{
		if (isRealTimeOptimizationEnabled && !isOptimizing && removeCount++ >= cattr.getOptimizeAtRemoveCount())
		{
			isOptimizing = true;

			if (log.isInfoEnabled())
			{
				log.info(cacheLogger + "optimize file. removeCount [" + removeCount + "] OptimizeAtRemoveCount ["
						+ cattr.getOptimizeAtRemoveCount() + "]");
			}

			if (currentOptimizationThread == null)
			{
				storageLock.writeLock().lock();

				try
				{
					if (currentOptimizationThread == null)
					{
						currentOptimizationThread = new Thread(new Runnable() {
							@Override
							public void run()
							{
								optimizeFile();

								currentOptimizationThread = null;
							}
						}, "IndexedDiskCache-OptimizationThread");
					}
				}
				finally
				{
					storageLock.writeLock().unlock();
				}

				if (currentOptimizationThread != null)
				{
					currentOptimizationThread.start();
				}
			}
		}
	}

	/**
	 * 整理文件碎片，去除文件空隙，删除垃圾链表(可重用空间也初始化为0)
	 */
	protected void optimizeFile()
	{
		ElapsedTimer timer = new ElapsedTimer();
		timesOptimized++;
		if (log.isInfoEnabled())
		{
			log.info(cacheLogger + "begin to optimize " + timesOptimized);
		}

		IndexedDiskElementDescriptor[] defragList = null;

		storageLock.writeLock().lock();

		try
		{
			queueInput = true;
			doRecycle = false;
			defragList = createPositionSortedDescriptorList();
		}
		finally
		{
			storageLock.writeLock().unlock();
		}
		//整理文件空隙
		long expectedNextPos = defragFile(defragList, 0);

		storageLock.writeLock().lock();

		try
		{
			try
			{
				if (!queuedPutList.isEmpty())
				{
					//将 queuedPutList 队列中元素填充到 DataFile 文件末尾
					defragList = queuedPutList.toArray(new IndexedDiskElementDescriptor[queuedPutList.size()]);

					expectedNextPos = defragFile(defragList, expectedNextPos);
				}
				dataFile.truncate(expectedNextPos);
			}
			catch (IOException e)
			{
				log.error(cacheLogger + "error occur in optimizing queued puts.", e);
			}

			removeCount = 0;
			resetBytesFree();
			initializeRecycleBin();
			queuedPutList.clear();
			queueInput = false;
			doRecycle = true;
			isOptimizing = false;
		}
		finally
		{
			storageLock.writeLock().unlock();
		}

		if (log.isInfoEnabled())
		{
			log.info(cacheLogger + "finished " + timesOptimized + " optimization took " + timer.getElapsedTimeString());
		}
	}

	/**
	 * 填充文件空隙，并返回更新后文件的最后位置
	 */
	private long defragFile(IndexedDiskElementDescriptor[] defragList, long startingPos)
	{
		ElapsedTimer timer = new ElapsedTimer();
		long preFileSize = 0;
		long postFileSize = 0;
		long expectedNextPos = 0;
		try
		{
			preFileSize = this.dataFile.length();
			expectedNextPos = startingPos;
			for (int i = 0; i < defragList.length; i++)
			{
				storageLock.writeLock().lock();
				try
				{
					if (expectedNextPos != defragList[i].pos)
					{
						//将元素向前移动，填充前面的空隙，并更新IndexedDiskElementDescriptor的pos
						dataFile.move(defragList[i], expectedNextPos);
					}
					expectedNextPos = defragList[i].pos + IndexedDisk.HEADER_SIZE_BYTES + defragList[i].len;
				}
				finally
				{
					storageLock.writeLock().unlock();
				}
			}

			postFileSize = this.dataFile.length();

			return expectedNextPos;
		}
		catch (IOException e)
		{
			log.error(cacheLogger + "error occur in during defragmentation.", e);
		}
		finally
		{
			if (log.isInfoEnabled())
			{
				log.info(cacheLogger + "defragmentation took " + timer.getElapsedTimeString() + ". File Size (before="
						+ preFileSize + ") (after=" + postFileSize + ") (truncating to " + expectedNextPos + ")");
			}
		}

		return 0;
	}

	/**
	 * 将 keyHash中的 value(IndexedDiskElementDescriptor) 按位置排序返回一个数组
	 */
	private IndexedDiskElementDescriptor[] createPositionSortedDescriptorList()
	{
		IndexedDiskElementDescriptor[] defragList = new IndexedDiskElementDescriptor[keyHash.size()];
		Iterator<Map.Entry<K, IndexedDiskElementDescriptor>> iterator = keyHash.entrySet().iterator();
		for (int i = 0; iterator.hasNext(); i++)
		{
			Map.Entry<K, IndexedDiskElementDescriptor> next = iterator.next();
			defragList[i] = next.getValue();
		}

		Arrays.sort(defragList, new PositionComparator());

		return defragList;
	}

	@Override
	public int getSize()
	{
		return keyHash.size();
	}

	protected int getRecyleBinSize()
	{
		return this.recycle.size();
	}

	protected int getRecyleCount()
	{
		return this.recycleCnt;
	}

	protected long getBytesFree()
	{
		return this.bytesFree.get();
	}

	/**
	 * 重置可重用空间
	 */
	private void resetBytesFree()
	{
		this.bytesFree.set(0);
	}

	/**
	 * 更新空闲空间大小 bytesFree
	 */
	private void adjustBytesFree(IndexedDiskElementDescriptor ded, boolean add)
	{
		if (ded != null)
		{
			int amount = ded.len + IndexedDisk.HEADER_SIZE_BYTES;

			if (add)
			{
				this.bytesFree.addAndGet(amount);
			}
			else
			{
				this.bytesFree.addAndGet(-amount);
			}
		}
	}

	protected long getDataFileSize() throws IOException
	{
		long size = 0;

		storageLock.readLock().lock();

		try
		{
			if (dataFile != null)
			{
				size = dataFile.length();
			}
		}
		finally
		{
			storageLock.readLock().unlock();
		}

		return size;
	}

	public void dump()
	{
		dump(true);
	}

	public void dump(boolean dumpValues)
	{
		if (log.isDebugEnabled())
		{
			log.debug(cacheLogger + "[dump] number of keys: " + keyHash.size());

			for (Map.Entry<K, IndexedDiskElementDescriptor> e : keyHash.entrySet())
			{
				K key = e.getKey();
				IndexedDiskElementDescriptor ded = e.getValue();

				log.debug(cacheLogger + "[dump] disk element, key: " + key + ", pos: " + ded.pos + ", ded.len" + ded.len
						+ (dumpValues ? ", val: " + get(key) : ""));
			}
		}
	}

	@Override
	public KitCacheAttributes getKitCacheAttributes()
	{
		return this.cattr;
	}

	@Override
	public synchronized IStats getStatistics()
	{
		IStats stats = new Stats();
		stats.setTypeName("Indexed Disk Cache");

		ArrayList<IStatElement<?>> elems = new ArrayList<IStatElement<?>>();

		elems.add(new StatElement<Boolean>("Is Alive", Boolean.valueOf(isAlive())));
		elems.add(new StatElement<Integer>("Key Map Size",
				Integer.valueOf(this.keyHash != null ? this.keyHash.size() : -1)));
		try
		{
			elems.add(new StatElement<Long>("Data File Length",
					Long.valueOf(this.dataFile != null ? this.dataFile.length() : -1L)));
		}
		catch (IOException e)
		{
			log.error(e);
		}
		elems.add(new StatElement<Integer>("Max Key Size", this.maxKeySize));
		elems.add(new StatElement<AtomicInteger>("Hit Count", this.hitCount));
		elems.add(new StatElement<AtomicLong>("Bytes Free", this.bytesFree));
		elems.add(new StatElement<Integer>("Optimize Operation Count", Integer.valueOf(this.removeCount)));
		elems.add(new StatElement<Integer>("Times Optimized", Integer.valueOf(this.timesOptimized)));
		elems.add(new StatElement<Integer>("Recycle Count", Integer.valueOf(this.recycleCnt)));
		elems.add(new StatElement<Integer>("Recycle Bin Size", Integer.valueOf(this.recycle.size())));
		elems.add(new StatElement<Integer>("Startup Size", Integer.valueOf(this.startupSize)));

		IStats sStats = super.getStatistics();
		elems.addAll(sStats.getStatElements());

		stats.setStatElements(elems);

		return stats;
	}

	protected int getTimesOptimized()
	{
		return timesOptimized;
	}

	@Override
	protected String getDiskLocation()
	{
		return dataFile.getFilePath();
	}

	protected static final class PositionComparator implements Comparator<IndexedDiskElementDescriptor>, Serializable
	{
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(IndexedDiskElementDescriptor ded1, IndexedDiskElementDescriptor ded2)
		{
			if (ded1.pos < ded2.pos)
			{
				return -1;
			}
			else if (ded1.pos == ded2.pos)
			{
				return 0;
			}
			else
			{
				return 1;
			}
		}
	}

	/**
	 * 以文件大小作为限制
	 */
	public class LRUMapSizeLimited extends AbstractLRUMap<K, IndexedDiskElementDescriptor>
	{
		//以 KB 为单位
		private AtomicInteger contentSize;
		private int maxSize;

		public LRUMapSizeLimited()
		{
			this(-1);
		}

		public LRUMapSizeLimited(int maxKeySize)
		{
			super();
			this.maxSize = maxKeySize;
			this.contentSize = new AtomicInteger(0);
		}

		private void subLengthFromCacheSize(IndexedDiskElementDescriptor value)
		{
			contentSize.addAndGet((value.len + IndexedDisk.HEADER_SIZE_BYTES) / -1024 - 1);
		}

		private void addLengthToCacheSize(IndexedDiskElementDescriptor value)
		{
			contentSize.addAndGet((value.len + IndexedDisk.HEADER_SIZE_BYTES) / 1024 + 1);
		}

		@Override
		public IndexedDiskElementDescriptor put(K key, IndexedDiskElementDescriptor value)
		{
			IndexedDiskElementDescriptor oldValue = null;

			try
			{
				oldValue = super.put(key, value);
			}
			finally
			{
				if (value != null)
				{
					addLengthToCacheSize(value);
				}
				if (oldValue != null)
				{
					subLengthFromCacheSize(oldValue);
				}
			}

			return oldValue;
		}

		@Override
		public IndexedDiskElementDescriptor remove(Object key)
		{
			IndexedDiskElementDescriptor value = null;

			try
			{
				value = super.remove(key);
				return value;
			}
			finally
			{
				if (value != null)
				{
					subLengthFromCacheSize(value);
				}
			}
		}

		@Override
		protected void processRemovedLRU(K key, IndexedDiskElementDescriptor value)
		{
			if (value != null)
			{
				subLengthFromCacheSize(value);
			}
			//更新 自由空间 和 垃圾列表
			addToRecycleBin(value);

			if (log.isDebugEnabled())
			{
				log.debug(cacheLogger + "remove key: [" + key + "] from key store.");
				log.debug(cacheLogger + "key store size: [" + this.size() + "].");
			}

			doOptimizeRealTime();
		}

		@Override
		protected boolean shouldRemove()
		{
			return maxSize > 0 && contentSize.get() > maxSize && this.size() > 0;
		}
	}

	/**
	 * 以 IndexedDiskElementDescriptor 数量作为限制
	 */
	public class LRUMapCountLimited extends LRUMap<K, IndexedDiskElementDescriptor> implements Serializable
	{

		private static final long serialVersionUID = 1L;

		public LRUMapCountLimited(int maxKeySize)
		{
			super(maxKeySize);
		}

		@Override
		protected void processRemovedLRU(K key, IndexedDiskElementDescriptor value)
		{
			//更新 自由空间 和 垃圾列表
			addToRecycleBin(value);

			if (log.isDebugEnabled())
			{
				log.debug(cacheLogger + "remove key: [" + key + "] from key store.");
				log.debug(cacheLogger + "key store size: [" + this.size() + "].");
			}

			doOptimizeRealTime();
		}
	}
}
