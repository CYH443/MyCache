package com.cachekit.kits.disk.block;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.model.IElementSerializer;
import com.cachekit.utils.serialization.StandardSerializer;

/**
 * 一个block 分为 head 和 data 部分，同时具有block_id，从0号开始
 */
public class BlockDisk
{
	private static final Log log = LogFactory.getLog(BlockDisk.class);

	public static final byte HEADER_SIZE_BYTES = 4;

	private static final int DEFAULT_BLOCK_SIZE_BYTES = 4 * 1024;
	//一个block的大小
	private final int blockSizeBytes;
	//文件中最大的block的编号
	private final AtomicInteger numberOfBlocks = new AtomicInteger(0);
    //回收队列，内存可重用
	private final ConcurrentLinkedQueue<Integer> emptyBlocks = new ConcurrentLinkedQueue<Integer>();

	private final IElementSerializer elementSerializer;

	private final String filePath;

	private final FileChannel fc;

	private final AtomicLong putBytes = new AtomicLong(0);

	private final AtomicLong putCount = new AtomicLong(0);

	public BlockDisk(File file, IElementSerializer elementSerializer) throws IOException
	{
		this(file, DEFAULT_BLOCK_SIZE_BYTES, elementSerializer);
	}

	public BlockDisk(File file, int blockSizeBytes) throws IOException
	{
		this(file, blockSizeBytes, new StandardSerializer());
	}

	public BlockDisk(File file, int blockSizeBytes, IElementSerializer elementSerializer) throws IOException
	{
		this.filePath = file.getAbsolutePath();
		RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
		this.fc = raf.getChannel();
		this.numberOfBlocks.set((int) Math.ceil(1f * this.fc.size() / blockSizeBytes));

		if (log.isInfoEnabled())
		{
			log.info("Construct BlockDisk, blockSizeBytes [" + blockSizeBytes + "]");
		}

		this.blockSizeBytes = blockSizeBytes;
		this.elementSerializer = elementSerializer;
	}

	private int[] allocateBlocks(int numBlocksNeeded)
	{
		assert numBlocksNeeded >= 1;

		int[] blocks = new int[numBlocksNeeded];
		for (int i = 0; i < numBlocksNeeded; i++)
		{
			//从可回收的block中取出block_id
			Integer emptyBlock = emptyBlocks.poll();
			if (emptyBlock == null)
			{
				//没有可回收区域，则分配新的block_id
				emptyBlock = Integer.valueOf(numberOfBlocks.getAndIncrement());
			}
			blocks[i] = emptyBlock.intValue();
		}

		return blocks;
	}

	protected int[] write(Serializable object) throws IOException
	{
		byte[] data = elementSerializer.serialize(object);

		if (log.isDebugEnabled())
		{
			log.debug("Write object, total chunks data length = " + data.length);
		}

		this.putBytes.addAndGet(data.length);
		this.putCount.incrementAndGet();
		//计算需要多少个block
		int numBlocksNeeded = calculateTheNumberOfBlocksNeeded(data);

		if (log.isDebugEnabled())
		{
			log.debug("numBlocksNeeded = " + numBlocksNeeded);
		}
		//给block分配编号
		int[] blocks = allocateBlocks(numBlocksNeeded);

		int offset = 0;
		final int maxChunkSize = blockSizeBytes - HEADER_SIZE_BYTES;
		//存放头数据的buffer
		ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE_BYTES);

		for (int i = 0; i < numBlocksNeeded; i++)
		{
			headerBuffer.clear();
			int length = Math.min(maxChunkSize, data.length - offset);
			headerBuffer.putInt(length);
			//存放实际数据的buffer
			ByteBuffer dataBuffer = ByteBuffer.wrap(data, offset, length);

			long position = calculateByteOffsetForBlockAsLong(blocks[i]);
			headerBuffer.flip();
			int written = fc.write(headerBuffer, position);
			assert written == HEADER_SIZE_BYTES;

			written = fc.write(dataBuffer, position + HEADER_SIZE_BYTES);
			assert written == length;

			offset += length;
		}

		return blocks;
	}

	protected byte[][] getBlockChunks(byte[] complete, int numBlocksNeeded)
	{
		byte[][] chunks = new byte[numBlocksNeeded][];

		if (numBlocksNeeded == 1)
		{
			chunks[0] = complete;
		}
		else
		{
			int maxChunkSize = this.blockSizeBytes - HEADER_SIZE_BYTES;
			int totalBytes = complete.length;
			int totalUsed = 0;
			for (short i = 0; i < numBlocksNeeded; i++)
			{
				int chunkSize = Math.min(maxChunkSize, totalBytes - totalUsed);
				byte[] chunk = new byte[chunkSize];
				System.arraycopy(complete, totalUsed, chunk, 0, chunkSize);
				chunks[i] = chunk;
				totalUsed += chunkSize;
			}
		}

		return chunks;
	}

	protected <T extends Serializable> T read(int[] blockNumbers) throws IOException, ClassNotFoundException
	{
		byte[] data = null;

		if (blockNumbers.length == 1)
		{
			data = readBlock(blockNumbers[0]);
		}
		else
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream(getBlockSizeBytes());
			for (short i = 0; i < blockNumbers.length; i++)
			{
				byte[] chunk = readBlock(blockNumbers[i]);
				baos.write(chunk);
			}

			data = baos.toByteArray();
			baos.close();
		}

		if (log.isDebugEnabled())
		{
			log.debug("Read blocks, total data.length = " + data.length);
		}

		return elementSerializer.deSerialize(data, null);
	}

	private byte[] readBlock(int block) throws IOException
	{
		int datalen = 0;

		String message = null;
		boolean corrupted = false;
		long fileLength = fc.size();

		long position = calculateByteOffsetForBlockAsLong(block);
		if (position > fileLength)
		{
			corrupted = true;
			message = "record " + position + " starts past EOF.";
		}
		else
		{
			ByteBuffer datalength = ByteBuffer.allocate(HEADER_SIZE_BYTES);
			fc.read(datalength, position);
			datalength.flip();
			datalen = datalength.getInt();
			if (position + datalen > fileLength)
			{
				corrupted = true;
				message = "record " + position + " exceeds file length.";
			}
		}

		if (corrupted)
		{
			log.warn("The file is corrupt: " + message);
			throw new IOException("The file is corrupt, need to reset");
		}

		ByteBuffer data = ByteBuffer.allocate(datalen);
		fc.read(data, position + HEADER_SIZE_BYTES);
		data.flip();

		return data.array();
	}

	protected void freeBlocks(int[] blocksToFree)
	{
		if (blocksToFree != null)
		{
			for (short i = 0; i < blocksToFree.length; i++)
			{
				emptyBlocks.offer(Integer.valueOf(blocksToFree[i]));
			}
		}
	}

	protected int calculateByteOffsetForBlock(int block)
	{
		return block * blockSizeBytes;
	}

	/**
	 * 计算 block 在文件中的偏移量
	 * @param block block编号
	 */
	protected long calculateByteOffsetForBlockAsLong(int block)
	{
		return (long) block * blockSizeBytes;
	}

	/**
	 * 计算 存储数据需要几个 block
	 * @param data 要存储的数据
	 */
	protected int calculateTheNumberOfBlocksNeeded(byte[] data)
	{
		int dataLength = data.length;

		int oneBlock = blockSizeBytes - HEADER_SIZE_BYTES;

		if (dataLength <= oneBlock)
		{
			return 1;
		}

		int dividend = dataLength / oneBlock;

		if (dataLength % oneBlock != 0)
		{
			dividend++;
		}
		return dividend;
	}

	/**
	 * 文件大小
	 */
	protected long length() throws IOException
	{
		return fc.size();
	}

	protected void close() throws IOException
	{
		fc.close();
	}

	protected synchronized void reset() throws IOException
	{
		this.numberOfBlocks.set(0);
		this.emptyBlocks.clear();
		fc.truncate(0);
		fc.force(true);
	}

	/**
	 *返回block 的最大 id
	 */
	protected int getNumberOfBlocks()
	{
		return numberOfBlocks.get();
	}

	protected int getBlockSizeBytes()
	{
		return blockSizeBytes;
	}

	protected long getAveragePutSizeBytes()
	{
		long count = this.putCount.get();

		if (count == 0)
		{
			return 0;
		}
		return this.putBytes.get() / count;
	}

	/**
	 * 可回收 block 的数目
	 */
	protected int getEmptyBlocks()
	{
		return this.emptyBlocks.size();
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("\n  Block Disk ");
		sb.append("\n  Filepath [" + filePath + "]");
		sb.append("\n  NumberOfBlocks [" + this.numberOfBlocks.get() + "]");
		sb.append("\n  BlockSizeBytes [" + this.blockSizeBytes + "]");
		sb.append("\n  Put Bytes [" + this.putBytes + "]");
		sb.append("\n  Put Count [" + this.putCount + "]");
		sb.append("\n  Average Size [" + getAveragePutSizeBytes() + "]");
		sb.append("\n  Empty Blocks [" + this.getEmptyBlocks() + "]");
		try
		{
			sb.append("\n  Length [" + length() + "]");
		}
		catch (IOException e)
		{

		}
		return sb.toString();
	}

	protected String getFilePath()
	{
		return filePath;
	}
}
