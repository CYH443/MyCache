package com.cachekit.kits.disk.indexed;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.core.model.IElementSerializer;

/**
 * 数据由head和数据体组成：head表示数据长度，int类型，占用4个字节
 */
	class IndexedDisk
{
	public static final byte HEADER_SIZE_BYTES = 4;

	private final IElementSerializer elementSerializer;

	private static final Log log = LogFactory.getLog(IndexedDisk.class);

	private final String filepath;

	private final FileChannel fc;

	public IndexedDisk(File file, IElementSerializer elementSerializer) throws FileNotFoundException
	{
		this.filepath = file.getAbsolutePath();
		this.elementSerializer = elementSerializer;
		RandomAccessFile raf = new RandomAccessFile(filepath, "rw");
		this.fc = raf.getChannel();
	}

	protected <T extends Serializable> T readObject(IndexedDiskElementDescriptor ded) throws IOException, ClassNotFoundException
	{
		boolean corrupted = false;
		long fileLength = fc.size();
		if (ded.pos > fileLength)
		{
			corrupted = true;
		}
		else
		{
			//读取数据头4个字节
			ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE_BYTES);
			fc.read(buffer, ded.pos);
			//数据读写完成，则buffer执行flip，将指针重置为0，方便后续读写操作
			buffer.flip();
			int datalen = buffer.getInt();
			if (ded.len != datalen)
			{
				corrupted = true;
			}
			else if (ded.pos + ded.len > fileLength)
			{
				corrupted = true;
			}
		}

		if (corrupted)
		{
			throw new IOException("The file is corrupt.");
		}
		//读取数据体
		ByteBuffer data = ByteBuffer.allocate(ded.len);
		fc.read(data, ded.pos + HEADER_SIZE_BYTES);
		data.flip();

		return elementSerializer.deSerialize(data.array(), null);
	}

	protected void move(final IndexedDiskElementDescriptor ded, final long newPosition) throws IOException
	{
		ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE_BYTES);
		fc.read(buf, ded.pos);
		buf.flip();
		int length = buf.getInt();

		if (length != ded.len)
		{
			throw new IOException("Mismatch memory and disk length (" + length + ") for " + ded);
		}

		long readPos = ded.pos;
		long writePos = newPosition;
		//剩余未转移字节数
		int remaining = HEADER_SIZE_BYTES + length;
		ByteBuffer buffer = ByteBuffer.allocate(16384);

		while (remaining > 0)
		{
			int chunkSize = Math.min(remaining, buffer.capacity());
			buffer.limit(chunkSize);
			fc.read(buffer, readPos);
			buffer.flip();
			fc.write(buffer, writePos);
			buffer.clear();

			writePos += chunkSize;
			readPos += chunkSize;
			remaining -= chunkSize;
		}

		ded.pos = newPosition;
	}

	protected boolean write(IndexedDiskElementDescriptor ded, byte[] data) throws IOException
	{
		long pos = ded.pos;
		//实际数据大小与描述符不统一
		if (data.length != ded.len)
		{
			throw new IOException("Descriptor does not match data length");
		}

		ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE_BYTES + data.length);
		//头数据:int是4个字节
		buffer.putInt(data.length);
		//实体数据
		buffer.put(data);
		buffer.flip();
		int written = fc.write(buffer, pos);
		//fc.force(true);

		return written == data.length;
	}

	protected boolean writeObject(Serializable obj, long pos) throws IOException
	{
		byte[] data = elementSerializer.serialize(obj);
		write(new IndexedDiskElementDescriptor(pos, data.length), data);
		return true;
	}

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
		if (log.isDebugEnabled())
		{
			log.debug("Reset Indexed File [" + filepath + "]");
		}
		fc.truncate(0);
		//强制刷新
		fc.force(true);
	}

	//截断文件
	protected void truncate(long length) throws IOException
	{
		if (log.isInfoEnabled())
		{
			log.info("Truncate file [" + filepath + "] to " + length);
		}
		fc.truncate(length);
	}

	protected String getFilePath()
	{
		return filepath;
	}
}
