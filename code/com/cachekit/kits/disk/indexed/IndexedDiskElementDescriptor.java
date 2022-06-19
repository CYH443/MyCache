package com.cachekit.kits.disk.indexed;

import java.io.Serializable;

/**
 * 一个 IndexedDiskElementDescriptor 表示一个 block的描述符，包括该 block 在文件中的位置和 block 的大小
 */
public class IndexedDiskElementDescriptor implements Serializable, Comparable<IndexedDiskElementDescriptor>
{
	private static final long serialVersionUID = 1L;
	//数据头（head）在文件的位置
	long pos;
	//数据的长度
	int len;

	public IndexedDiskElementDescriptor(long pos, int len)
	{
		this.pos = pos;
		this.len = len;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("[DED: ");
		sb.append(" pos = " + pos);
		sb.append(" len = " + len);
		sb.append("]");
		return sb.toString();
	}

	@Override
	public int hashCode()
	{
		return Long.valueOf(this.pos).hashCode() ^ Integer.valueOf(len).hashCode();
	}

	@Override
	public boolean equals(Object o)
	{
		if (o == null)
		{
			return false;
		}
		else if (o instanceof IndexedDiskElementDescriptor)
		{
			IndexedDiskElementDescriptor ided = (IndexedDiskElementDescriptor) o;
			return pos == ided.pos && len == ided.len;
		}

		return false;
	}

	/**
	 * 先比数据长度len，后比位置pos
	 */
	@Override
	public int compareTo(IndexedDiskElementDescriptor o)
	{
		if (o == null)
		{
			return 1;
		}

		if (o.len == len)
		{
			if (o.pos == pos)
			{
				return 0;
			}
			else if (o.pos < pos)
			{
				return -1;
			}
			else
			{
				return 1;
			}
		}
		else if (o.len > len)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
}
