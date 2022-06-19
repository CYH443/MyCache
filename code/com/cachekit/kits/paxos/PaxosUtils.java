package com.cachekit.kits.paxos;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

import com.cachekit.kits.paxos.comm.Member;

public class PaxosUtils
{
	/**
	 * 默认选举 ip 字节数组低4位最大的节点作为 leader，如果ip低四位相同，则选择端口号最大的
	 */
	static Member selectLeader(Collection<Member> members)
	{
		Member leader = null;
		for (Member member : members)
		{
			if (leader == null)
			{
				leader = member;
			}
			else
			{
				if (member.compareTo(leader) > 0)
				{
					leader = member;
				}
			}
		}
		return leader;
	}

	public static byte[] serialize(Serializable serializable)
	{
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(baos);
			out.writeObject(serializable);
			out.close();
			return baos.toByteArray();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public static Object deserialize(byte[] bytes)
	{
		ObjectInputStream ois = null;
		try
		{
			ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
			return ois.readObject();
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		catch (IOException e)
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		finally
		{
			if (ois != null)
				try
				{
					ois.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
		}
	}

	public static long findMax(Set<Long> longs)
	{
		long max = -1;
		for (Long n : longs)
			max = Math.max(n, max);
		return max;
	}
}
