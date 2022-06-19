package com.cachekit.kits.paxos.comm;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Member implements Comparable<Member>, Serializable
{
	private static final long serialVersionUID = 1L;
	public static final int DEFAULT_PORT = 2440;

	private final InetAddress address;
	private final int port;
	private final byte[] addressBytes;

	public Member() throws UnknownHostException
	{
		this(DEFAULT_PORT);
	}

	public Member(int port) throws UnknownHostException
	{
		this(InetAddress.getLocalHost(), port);
	}

	public Member(InetAddress address, int port)
	{
		this.address = address;
		this.port = port;
		this.addressBytes = address.getAddress();
	}

	public InetAddress getAddress()
	{
		return address;
	}

	public int getPort()
	{
		return port;
	}

	/**
	 * address字节数组的低4位
	 */
	public int compareTo(Member other)
	{
		if (this.addressBytes[0] < other.addressBytes[0])
			return -1;
		if (this.addressBytes[0] > other.addressBytes[0])
			return 1;
		if (this.addressBytes[1] < other.addressBytes[1])
			return -1;
		if (this.addressBytes[1] > other.addressBytes[1])
			return 1;
		if (this.addressBytes[2] < other.addressBytes[2])
			return -1;
		if (this.addressBytes[2] > other.addressBytes[2])
			return 1;
		if (this.addressBytes[3] < other.addressBytes[3])
			return -1;
		if (this.addressBytes[3] > other.addressBytes[3])
			return 1;
		return port - other.port;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
		{
			return true;
		}
		if (o == null || getClass() != o.getClass())
		{
			return false;
		}

		Member member = (Member) o;

		return (port == member.port) && address.equals(member.address);
	}

	/**
	 * 哈希值怎么算的？？？？？？
	 */
	@Override
	public int hashCode()
	{
		int result = address.hashCode();
		result = 31 * result + port;
		return result;
	}

	@Override
	public String toString()
	{
		return address.getHostName() + ":" + port;
	}
}
