package com.cachekit.kits.paxos.comm;

import java.util.Arrays;
import java.util.List;

import com.cachekit.kits.paxos.GroupMembership;

public class Members
{
	private final List<Member> members;

	public Members(Member... members)
	{
		this(Arrays.asList(members));
	}

	public Members(List<Member> members)
	{
		this.members = members;
	}

	public GroupMembership get(int i)
	{
		return new GroupMembership(members, i);
	}
}
