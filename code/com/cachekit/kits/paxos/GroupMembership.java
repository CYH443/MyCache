package com.cachekit.kits.paxos;

import java.util.Collections;
import java.util.List;

import com.cachekit.kits.paxos.comm.Member;

public class GroupMembership
{
	private final List<Member> members;
	private final Member me;
	//me 在members中的索引下标
	private final int positionInGroup;

	public GroupMembership(List<Member> members, int i)
	{
		this(members, members.get(i));
	}

	public GroupMembership(List<Member> members, Member me)
	{
		this.members = members;
		Collections.sort(members);
		this.me = me;
		this.positionInGroup = findPositionInGroup(me, members);
	}

	public Member getUID()
	{
		return me;
	}

	public int groupSize()
	{
		return members.size();
	}

	public List<Member> getMembers()
	{
		return members;
	}

	public int getPositionInGroup()
	{
		return positionInGroup;
	}

	public static int findPositionInGroup(Member me, List<Member> sortedMembers)
	{
		for (int i = 0; i < sortedMembers.size(); i++)
		{
			if (sortedMembers.get(i).equals(me))
				return i;
		}
		throw new RuntimeException("Could not find " + me + " in " + sortedMembers.toString());
	}
}
