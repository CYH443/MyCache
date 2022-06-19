package com.cachekit.kits.paxos;

import java.util.Set;

import com.cachekit.kits.paxos.comm.Member;

public interface FailureListener
{
	void memberFailed(Member member, Set<Member> aliveMembers);
}
