package com.cachekit.kits.paxos;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.cachekit.kits.paxos.comm.CommLayer;
import com.cachekit.kits.paxos.comm.Member;
import com.cachekit.kits.paxos.comm.Tick;
import com.cachekit.kits.paxos.messages.Heartbeat;

public class FailureDetector
{
	//发送心跳间隔
	private static final long INTERVAL = 1000; // 1 秒
	private static final long TIMEOUT = 3000; // 3 秒

	private final GroupMembership membership;
	//UDPMessenger
	private final CommLayer messenger;
	//LeaderRole
	private final FailureListener listener;
	private final byte[] heartbeat;
	//存活着的节点集合，初始化时为全部节点都存活
	private Set<Member> membersAlive = new HashSet<Member>();
	//key为Member，value为time
	private final Map<Member, Long> lastHeardFrom = new ConcurrentHashMap<Member, Long>();
	//上一次发送心跳的时间
	private long lastHearbeat = 0;
	//当前时间
	private long time = 0;

	public FailureDetector(final GroupMembership membership, final CommLayer messenger, final FailureListener listener)
	{
		this.membership = membership;
		this.messenger = messenger;
		this.listener = listener;

		membersAlive.addAll(membership.getMembers());
		//心跳信号包含发送者的信息（Member）
		heartbeat = PaxosUtils.serialize(new Heartbeat(membership.getUID()));
	}

	private void sendHeartbeat(long time)
	{
		messenger.sendTo(membership.getMembers(), heartbeat);
		this.lastHearbeat = time;
	}

	private void checkForFailedMembers(long time)
	{
		for (Member member : membership.getMembers())
		{
			//忽略自身节点
			if (member.equals(membership.getUID()))
			{
				continue;
			}
			if (!lastHeardFrom.containsKey(member))
			{
				initialize(time, member);
			}
			//别的节点心跳超过3s，主观下线
			if (time - lastHeardFrom.get(member) > TIMEOUT)
			{
				if (membersAlive.contains(member))
				{
					membersAlive.remove(member);
					//自身节点leader角色试图参与选举一次
					listener.memberFailed(member, membersAlive);
				}
			}
			else
			{
				if (!membersAlive.contains(member))
				{
					membersAlive.add(member);
				}
			}
		}
	}

	private Long initialize(long time, Member member)
	{
		return lastHeardFrom.put(member, time);
	}

	public void update(long time)
	{
		this.time = time;
		//距离上次发送心跳超过1s，向其他节点发送心跳
		if (lastHearbeat + INTERVAL < time)
		{
			sendHeartbeat(time);
		}
		//100ms检查一次，是否有节点下线
		checkForFailedMembers(time);
	}

	public void dispatch(Serializable message)
	{
		//其他节点的心跳信息，存入 lastHeardFrom 哈希表中
		if (message instanceof Heartbeat)
		{
			Heartbeat heartbeat = (Heartbeat) message;
			lastHeardFrom.put(heartbeat.sender, time);
		}
		//自身的Tick信号，更新自身的时间，并发送心跳信息
		else if (message instanceof Tick)
		{
			update(((Tick) message).time);
		}
	}
}
