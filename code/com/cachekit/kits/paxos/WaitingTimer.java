package com.cachekit.kits.paxos;

import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class WaitingTimer
{
	private final HashMap<Long, Semaphore> waitingForResponse = new HashMap<Long, Semaphore>();

	public boolean waitALittle(long msgId) throws InterruptedException
	{
		Semaphore semaphore;
		synchronized (waitingForResponse)
		{
			semaphore = getOrCreateSemaphore(msgId);
		}
		return semaphore.tryAcquire(1000, TimeUnit.MILLISECONDS);
	}

	public void unblock(long msgId)
	{
		synchronized (waitingForResponse)
		{
			Semaphore semaphore = getOrCreateSemaphore(msgId);
			if (semaphore != null)
			{
				semaphore.release();
			}
		}
	}

	private Semaphore getOrCreateSemaphore(long msgId)
	{
		if (!waitingForResponse.containsKey(msgId))
		{
			waitingForResponse.put(msgId, new Semaphore(1));
		}
		return waitingForResponse.get(msgId);
	}
}
