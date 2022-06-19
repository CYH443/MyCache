package com.cachekit.utils.access;

public interface CacheKitWorkerHelper<V>
{
	boolean isFinished();

	void setFinished(boolean isFinished);

	V doWork() throws Exception;
}
