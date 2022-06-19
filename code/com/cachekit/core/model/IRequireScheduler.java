package com.cachekit.core.model;

import java.util.concurrent.ScheduledExecutorService;

public interface IRequireScheduler
{
	void setScheduledExecutorService(ScheduledExecutorService scheduledExecutor);
}
