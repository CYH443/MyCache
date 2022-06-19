package com.cachekit.core.model;

import java.util.concurrent.ScheduledExecutorService;

public interface IProvideScheduler
{
	ScheduledExecutorService getScheduledExecutorService();
}
