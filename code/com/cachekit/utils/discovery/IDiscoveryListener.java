package com.cachekit.utils.discovery;

import com.cachekit.utils.discovery.DiscoveredService;

public interface IDiscoveryListener
{
	void addDiscoveredService(DiscoveredService service);

	void removeDiscoveredService(DiscoveredService service);
}
