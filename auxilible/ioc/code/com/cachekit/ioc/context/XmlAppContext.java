package com.cachekit.ioc.context;

public class XmlAppContext extends AbstractAppContext
{

	public XmlAppContext(String[] xmlPaths)
	{
		setUpElements(xmlPaths);
		createBeans();
	}
}
