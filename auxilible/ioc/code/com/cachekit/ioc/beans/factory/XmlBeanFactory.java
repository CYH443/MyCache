package com.cachekit.ioc.beans.factory;

import com.cachekit.ioc.context.AbstractAppContext;

public class XmlBeanFactory extends AbstractAppContext
{
	public XmlBeanFactory(String[] xmlPaths)
	{
		super.setUpElements(xmlPaths);
	}
}
