package com.cachekit.ioc.xml.autowire;

public class ByNameAutowire implements IAutowire
{
	private String value;

	public ByNameAutowire(String value)
	{
		this.value = value;
	}

	public String getValue()
	{
		return value;
	}
}
