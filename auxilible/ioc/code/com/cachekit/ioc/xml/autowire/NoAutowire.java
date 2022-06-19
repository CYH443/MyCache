package com.cachekit.ioc.xml.autowire;

public class NoAutowire implements IAutowire
{

	private String value;

	public NoAutowire(String value)
	{
		this.value = value;
	}

	public String getValue()
	{
		return "no";
	}

}
