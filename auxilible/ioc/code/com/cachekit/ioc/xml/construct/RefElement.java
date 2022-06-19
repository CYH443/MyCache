package com.cachekit.ioc.xml.construct;

public class RefElement implements ParamElement
{

	private Object value;

	public RefElement(Object value)
	{
		this.value = value;
	}

	public String getType()
	{
		return "ref";
	}

	public Object getValue()
	{
		return this.value;
	}

}
