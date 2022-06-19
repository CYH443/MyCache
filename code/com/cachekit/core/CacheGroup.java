package com.cachekit.core;

import com.cachekit.core.model.IElementAttributes;

public class CacheGroup
{
	private IElementAttributes attr;

	public CacheGroup()
	{
		super();
	}

	public void setElementAttributes(IElementAttributes attr)
	{
		this.attr = attr;
	}

	public IElementAttributes getElementAttrributes()
	{
		return attr;
	}
}
