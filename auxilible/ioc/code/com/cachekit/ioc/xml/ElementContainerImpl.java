package com.cachekit.ioc.xml;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.Element;

public class ElementContainerImpl implements IElementContainer
{

	private Map<String, Element> elements = new HashMap<String, Element>();

	public void addElements(Document doc)
	{
		List<Element> eles = doc.getRootElement().elements();
		for (Element e : eles)
		{
			String id = e.attributeValue("id");
			elements.put(id, e);
		}
	}

	public Element getElement(String id)
	{
		return elements.get(id);
	}

	public Collection<Element> getElements()
	{
		return elements.values();
	}

}
