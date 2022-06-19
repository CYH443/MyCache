package com.cachekit.ioc.xml;

import java.util.List;

import com.cachekit.ioc.xml.autowire.IAutowire;
import com.cachekit.ioc.xml.construct.ParamElement;
import com.cachekit.ioc.xml.property.PropertyElement;
import org.dom4j.Element;

public interface IElementReader
{

	boolean isLazy(Element element);

	List<Element> getConstructorElements(Element element);

	String getAttribute(Element element, String name);

	boolean isSingleton(Element element);

	List<Element> getPropertyElements(Element element);

	IAutowire getAutowire(Element element);

	List<ParamElement> getConstructorValue(Element element);

	List<PropertyElement> getPropertyValue(Element element);
}
