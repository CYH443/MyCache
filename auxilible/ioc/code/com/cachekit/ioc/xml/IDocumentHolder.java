package com.cachekit.ioc.xml;

import org.dom4j.Document;

public interface IDocumentHolder
{
	Document getDocument(String filePath);
}
