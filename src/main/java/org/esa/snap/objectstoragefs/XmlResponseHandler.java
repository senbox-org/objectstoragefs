package org.esa.snap.objectstoragefs;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.LinkedList;
import java.util.List;

public class XmlResponseHandler extends DefaultHandler {
    private static final String KEY = "Key";
    private static final String SIZE = "Size";
    private static final String CONTENT = "Content";
    private static final String LAST_MODIFIED = "LastModified";
    private static final String NEXT_CONTINUATION_TOKEN = "NextContinuationToken";
    private static final String IS_TRUNCATED = "IsTruncated";
    private static final String COMMON_PREFIXES = "CommonPrefixes";
    private static final String PREFIX = "Prefix";

    private LinkedList<String> elementStack = new LinkedList<>();
    private List<ObjectStorageItemRef> items;

    private String key;
    private long size;
    private String lastModified;
    private String nextContinuationToken;
    private boolean isTruncated;
    private String prefix;

    XmlResponseHandler(List<ObjectStorageItemRef> items) {
        this.items = items;
    }

    private static String getTextValue(char[] ch, int start, int length) {
        return new String(ch, start, length).trim();
    }

    String getNextContinuationToken() {
        return nextContinuationToken;
    }

    boolean getIsTruncated() {
        return isTruncated;
    }

    @Override
    public void startElement(String namespaceURI,
                             String localName,
                             String qName,
                             Attributes atts)
            throws SAXException {
        String currentElement = localName.intern();
        elementStack.addLast(currentElement);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        String currentElement = elementStack.removeLast();
        assert currentElement != null && currentElement.equals(localName);

        if (currentElement.equals(PREFIX) && elementStack.size() == 2 && elementStack.get(1).equals(COMMON_PREFIXES)) {
            items.add(new ObjectStorageDirRef(prefix));
        } else if (currentElement.equals(CONTENT) && elementStack.size() == 1) {
            items.add(new ObjectStorageFileRef(key, size, lastModified));
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        String currentElement = elementStack.getLast();

        switch (currentElement) {
            case KEY:
                key = getTextValue(ch, start, length);
                break;
            case SIZE:
                size = getLongValue(ch, start, length);
                break;
            case LAST_MODIFIED:
                lastModified = getTextValue(ch, start, length);
                break;
            case IS_TRUNCATED:
                isTruncated = getBooleanValue(ch, start, length);
                break;
            case NEXT_CONTINUATION_TOKEN:
                nextContinuationToken = getTextValue(ch, start, length);
                break;
            case PREFIX:
                prefix = getTextValue(ch, start, length);
                break;
        }
    }

    private boolean getBooleanValue(char[] ch, int start, int length) {
        return Boolean.parseBoolean(getTextValue(ch, start, length));
    }

    private long getLongValue(char[] ch, int start, int length) {
        return Long.parseLong(getTextValue(ch, start, length));
    }
}
