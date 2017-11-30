package org.esa.snap.objectstoragefs.aws;

import org.esa.snap.objectstoragefs.ObjectStorageItemRef;
import org.esa.snap.objectstoragefs.ObjectStorageScanner;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

class S3Scanner implements ObjectStorageScanner {

    private XMLReader xmlReader;

    S3Scanner() throws ParserConfigurationException, SAXException {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser saxParser = spf.newSAXParser();
        xmlReader = saxParser.getXMLReader();
    }

    private static void addParam(StringBuffer params, String name, String value) throws IOException {
        if (value == null || value.isEmpty()) {
            return;
        }
        if (params.length() > 0) {
            params.append("&");
        }
        params.append(name).append("=").append(URLEncoder.encode(value, "UTF8"));
    }

    public List<ObjectStorageItemRef> scan(String address, String delimiter, String prefix) throws IOException {
        StringBuffer paramBase = new StringBuffer();
        addParam(paramBase, "delimiter", delimiter);
        addParam(paramBase, "prefix", prefix);

        ArrayList<ObjectStorageItemRef> items = new ArrayList<>();
        String nextContinuationToken = null;
        S3ResponseHandler handler;
        do {
            handler = new S3ResponseHandler(items);
            xmlReader.setContentHandler(handler);
            StringBuffer params = new StringBuffer(paramBase);
            addParam(params, "continuation-token", nextContinuationToken);
            String systemId = address;
            if (params.length() > 0) {
                systemId += "?" + params;
            }
            // System.out.println("systemId = " + systemId);
            try {
                xmlReader.parse(systemId);
            } catch (SAXException e) {
                throw new IOException(e);
            }
            nextContinuationToken = handler.getNextContinuationToken();
        } while (handler.getIsTruncated());

        return items;
    }
}
