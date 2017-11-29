package org.esa.snap.objectstoragefs;

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

public class AwsS3RestApiMockTest {

    @Test
    public void testService() throws Exception {
        AwsS3RestApiMock apiMock = new AwsS3RestApiMock();
        apiMock.start(8080);

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse("http://localhost:8080");
        Assert.assertEquals("ListBucketResult", doc.getDocumentElement().getTagName());
        Assert.assertEquals(332, doc.getDocumentElement().getChildNodes().getLength());

        apiMock.stop();
    }

    @Test
    public void testParseRange() throws Exception {
        int[] defaultRange = {0, 199};
        Assert.assertArrayEquals(new int[]{4, 10}, AwsS3RestApiMock.parseRange("bytes=4-10", defaultRange));
        Assert.assertArrayEquals(new int[]{12, 199}, AwsS3RestApiMock.parseRange("bytes=12-", defaultRange));
        Assert.assertArrayEquals(new int[]{0, 16}, AwsS3RestApiMock.parseRange("bytes=-16", defaultRange));
    }
}
