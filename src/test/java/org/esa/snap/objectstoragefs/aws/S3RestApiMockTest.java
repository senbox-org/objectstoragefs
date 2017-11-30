package org.esa.snap.objectstoragefs.aws;

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class S3RestApiMockTest {

    @Test
    public void testService() throws Exception {
        S3RestApiMock apiMock = new S3RestApiMock();
        apiMock.start(8080);

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse("http://localhost:8080");
        Assert.assertEquals("ListBucketResult", doc.getDocumentElement().getTagName());
        Assert.assertEquals(21, doc.getDocumentElement().getChildNodes().getLength());

        apiMock.stop();
    }

    @Test
    public void testParseRange() throws Exception {
        int[] defaultRange = {0, 199};
        Assert.assertArrayEquals(new int[]{4, 10}, S3RestApiMock.parseRange("bytes=4-10", defaultRange));
        Assert.assertArrayEquals(new int[]{12, 199}, S3RestApiMock.parseRange("bytes=12-", defaultRange));
        Assert.assertArrayEquals(new int[]{0, 16}, S3RestApiMock.parseRange("bytes=-16", defaultRange));
    }
}
