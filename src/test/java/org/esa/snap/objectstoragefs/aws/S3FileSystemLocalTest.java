package org.esa.snap.objectstoragefs.aws;

import org.esa.snap.objectstoragefs.ObjectStorageItemRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class S3FileSystemLocalTest extends S3FileSystemTest {

    private static final int PORT = 8080;
    private static final String ADDRESS = "http://localhost:" + PORT;
    private static S3RestApiMock apiMock;

    @BeforeClass
    public static void setUpClass() throws Exception {
        apiMock = new S3RestApiMock();
        apiMock.start(PORT);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        apiMock.stop();
    }

    @Override
    String getAddress() {
        return ADDRESS;
    }

    @Test
    public void testScanner() throws Exception {
        List<ObjectStorageItemRef> items;

        items = new S3Scanner().scan(getAddress(), "/", "");
        assertEquals(4, items.size());
        assertEquals("index.html", items.get(0).getPathName());
        assertTrue(items.get(0).isFile());
        assertEquals("style.css", items.get(1).getPathName());
        assertTrue(items.get(1).isFile());
        assertEquals("products/", items.get(2).getPathName());
        assertTrue(items.get(2).isDirectory());
        assertEquals("tiles/", items.get(3).getPathName());
        assertTrue(items.get(3).isDirectory());

        items = new S3Scanner().scan(getAddress(), "/", "products/");
        assertEquals(3, items.size());
        assertEquals("products/2015/", items.get(0).getPathName());
        assertTrue(items.get(0).isDirectory());
        assertEquals("products/2016/", items.get(1).getPathName());
        assertTrue(items.get(1).isDirectory());
        assertEquals("products/2017/", items.get(2).getPathName());
        assertTrue(items.get(2).isDirectory());

        items = new S3Scanner().scan(getAddress(), "/", "tiles/");
        assertEquals(3, items.size());
        assertEquals("tiles/1/", items.get(0).getPathName());
        assertTrue(items.get(0).isDirectory());
        assertEquals("tiles/2/", items.get(1).getPathName());
        assertTrue(items.get(1).isDirectory());
        assertEquals("tiles/3/", items.get(2).getPathName());
        assertTrue(items.get(2).isDirectory());
    }

}
