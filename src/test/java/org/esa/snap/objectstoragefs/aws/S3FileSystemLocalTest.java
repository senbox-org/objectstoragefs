package org.esa.snap.objectstoragefs.aws;

import org.esa.snap.objectstoragefs.ObjectStorageFileAttributes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

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
    public void testNewDirectoryStream() throws Exception {
        FileSystemProvider provider = fs.provider();
        Path path = fs.getPath("/");
        DirectoryStream<Path> stream = provider.newDirectoryStream(path, p -> true);
        Iterator<Path> iterator = stream.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("/GENERAL_QUALITY.xml", iterator.next().toString());
        assertTrue(iterator.hasNext());
        assertEquals("/index.html", iterator.next().toString());
        assertTrue(iterator.hasNext());
        assertEquals("/style.css", iterator.next().toString());
        assertTrue(iterator.hasNext());
        assertEquals("/products/", iterator.next().toString());
        assertTrue(iterator.hasNext());
        assertEquals("/tiles/", iterator.next().toString());
        assertFalse(iterator.hasNext());

        path = fs.getPath("/products/");
        stream = provider.newDirectoryStream(path, p -> true);
        iterator = stream.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("/products/2015/", iterator.next().toString());
        assertTrue(iterator.hasNext());
        assertEquals("/products/2016/", iterator.next().toString());
        assertTrue(iterator.hasNext());
        assertEquals("/products/2017/", iterator.next().toString());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testScanner() throws Exception {
        List<ObjectStorageFileAttributes> items;

        items = new S3Walker().walk(getAddress(), "", "/");
        assertEquals(5, items.size());
        assertEquals("GENERAL_QUALITY.xml", items.get(0).fileKey());
        assertTrue(items.get(0).isRegularFile());
        assertEquals("index.html", items.get(1).fileKey());
        assertTrue(items.get(1).isRegularFile());
        assertEquals("style.css", items.get(2).fileKey());
        assertTrue(items.get(2).isRegularFile());
        assertEquals("products/", items.get(3).fileKey());
        assertTrue(items.get(3).isDirectory());
        assertEquals("tiles/", items.get(4).fileKey());
        assertTrue(items.get(4).isDirectory());

        items = new S3Walker().walk(getAddress(), "products/", "/");
        assertEquals(3, items.size());
        assertEquals("products/2015/", items.get(0).fileKey());
        assertTrue(items.get(0).isDirectory());
        assertEquals("products/2016/", items.get(1).fileKey());
        assertTrue(items.get(1).isDirectory());
        assertEquals("products/2017/", items.get(2).fileKey());
        assertTrue(items.get(2).isDirectory());

        items = new S3Walker().walk(getAddress(), "tiles/", "/");
        assertEquals(3, items.size());
        assertEquals("tiles/1/", items.get(0).fileKey());
        assertTrue(items.get(0).isDirectory());
        assertEquals("tiles/2/", items.get(1).fileKey());
        assertTrue(items.get(1).isDirectory());
        assertEquals("tiles/3/", items.get(2).fileKey());
        assertTrue(items.get(2).isDirectory());
    }
}
