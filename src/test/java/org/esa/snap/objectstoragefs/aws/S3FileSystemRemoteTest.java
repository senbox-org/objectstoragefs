package org.esa.snap.objectstoragefs.aws;

import org.esa.snap.objectstoragefs.ObjectStorageFileAttributes;
import org.junit.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class S3FileSystemRemoteTest extends S3FileSystemTest {

    private final static String ADDRESS = "http://sentinel-s2-l1c.s3.amazonaws.com";

    @Override
    String getAddress() {
        return ADDRESS;
    }

    @Test
    public void testScanner() throws Exception {
        List<BasicFileAttributes> items;

        items = new S3Walker().walk(getAddress(), "", "/");
        assertEquals(7, items.size());

        items = new S3Walker().walk(getAddress(), "products/", "/");
        assertEquals(3, items.size());

        items = new S3Walker().walk(getAddress(), "tiles/", "/");
        assertEquals(60, items.size());
    }


    @Test
    public void testGET() throws Exception {

        URL url = new URL(getAddress() + "/tiles/1/C/CV/2015/12/21/0/preview.jpg");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setDoInput(true);
        connection.connect();

        //int responseCode = connection.getResponseCode();
        //System.out.println("responseCode = " + responseCode);
        //String responseMessage = connection.getResponseMessage();
        //System.out.println("responseMessage = " + responseMessage);

        InputStream stream = connection.getInputStream();
        byte[] b = new byte[1024 * 1024];
        int read = stream.read(b);
        assertTrue(read > 0);
        //ReadableByteChannel channel = Channels.newChannel(stream);

        connection.disconnect();
    }

}
