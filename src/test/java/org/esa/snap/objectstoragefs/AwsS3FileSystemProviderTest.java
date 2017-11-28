package org.esa.snap.objectstoragefs;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

public class AwsS3FileSystemProviderTest {

    private static final String BUCKET = "http://sentinel-s2-l1c.s3.amazonaws.com";
    private static final String S3_BUCKET = "s3:" + BUCKET;

    private static FileSystem fs;

    @BeforeClass
    public static void setUp() throws Exception {
        Map<String, ?> env = new HashMap<>();
        fs = FileSystems.newFileSystem(new URI(S3_BUCKET), env);
        assertNotNull(fs);
        assertEquals("/", fs.getSeparator());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        fs.close();
    }

    @Test
    public void testGetRootDirectories() throws Exception {
        Iterable<Path> rootDirectories = fs.getRootDirectories();
        Iterator<Path> iterator = rootDirectories.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("/products/", iterator.next().toString());
        assertTrue(iterator.hasNext());
        assertEquals("/tiles/", iterator.next().toString());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNewDirectoryStream() throws Exception {
        FileSystemProvider provider = fs.provider();

        Path path = provider.getPath(new URI(S3_BUCKET));
        DirectoryStream<Path> stream = provider.newDirectoryStream(path, p -> true);
        Iterator<Path> iterator = stream.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("/products/", iterator.next().toString());
        assertTrue(iterator.hasNext());
        assertEquals("/tiles/", iterator.next().toString());
        assertFalse(iterator.hasNext());

        path = provider.getPath(new URI(S3_BUCKET + "/products/"));
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
    public void testByteChannel() throws Exception {
        FileSystemProvider provider = fs.provider();

        Path path = Paths.get(new URI(S3_BUCKET + "/tiles/1/C/CV/2015/12/21/0/preview.jpg"));
        HashSet<OpenOption> openOptions = new HashSet<>();
        openOptions.add(StandardOpenOption.READ);
        SeekableByteChannel channel = provider.newByteChannel(path, openOptions);

        assertNotNull(channel);
        assertEquals(113837, channel.size());

        ByteBuffer buffer = ByteBuffer.wrap(new byte[(int) channel.size()]);
        int numRead = channel.read(buffer);
        assertEquals(numRead, channel.size());
    }

    @Test
    public void testPathsGet() throws Exception {
        Path path = Paths.get(new URI(S3_BUCKET + "/tiles/1/C/CV/2015/12/21/0/preview.jpg"));
        assertNotNull(path);
        assertEquals("/tiles/1/C/CV/2015/12/21/0/preview.jpg", path.toString());
    }

    @Ignore
    @Test
    public void testFilesWalk() throws Exception {
        Path path = Paths.get(new URI(S3_BUCKET + "/tiles/"));

        Iterator<Path> iterator = Files.walk(path).iterator();
        while (iterator.hasNext()) {
            Path next = iterator.next();
            System.out.println("next = " + next + ", abs=" + path.isAbsolute());
        }
    }

    @Test
    public void testGET() throws Exception {
        URL url = new URL(BUCKET + "/tiles/1/C/CV/2015/12/21/0/preview.jpg");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setDoInput(true);
        connection.connect();

        int responseCode = connection.getResponseCode();
        String responseMessage = connection.getResponseMessage();

        System.out.println("responseCode = " + responseCode);
        System.out.println("responseMessage = " + responseMessage);

        InputStream stream = connection.getInputStream();
        byte[] b = new byte[1024 * 1024];
        int read = stream.read(b);
        assertTrue(read > 0);
        //ReadableByteChannel channel = Channels.newChannel(stream);

        connection.disconnect();
    }

}
