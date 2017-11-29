package org.esa.snap.objectstoragefs;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;

import static org.junit.Assert.*;

public abstract class AwsS3FileSystemTest {

    private FileSystem fs;

    abstract String getAddress();

    @Before
    public void setUp() throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("delimiter", "/");
        URI uri = new URI("s3:" + getAddress());
        fs = FileSystems.newFileSystem(uri, env);
        assertNotNull(fs);
    }

    @After
    public void tearDown() throws Exception {
        assertNotNull(fs);
        fs.close();
    }

    @Test
    public void testSeparator() throws Exception {
        assertEquals("/", fs.getSeparator());
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
        Path path = fs.getPath("/");
        DirectoryStream<Path> stream = provider.newDirectoryStream(path, p -> true);
        Iterator<Path> iterator = stream.iterator();
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
    public void testByteChannel() throws Exception {
        FileSystemProvider provider = fs.provider();
        Path path = fs.getPath("/tiles/1/C/CV/2015/12/25/0/preview.jpg");
        HashSet<OpenOption> openOptions = new HashSet<>();
        openOptions.add(StandardOpenOption.READ);
        SeekableByteChannel channel = provider.newByteChannel(path, openOptions);

        assertNotNull(channel);
        assertEquals(116665, channel.size());

        ByteBuffer buffer = ByteBuffer.wrap(new byte[(int) channel.size()]);
        int numRead = channel.read(buffer);
        assertEquals(numRead, channel.size());
    }

    @Test
    public void testPathsGet() throws Exception {
        Path path = fs.getPath("/tiles/1/C/CV/2015/12/25/0/preview.jpg");
        assertNotNull(path);
        assertEquals("/tiles/1/C/CV/2015/12/25/0/preview.jpg", path.toString());
    }

    @Ignore
    @Test
    public void testFilesWalk() throws Exception {
        Path path = fs.getPath("/tiles/");
        Iterator<Path> iterator = Files.walk(path).iterator();
        while (iterator.hasNext()) {
            Path next = iterator.next();
            System.out.println("next = " + next + ", abs=" + path.isAbsolute());
        }
    }

}
