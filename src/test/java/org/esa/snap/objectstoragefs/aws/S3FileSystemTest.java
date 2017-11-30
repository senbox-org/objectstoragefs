package org.esa.snap.objectstoragefs.aws;

import org.esa.snap.objectstoragefs.ObjectStorageFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.EOFException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

public abstract class S3FileSystemTest {

    private ObjectStorageFileSystem fs;

    abstract String getAddress();

    @Before
    public void setUp() throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("delimiter", "/");
        URI uri = new URI("s3:" + getAddress());
        FileSystem fs = FileSystems.newFileSystem(uri, env);
        assertNotNull(fs);
        assertTrue(fs instanceof ObjectStorageFileSystem);
        this.fs = (ObjectStorageFileSystem) fs;
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
    public void testClose() throws Exception {
        FileSystemProvider provider = fs.provider();
        HashSet<OpenOption> openOptions = new HashSet<>();
        openOptions.add(StandardOpenOption.READ);
        SeekableByteChannel channel1 = provider.newByteChannel(fs.getPath("/tiles/1/C/CV/2015/12/25/0/preview.jpg"), openOptions);
        SeekableByteChannel channel2 = provider.newByteChannel(fs.getPath("/tiles/1/C/CV/2015/12/25/0/preview.jpg"), openOptions);
        SeekableByteChannel channel3 = provider.newByteChannel(fs.getPath("/tiles/1/C/CV/2015/12/25/0/preview.jpg"), openOptions);
        assertTrue(fs.isOpen());
        assertTrue(channel1.isOpen());
        assertTrue(channel2.isOpen());
        assertTrue(channel3.isOpen());
        fs.close();
        assertFalse(fs.isOpen());
        assertFalse(channel1.isOpen());
        assertFalse(channel2.isOpen());
        assertFalse(channel3.isOpen());
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
        assertEquals(0, channel.position());

        ByteBuffer buffer = ByteBuffer.wrap(new byte[116665]);
        int numRead = channel.read(buffer);
        assertEquals(116665, numRead);
        assertEquals(116665, channel.size());
        assertEquals(116665, channel.position());

        channel.position(100000);
        assertEquals(100000, channel.position());
        assertEquals(116665, channel.size());

        buffer = ByteBuffer.wrap(new byte[10000]);
        numRead = channel.read(buffer);
        assertEquals(10000, numRead);
        assertEquals(110000, channel.position());
        assertEquals(116665, channel.size());

        buffer = ByteBuffer.wrap(new byte[6665]);
        numRead = channel.read(buffer);
        assertEquals(6665, numRead);
        assertEquals(116665, channel.position());
        assertEquals(116665, channel.size());

        buffer = ByteBuffer.wrap(new byte[10]);
        try {
            numRead = channel.read(buffer);
            fail("EOFException expected, but read " + numRead + " bytes");
        } catch (EOFException e) {
            // ok
        }
    }

    @Test
    public void testPathsGet() throws Exception {
        Path path = fs.getPath("/tiles/1/C/CV/2015/12/25/0/preview.jpg");
        assertNotNull(path);
        assertEquals("/tiles/1/C/CV/2015/12/25/0/preview.jpg", path.toString());
    }

    //@Ignore
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
