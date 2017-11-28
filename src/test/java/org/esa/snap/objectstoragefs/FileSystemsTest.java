package org.esa.snap.objectstoragefs;

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Created by Norman on 22.11.2017.
 */
public class FileSystemsTest {

    @Test
    public void testNewFileSystem() throws Exception {
        Map<String, ?> env = new HashMap<>();
        FileSystem fileSystem = FileSystems.newFileSystem(new URI("test:bibo"), env);
        assertTrue(fileSystem instanceof ObjectStorageFileSystem);

        FileSystem fileSystem2 = FileSystems.getFileSystem(new URI("test:bibo"));
        assertSame(fileSystem, fileSystem2);

        fileSystem.close();

        try {
            FileSystems.getFileSystem(new URI("test:bibo"));
            Assert.fail("FileSystemNotFoundException expected");
        } catch (FileSystemNotFoundException e) {
            // ok
        }
    }
}
