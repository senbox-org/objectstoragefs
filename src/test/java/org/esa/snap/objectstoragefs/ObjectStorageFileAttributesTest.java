package org.esa.snap.objectstoragefs;

import org.junit.Test;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.esa.snap.objectstoragefs.ObjectStorageFileAttributes.*;
import static org.junit.Assert.*;

public class ObjectStorageFileAttributesTest {
    @Test
    public void testFile() throws Exception {
        LocalDateTime dateTime = LocalDateTime.parse("2016-07-13T17:24:10.009Z", ISO_DATE_TIME);

        BasicFileAttributes fileAttributes = newFile("index.html", 34986, "2016-07-13T17:24:10.009Z");
        assertEquals("index.html", fileAttributes.fileKey());
        assertEquals(true, fileAttributes.isRegularFile());
        assertEquals(false, fileAttributes.isDirectory());
        assertEquals(false, fileAttributes.isSymbolicLink());
        assertEquals(false, fileAttributes.isOther());
        assertEquals(34986, fileAttributes.size());
        assertEquals(FileTime.from(dateTime.toInstant(ZoneOffset.UTC)), fileAttributes.lastModifiedTime());
    }

    @Test
    public void testDir() throws Exception {
        BasicFileAttributes fileAttributes = newDir("products/");
        assertEquals("products/", fileAttributes.fileKey());
        assertEquals(false, fileAttributes.isRegularFile());
        assertEquals(true, fileAttributes.isDirectory());
        assertEquals(false, fileAttributes.isSymbolicLink());
        assertEquals(false, fileAttributes.isOther());
        assertEquals(0, fileAttributes.size());
        assertEquals(UNKNOWN_FILE_TIME, fileAttributes.lastModifiedTime());
    }

    @Test
    public void testEmpty() throws Exception {
        BasicFileAttributes fileAttributes = EMPTY;
        assertEquals("", fileAttributes.fileKey());
        assertEquals(false, fileAttributes.isRegularFile());
        assertEquals(false, fileAttributes.isDirectory());
        assertEquals(false, fileAttributes.isSymbolicLink());
        assertEquals(false, fileAttributes.isOther());
        assertEquals(0, fileAttributes.size());
        assertEquals(UNKNOWN_FILE_TIME, fileAttributes.lastModifiedTime());
    }
}
