package org.esa.snap.objectstoragefs;

import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.TimeUnit;

/**
 * Basic attributes associated with a file in a file system.
 */
public class ObjectStorageFileAttributes implements BasicFileAttributes {
    private static final FileTime UNKNOWN_FILE_TIME = FileTime.from(Instant.EPOCH);
    private static final long DIRECTORY_SIZE = -1;

    private final String fileKey;
    private final boolean directory;
    private final long size;
    private final String lastModified;
    private FileTime lastModifiedTime;

    static ObjectStorageFileAttributes fromPath(ObjectStoragePath path) {
        ObjectStorageFileAttributes fileAttributes = path.getFileAttributes();
        if (fileAttributes == null) {
            if (path.isDirectory()) {
                fileAttributes = newDir(path.toString().substring(1));
                path.setFileAttributes(fileAttributes);
            } else {
                // TODO - fetch remote attributes size, lastModified
                // fileAttributes = newFile(path.toString(), size, lastModified);
                // path.setFileAttributes(fileAttributes);
                throw new UnsupportedOperationException();
            }
        }
        return fileAttributes;
    }

    public static ObjectStorageFileAttributes newFile(String fileKey, long size, String lastModified) {
        return new ObjectStorageFileAttributes(fileKey, false, lastModified, size);
    }

    public static ObjectStorageFileAttributes newDir(String prefix) {
        return new ObjectStorageFileAttributes(prefix, true, null, DIRECTORY_SIZE);
    }

    ObjectStorageFileAttributes(String fileKey, boolean directory, String lastModified, long size) {
        this.fileKey = fileKey;
        this.directory = directory;
        this.size = size;
        this.lastModified = lastModified;
    }

    /**
     * Returns an object that uniquely identifies the given file, or {@code
     * null} if a file key is not available.
     *
     * @return an object that uniquely identifies the given file, or {@code null}
     * @see Files#walkFileTree
     */
    @Override
    public Object fileKey() {
        return fileKey;
    }

    /**
     * Returns the time of last modification.
     *
     * @return a {@code FileTime} representing the time the file was last modified
     */
    @Override
    public FileTime lastModifiedTime() {
        if (lastModifiedTime == null) {
            lastModifiedTime = UNKNOWN_FILE_TIME;
            if (lastModified != null) {
                try {
                    LocalDateTime dateTime = LocalDateTime.parse(lastModified, DateTimeFormatter.ISO_INSTANT);
                    OffsetDateTime offsetDateTime = dateTime.atOffset(ZoneOffset.UTC);
                    lastModifiedTime = FileTime.from(offsetDateTime.toEpochSecond(), TimeUnit.SECONDS);
                } catch (DateTimeParseException ignored) {
                }
            }
        }
        return lastModifiedTime;
    }

    /**
     * Returns the time of last access.
     *
     * @return a {@code FileTime} representing the time of last access
     */
    @Override
    public FileTime lastAccessTime() {
        return UNKNOWN_FILE_TIME;
    }

    /**
     * Returns the creation time. The creation time is the time that the file
     * was created.
     *
     * @return a {@code FileTime} representing the time the file was created
     */
    @Override
    public FileTime creationTime() {
        return UNKNOWN_FILE_TIME;
    }

    /**
     * Tells whether the file is a regular file with opaque content.
     *
     * @return {@code true} if the file is a regular file with opaque content
     */
    @Override
    public boolean isRegularFile() {
        return !directory;
    }

    /**
     * Tells whether the file is a directory.
     *
     * @return {@code true} if the file is a directory
     */
    @Override
    public boolean isDirectory() {
        return directory;
    }

    /**
     * Tells whether the file is a symbolic link.
     *
     * @return {@code true} if the file is a symbolic link
     */
    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    /**
     * Tells whether the file is something other than a regular file, directory,
     * or symbolic link.
     *
     * @return {@code true} if the file something other than a regular file,
     * directory or symbolic link
     */
    @Override
    public boolean isOther() {
        return false;
    }

    /**
     * Returns the size of the file (in bytes).
     *
     * @return the file size, in bytes
     */
    @Override
    public long size() {
        return size;
    }
}
