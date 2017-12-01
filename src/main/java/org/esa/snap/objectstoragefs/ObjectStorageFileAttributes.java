package org.esa.snap.objectstoragefs;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.TimeUnit;

/**
 * Basic attributes associated with a file in a file system.
 */
public abstract class ObjectStorageFileAttributes implements BasicFileAttributes {
    static final BasicFileAttributes ROOT;
    static final BasicFileAttributes EMPTY;
    static final FileTime UNKNOWN_FILE_TIME = FileTime.from(Instant.EPOCH);
    static final DateTimeFormatter ISO_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'.'SSS'Z'");

    static BasicFileAttributes fromPath(ObjectStoragePath path) throws IOException {
        BasicFileAttributes fileAttributes = path.getFileAttributes();
        if (fileAttributes == null) {
            if (path.isDirectory()) {
                fileAttributes = newDir(path.toString().substring(1));
            } else {
                HttpURLConnection urlConnection = (HttpURLConnection) path.getFileURL().openConnection();
                urlConnection.connect();
                long contentLength = urlConnection.getContentLengthLong();
                String lastModified = urlConnection.getHeaderField("last-modified");
                urlConnection.disconnect();
                fileAttributes = newFile(path.toString(), contentLength, lastModified);
            }
            path.setFileAttributes(fileAttributes);
        }
        return fileAttributes;
    }

    public static BasicFileAttributes newFile(String fileKey, long size, String lastModified) {
        return new RegularFileAttributes(fileKey, lastModified, size);
    }

    public static BasicFileAttributes newDir(String prefix) {
        return new DirAttributes(prefix);
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public FileTime lastModifiedTime() {
        return UNKNOWN_FILE_TIME;
    }

    @Override
    public FileTime lastAccessTime() {
        return UNKNOWN_FILE_TIME;
    }

    @Override
    public FileTime creationTime() {
        return UNKNOWN_FILE_TIME;
    }

    @Override
    public boolean isRegularFile() {
        return false;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    public static class RegularFileAttributes extends ObjectStorageFileAttributes {

        private final String fileKey;
        private final long size;
        private final String lastModified;
        private FileTime lastModifiedTime;

        RegularFileAttributes(String fileKey, String lastModified, long size) {
            this.fileKey = fileKey;
            this.size = size;
            this.lastModified = lastModified;
        }

        @Override
        public Object fileKey() {
            return fileKey;
        }

        @Override
        public boolean isRegularFile() {
            return true;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public FileTime lastModifiedTime() {
            if (lastModifiedTime == null) {
                synchronized (this) {
                    if (lastModifiedTime == null) {
                        lastModifiedTime = UNKNOWN_FILE_TIME;
                        if (lastModified != null) {
                            try {
                                LocalDateTime dateTime = LocalDateTime.parse(lastModified, ISO_DATE_TIME);
                                lastModifiedTime = FileTime.from(dateTime.toInstant(ZoneOffset.UTC));
                            } catch (DateTimeParseException ignored) {
                            }
                        }
                    }
                }
            }
            return lastModifiedTime;
        }
    }

    private static class DirAttributes extends ObjectStorageFileAttributes {

        private final String prefix;

        DirAttributes(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Object fileKey() {
            return prefix;
        }

        @Override
        public boolean isDirectory() {
            return true;
        }
    }

    private static class EmptyAttributes extends ObjectStorageFileAttributes {

        @Override
        public Object fileKey() {
            return "";
        }
    }

    static {
        ROOT = new DirAttributes("/");
        EMPTY = new EmptyAttributes();
    }
}
