package org.esa.snap.objectstoragefs;

public class ObjectStorageFileRef implements ObjectStorageItemRef {
    private final String key;
    private final long size;
    private final String lastModified;

    public ObjectStorageFileRef(String key, long size, String lastModified) {
        this.key = key;
        this.size = size;
        this.lastModified = lastModified;
    }

    @Override
    public String getPathName() {
        return key;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public boolean isFile() {
        return true;
    }

    public long getSize() {
        return size;
    }

    public String getLastModified() {
        return lastModified;
    }
}

