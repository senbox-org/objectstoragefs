package org.esa.snap.objectstoragefs;

public class ObjectStorageDirRef implements ObjectStorageItemRef {
    private final String prefix;

    public ObjectStorageDirRef(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String getPathName() {
        return prefix;
    }

    @Override
    public boolean isDirectory() {
        return true;
    }

    @Override
    public boolean isFile() {
        return false;
    }
}

