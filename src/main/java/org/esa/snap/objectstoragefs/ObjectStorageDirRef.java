package org.esa.snap.objectstoragefs;

class ObjectStorageDirRef implements ObjectStorageItemRef {
    private final String prefix;

    ObjectStorageDirRef(String prefix) {
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

