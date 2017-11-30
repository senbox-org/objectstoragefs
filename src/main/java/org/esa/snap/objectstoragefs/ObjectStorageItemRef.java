package org.esa.snap.objectstoragefs;

public interface ObjectStorageItemRef {
    String getPathName();

    boolean isDirectory();

    boolean isFile();
}

