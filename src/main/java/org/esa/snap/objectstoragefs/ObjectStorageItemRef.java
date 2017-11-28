package org.esa.snap.objectstoragefs;

interface ObjectStorageItemRef {
    String getPathName();

    boolean isDirectory();

    boolean isFile();
}

