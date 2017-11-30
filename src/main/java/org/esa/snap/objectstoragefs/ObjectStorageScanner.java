package org.esa.snap.objectstoragefs;

import java.io.IOException;
import java.util.List;

public interface ObjectStorageScanner {
    // TODO - return stream instead of fixed-size list
    List<ObjectStorageItemRef> scan(String address, String delimiter, String prefix) throws IOException;
}
