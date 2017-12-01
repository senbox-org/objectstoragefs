package org.esa.snap.objectstoragefs;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

public interface ObjectStorageWalker {
    // TODO - return stream instead of fixed-size list
    List<BasicFileAttributes> walk(String address, String prefix, String delimiter) throws IOException;
}
