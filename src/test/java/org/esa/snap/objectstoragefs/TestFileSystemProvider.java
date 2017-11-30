package org.esa.snap.objectstoragefs;

import org.esa.snap.objectstoragefs.aws.S3FileSystemProvider;

import java.io.IOException;
import java.util.Map;

public class TestFileSystemProvider extends S3FileSystemProvider {
    /**
     * Returns the URI scheme that identifies this provider.
     *
     * @return The URI scheme
     */
    @Override
    public String getScheme() {
        return "test";
    }

    @Override
    protected ObjectStorageFileSystem newFileSystem(String address, Map<String, ?> env) throws IOException {
        Object delimiter = env.get("delimiter");
        return new ObjectStorageFileSystem(this,
                                           address,
                                           delimiter != null ? delimiter.toString() : "/");
    }
}
