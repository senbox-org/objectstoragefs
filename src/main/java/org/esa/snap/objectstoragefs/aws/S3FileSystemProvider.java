package org.esa.snap.objectstoragefs.aws;

import org.esa.snap.objectstoragefs.ObjectStorageFileSystem;
import org.esa.snap.objectstoragefs.ObjectStorageFileSystemProvider;
import org.esa.snap.objectstoragefs.ObjectStorageWalker;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Map;

/**
 * Service-provider class for the AWS S3 object storage system.
 */
public class S3FileSystemProvider extends ObjectStorageFileSystemProvider {
    /**
     * Returns the URI scheme that identifies this provider.
     *
     * @return The URI scheme
     */
    @Override
    public String getScheme() {
        return "s3";
    }

    @Override
    protected ObjectStorageWalker newObjectStorageWalker() {
        try {
            return new S3Walker();
        } catch (ParserConfigurationException | SAXException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected ObjectStorageFileSystem newFileSystem(String address, Map<String, ?> env) throws IOException {
        Object delimiter = env.get("delimiter");
        return new ObjectStorageFileSystem(this,
                                           address,
                                           delimiter != null ? delimiter.toString() : "/");
    }
}
