package org.esa.snap.objectstoragefs;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Map;

/**
 * Service-provider class for the AWS S3 object storage system.
 */
public class AwsS3FileSystemProvider extends ObjectStorageFileSystemProvider {
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
    ObjectStorageScanner createObjectStorageScanner() {
        try {
            return new AwsS3Scanner();
        } catch (ParserConfigurationException | SAXException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    ObjectStorageFileSystem createFileSystem(String address, Map<String, ?> env) throws IOException {
        Object delimiter = env.get("delimiter");
        return new ObjectStorageFileSystem(this,
                                           address,
                                           delimiter != null ? delimiter.toString() : "/");
    }
}
