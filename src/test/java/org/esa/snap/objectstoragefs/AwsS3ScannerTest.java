package org.esa.snap.objectstoragefs;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class AwsS3ScannerTest {

    private static final String ADDRESS = "http://sentinel-s2-l1c.s3.amazonaws.com";

    @Test
    public void testIt() throws Exception {
        List<ObjectStorageItemRef> items;

        items = new AwsS3Scanner().scan(ADDRESS, "/", "");
        assertEquals(2, items.size());

        items = new AwsS3Scanner().scan(ADDRESS, "/", "products/");
        assertEquals(3, items.size());

        items = new AwsS3Scanner().scan(ADDRESS, "/", "tiles/");
        assertEquals(60, items.size());
    }
}
