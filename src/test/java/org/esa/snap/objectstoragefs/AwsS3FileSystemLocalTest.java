package org.esa.snap.objectstoragefs;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class AwsS3FileSystemLocalTest extends AwsS3FileSystemTest {

    private static final int PORT = 8080;
    private static final String ADDRESS = "http://localhost:" + PORT;
    private static AwsS3RestApiMock apiMock;

    @Override
    String getAddress() {
        return ADDRESS;
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        apiMock = new AwsS3RestApiMock();
        apiMock.start(PORT);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        apiMock.stop();
    }

    @Test
    public void testScanner() throws Exception {
        List<ObjectStorageItemRef> items;

        items = new AwsS3Scanner().scan(getAddress(), "/", "");
        assertEquals(0, items.size());

        items = new AwsS3Scanner().scan(getAddress(), "/", "products/");
        assertEquals(3, items.size());

        items = new AwsS3Scanner().scan(getAddress(), "/", "tiles/");
        assertEquals(3, items.size());
    }

}
