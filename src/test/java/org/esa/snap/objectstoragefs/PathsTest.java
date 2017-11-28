package org.esa.snap.objectstoragefs;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Created by Norman on 22.11.2017.
 */
public class PathsTest {

    @Test
    public void testGet() throws Exception {
        Path path = Paths.get(new URI("test:bibo/hello/world/README.md"));
        Assert.assertNotNull(path);
        Assert.assertEquals("/hello/world/README.md", path.toString());
    }

    @Ignore
    @Test
    public void testDefault() throws Exception {
        Path path = Paths.get(".").toAbsolutePath();

        Iterator<Path> iterator = Files.walk(path).iterator();
        while (iterator.hasNext()) {
            Path next = iterator.next();
            System.out.println("next = " + next + ", abs=" + path.isAbsolute());
        }
    }
    @Test
    public void testDefaultRoot() throws Exception {
        Path path = Paths.get(".").toAbsolutePath();
        Path root = path.getRoot();
        System.out.println("root = " + root);
    }

}