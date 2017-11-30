package org.esa.snap.objectstoragefs;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * An object that may be used to locate a file in a file system. It will
 * typically represent a system dependent file path.
 * <p>
 * <p> A {@code Path} represents a path that is hierarchical and composed of a
 * sequence of directory and file name elements separated by a special separator
 * or delimiter. A <em>root component</em>, that identifies a file system
 * hierarchy, may also be present. The name element that is <em>farthest</em>
 * from the root of the directory hierarchy is the name of a file or directory.
 * The other name elements are directory names. A {@code Path} can represent a
 * root, a root and a sequence of names, or simply one or more name elements.
 * A {@code Path} is considered to be an <i>empty path</i> if it consists
 * solely of one name element that is empty. Accessing a file using an
 * <i>empty path</i> is equivalent to accessing the default directory of the
 * file system. {@code Path} defines the {@link #getFileName() getFileName},
 * {@link #getParent getParent}, {@link #getRoot getRoot}, and {@link #subpath
 * subpath} methods to access the path components or a subsequence of its name
 * elements.
 * <p>
 * <p> In addition to accessing the components of a path, a {@code Path} also
 * defines the {@link #resolve(Path) resolve} and {@link #resolveSibling(Path)
 * resolveSibling} methods to combine paths. The {@link #relativize relativize}
 * method that can be used to construct a relative path between two paths.
 * Paths can be {@link #compareTo compared}, and tested against each other using
 * the {@link #startsWith startsWith} and {@link #endsWith endsWith} methods.
 * <p>
 * <p> This interface extends {@link Watchable} interface so that a directory
 * located by a path can be {@link #register registered} with a {@link
 * WatchService} and entries in the directory watched. </p>
 * <p>
 * <p> <b>WARNING:</b> This interface is only intended to be implemented by
 * those developing custom file system implementations. Methods may be added to
 * this interface in future releases. </p>
 * <p>
 * <h2>Accessing Files</h2>
 * <p> Paths may be used with the {@link Files} class to operate on files,
 * directories, and other types of files. For example, suppose we want a {@link
 * java.io.BufferedReader} to read text from a file "{@code access.log}". The
 * file is located in a directory "{@code logs}" relative to the current working
 * directory and is UTF-8 encoded.
 * <pre>
 *     Path path = FileSystems.getDefault().getPath("logs", "access.log");
 *     BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
 * </pre>
 * <p>
 * <a name="interop"></a><h2>Interoperability</h2>
 * <p> Paths associated with the default {@link
 * java.nio.file.spi.FileSystemProvider provider} are generally interoperable
 * with the {@link java.io.File java.io.File} class. Paths created by other
 * providers are unlikely to be interoperable with the abstract path names
 * represented by {@code java.io.File}. The {@link java.io.File#toPath toPath}
 * method may be used to obtain a {@code Path} from the abstract path name
 * represented by a {@code java.io.File} object. The resulting {@code Path} can
 * be used to operate on the same file as the {@code java.io.File} object. In
 * addition, the {@link #toFile toFile} method is useful to construct a {@code
 * File} from the {@code String} representation of a {@code Path}.
 * <p>
 * <h2>Concurrency</h2>
 * <p> Implementations of this interface are immutable and safe for use by
 * multiple concurrent threads.
 *
 * @see Paths
 */
public class ObjectStoragePath implements Path {

    private final ObjectStorageFileSystem fileSystem;
    private final boolean absolute;
    private final boolean directory;
    private final String pathName;
    private String[] names;
    private ObjectStorageFileAttributes fileAttributes;

    ObjectStoragePath(ObjectStorageFileSystem fileSystem, boolean absolute, boolean directory, String pathName, ObjectStorageFileAttributes fileAttributes) {
        if (fileSystem == null) {
            throw new NullPointerException("fileSystem");
        }
        if (pathName == null) {
            throw new NullPointerException("pathName");
        }
        this.fileSystem = fileSystem;
        this.absolute = absolute;
        this.directory = directory;
        this.pathName = pathName;
        this.fileAttributes = fileAttributes;
    }

    static ObjectStoragePath fromFileAttributes(ObjectStorageFileSystem fileSystem, ObjectStorageFileAttributes fileAttributes) {
        String separator = fileSystem.getSeparator();
        String pathName = fileAttributes.fileKey().toString();
        if (fileAttributes.isDirectory() && pathName.endsWith(separator)) {
            int beginIndex = 0;
            int endIndex = pathName.length() - separator.length();
            pathName = pathName.substring(beginIndex, endIndex);
        }
        return new ObjectStoragePath(fileSystem, true, fileAttributes.isDirectory(), pathName, fileAttributes);
    }

    static ObjectStoragePath parsePath(ObjectStorageFileSystem fileSystem, String pathName) {
        String separator = fileSystem.getSeparator();
        if (pathName.isEmpty()) {
            return fileSystem.getEmpty();
        }
        if (pathName.equals(separator)) {
            return fileSystem.getRoot();
        }
        boolean absolute = false;
        boolean directory = false;
        int beginIndex = 0;
        int endIndex = pathName.length();
        if (pathName.startsWith(separator)) {
            absolute = true;
            beginIndex += separator.length();
        }
        if (pathName.endsWith("/")) {
            directory = true;
            endIndex -= separator.length();
        }
        return new ObjectStoragePath(fileSystem, absolute, directory, pathName.substring(beginIndex, endIndex), null);
    }

    String getLocation() {
        return fileSystem.getAddress() + toString();
    }

    private String[] getNames() {
        if (names == null) {
            if (pathName.isEmpty()) {
                names = new String[0];
            } else {
                names = pathName.split(fileSystem.getSeparator());
            }
        }
        return names;
    }

    private ObjectStoragePath parsePath(String pathName) {
        return parsePath(fileSystem, pathName);
    }

    /**
     * Returns the file system that created this object.
     *
     * @return the file system that created this object
     */
    @Override
    public FileSystem getFileSystem() {
        return fileSystem;
    }

    /**
     * Tells whether or not this path is absolute.
     * <p>
     * <p> An absolute path is complete in that it doesn't need to be combined
     * with other path information in order to locate a file.
     *
     * @return {@code true} if, and only if, this path is absolute
     */
    @Override
    public boolean isAbsolute() {
        return absolute;
    }


    boolean isDirectory() {
        return directory;
    }

    ObjectStorageFileAttributes getFileAttributes() {
        return fileAttributes;
    }

    void setFileAttributes(ObjectStorageFileAttributes fileAttributes) {
        this.fileAttributes = fileAttributes;
    }

    /**
     * Returns the root component of this path as a {@code Path} object,
     * or {@code null} if this path does not have a root component.
     *
     * @return a path representing the root component of this path,
     * or {@code null}
     */
    @Override
    public Path getRoot() {
        return isAbsolute() ? fileSystem.getRoot() : null;
    }

    /**
     * Returns the name of the file or directory denoted by this path as a
     * {@code Path} object. The file name is the <em>farthest</em> element from
     * the root in the directory hierarchy.
     *
     * @return a path representing the name of the file or directory, or
     * {@code null} if this path has zero elements
     */
    @Override
    public Path getFileName() {
        int nameCount = getNameCount();
        if (nameCount == 0) {
            return null;
        }
        String name = getNames()[nameCount - 1];
        return new ObjectStoragePath(fileSystem, false, directory, name, fileAttributes);
    }

    /**
     * Returns the <em>parent path</em>, or {@code null} if this path does not
     * have a parent.
     * <p>
     * <p> The parent of this path object consists of this path's root
     * component, if any, and each element in the path except for the
     * <em>farthest</em> from the root in the directory hierarchy. This method
     * does not access the file system; the path or its parent may not exist.
     * Furthermore, this method does not eliminate special names such as "."
     * and ".." that may be used in some implementations. On UNIX for example,
     * the parent of "{@code /a/b/c}" is "{@code /a/b}", and the parent of
     * {@code "x/y/.}" is "{@code x/y}". This method may be used with the {@link
     * #normalize normalize} method, to eliminate redundant names, for cases where
     * <em>shell-like</em> navigation is required.
     * <p>
     * <p> If this path has one or more elements, and no root component, then
     * this method is equivalent to evaluating the expression:
     * <blockquote><pre>
     * subpath(0,&nbsp;getNameCount()-1);
     * </pre></blockquote>
     *
     * @return a path representing the path's parent
     */
    @Override
    public Path getParent() {
        int nameCount = getNameCount();
        if (nameCount == 0) {
            return null;
        }
        return subpath(0, nameCount - 1);
    }

    /**
     * Returns the number of name elements in the path.
     *
     * @return the number of elements in the path, or {@code 0} if this path
     * only represents a root component
     */
    @Override
    public int getNameCount() {
        return getNames().length;
    }

    /**
     * Returns a name element of this path as a {@code Path} object.
     * <p>
     * <p> The {@code index} parameter is the index of the name element to return.
     * The element that is <em>closest</em> to the root in the directory hierarchy
     * has index {@code 0}. The element that is <em>farthest</em> from the root
     * has index {@link #getNameCount count}{@code -1}.
     *
     * @param index the index of the element
     * @return the name element
     * @throws IllegalArgumentException if {@code index} is negative, {@code index} is greater than or
     *                                  equal to the number of elements, or this path has zero name
     *                                  elements
     */
    @Override
    public Path getName(int index) {
        return new ObjectStoragePath(fileSystem, false, false, getNames()[index], null);
    }

    /**
     * Returns a relative {@code Path} that is a subsequence of the name
     * elements of this path.
     * <p>
     * <p> The {@code beginIndex} and {@code endIndex} parameters specify the
     * subsequence of name elements. The name that is <em>closest</em> to the root
     * in the directory hierarchy has index {@code 0}. The name that is
     * <em>farthest</em> from the root has index {@link #getNameCount
     * count}{@code -1}. The returned {@code Path} object has the name elements
     * that begin at {@code beginIndex} and extend to the element at index {@code
     * endIndex-1}.
     *
     * @param beginIndex the index of the first element, inclusive
     * @param endIndex   the index of the last element, exclusive
     * @return a new {@code Path} object that is a subsequence of the name
     * elements in this {@code Path}
     * @throws IllegalArgumentException if {@code beginIndex} is negative, or greater than or equal to
     *                                  the number of elements. If {@code endIndex} is less than or
     *                                  equal to {@code beginIndex}, or larger than the number of elements.
     */
    @Override
    public Path subpath(int beginIndex, int endIndex) {
        String[] subPath = Arrays.copyOfRange(names, beginIndex, endIndex);
        String subPathName = String.join(fileSystem.getSeparator(), subPath);
        return new ObjectStoragePath(fileSystem,
                                     beginIndex == 0 && absolute,
                                     endIndex < getNameCount() || directory,
                                     subPathName,
                                     null);
    }

    /**
     * Tests if this path starts with the given path.
     * <p>
     * <p> This path <em>starts</em> with the given path if this path's root
     * component <em>starts</em> with the root component of the given path,
     * and this path starts with the same name elements as the given path.
     * If the given path has more name elements than this path then {@code false}
     * is returned.
     * <p>
     * <p> Whether or not the root component of this path starts with the root
     * component of the given path is file system specific. If this path does
     * not have a root component and the given path has a root component then
     * this path does not beginIndex with the given path.
     * <p>
     * <p> If the given path is associated with a different {@code FileSystem}
     * to this path then {@code false} is returned.
     *
     * @param other the given path
     * @return {@code true} if this path starts with the given path; otherwise
     * {@code false}
     */
    @Override
    public boolean startsWith(Path other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        if (other instanceof ObjectStoragePath) {
            return pathName.startsWith(((ObjectStoragePath) other).pathName);
        }
        return pathName.startsWith(other.toString());
    }

    /**
     * Tests if this path starts with a {@code Path}, constructed by converting
     * the given path string, in exactly the manner specified by the {@link
     * #startsWith(Path) startsWith(Path)} method. On UNIX for example, the path
     * "{@code foo/bar}" starts with "{@code foo}" and "{@code foo/bar}". It
     * does not beginIndex with "{@code f}" or "{@code fo}".
     *
     * @param other the given path string
     * @return {@code true} if this path starts with the given path; otherwise
     * {@code false}
     * @throws InvalidPathException If the path string cannot be converted to a Path.
     */
    @Override
    public boolean startsWith(String other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        return startsWith(parsePath(other));
    }

    /**
     * Tests if this path ends with the given path.
     * <p>
     * <p> If the given path has <em>N</em> elements, and no root component,
     * and this path has <em>N</em> or more elements, then this path ends with
     * the given path if the last <em>N</em> elements of each path, starting at
     * the element farthest from the root, are equal.
     * <p>
     * <p> If the given path has a root component then this path ends with the
     * given path if the root component of this path <em>ends with</em> the root
     * component of the given path, and the corresponding elements of both paths
     * are equal. Whether or not the root component of this path ends with the
     * root component of the given path is file system specific. If this path
     * does not have a root component and the given path has a root component
     * then this path does not endIndex with the given path.
     * <p>
     * <p> If the given path is associated with a different {@code FileSystem}
     * to this path then {@code false} is returned.
     *
     * @param other the given path
     * @return {@code true} if this path ends with the given path; otherwise
     * {@code false}
     */
    @Override
    public boolean endsWith(Path other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        if (other instanceof ObjectStoragePath) {
            return pathName.endsWith(((ObjectStoragePath) other).pathName);
        }
        return pathName.endsWith(other.toString());
    }

    /**
     * Tests if this path ends with a {@code Path}, constructed by converting
     * the given path string, in exactly the manner specified by the {@link
     * #endsWith(Path) endsWith(Path)} method. On UNIX for example, the path
     * "{@code foo/bar}" ends with "{@code foo/bar}" and "{@code bar}". It does
     * not endIndex with "{@code r}" or "{@code /bar}". Note that trailing separators
     * are not taken into account, and so invoking this method on the {@code
     * Path}"{@code foo/bar}" with the {@code String} "{@code bar/}" returns
     * {@code true}.
     *
     * @param other the given path string
     * @return {@code true} if this path ends with the given path; otherwise
     * {@code false}
     * @throws InvalidPathException If the path string cannot be converted to a Path.
     */
    @Override
    public boolean endsWith(String other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        return startsWith(parsePath(other));
    }

    /**
     * Returns a path that is this path with redundant name elements eliminated.
     * <p>
     * <p> The precise definition of this method is implementation dependent but
     * in general it derives from this path, a path that does not contain
     * <em>redundant</em> name elements. In many file systems, the "{@code .}"
     * and "{@code ..}" are special names used to indicate the current directory
     * and parent directory. In such file systems all occurrences of "{@code .}"
     * are considered redundant. If a "{@code ..}" is preceded by a
     * non-"{@code ..}" name then both names are considered redundant (the
     * process to identify such names is repeated until it is no longer
     * applicable).
     * <p>
     * <p> This method does not access the file system; the path may not locate
     * a file that exists. Eliminating "{@code ..}" and a preceding name from a
     * path may result in the path that locates a different file than the original
     * path. This can arise when the preceding name is a symbolic link.
     *
     * @return the resulting path or this path if it does not contain
     * redundant name elements; an empty path is returned if this path
     * does have a root component and all name elements are redundant
     * @see #getParent
     * @see #toRealPath
     */
    @Override
    public Path normalize() {
        // We don't support links and special directories "." and ".."
        return this;
    }

    /**
     * Resolve the given path against this path.
     * <p>
     * <p> If the {@code other} parameter is an {@link #isAbsolute() absolute}
     * path then this method trivially returns {@code other}. If {@code other}
     * is an <i>empty path</i> then this method trivially returns this path.
     * Otherwise this method considers this path to be a directory and resolves
     * the given path against this path. In the simplest case, the given path
     * does not have a {@link #getRoot root} component, in which case this method
     * <em>joins</em> the given path to this path and returns a resulting path
     * that {@link #endsWith ends} with the given path. Where the given path has
     * a root component then resolution is highly implementation dependent and
     * therefore unspecified.
     *
     * @param other the path to resolve against this path
     * @return the resulting path
     * @see #relativize
     */
    @Override
    public Path resolve(Path other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        if (other.isAbsolute() || toString().isEmpty()) {
            return other;
        }
        if (other.toString().isEmpty()) {
            return this;
        }
        if (directory) {
            return parsePath(toString() + other.toString());
        }
        throw new IllegalArgumentException("other");
    }

    /**
     * Converts a given path string to a {@code Path} and resolves it against
     * this {@code Path} in exactly the manner specified by the {@link
     * #resolve(Path) resolve} method. For example, suppose that the name
     * separator is "{@code /}" and a path represents "{@code foo/bar}", then
     * invoking this method with the path string "{@code gus}" will result in
     * the {@code Path} "{@code foo/bar/gus}".
     *
     * @param other the path string to resolve against this path
     * @return the resulting path
     * @throws InvalidPathException if the path string cannot be converted to a Path.
     * @see FileSystem#getPath
     */
    @Override
    public Path resolve(String other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        return resolve(parsePath(other));
    }

    /**
     * Resolves the given path against this path's {@link #getParent parent}
     * path. This is useful where a file name needs to be <i>replaced</i> with
     * another file name. For example, suppose that the name separator is
     * "{@code /}" and a path represents "{@code dir1/dir2/foo}", then invoking
     * this method with the {@code Path} "{@code bar}" will result in the {@code
     * Path} "{@code dir1/dir2/bar}". If this path does not have a parent path,
     * or {@code other} is {@link #isAbsolute() absolute}, then this method
     * returns {@code other}. If {@code other} is an empty path then this method
     * returns this path's parent, or where this path doesn't have a parent, the
     * empty path.
     *
     * @param other the path to resolve against this path's parent
     * @return the resulting path
     * @see #resolve(Path)
     */
    @Override
    public Path resolveSibling(Path other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        if (other.toString().isEmpty()) {
            return this;
        }
        Path parent = getParent();
        if (parent == null || other.isAbsolute()) {
            return other;
        }
        return parent.resolve(other);
    }

    /**
     * Converts a given path string to a {@code Path} and resolves it against
     * this path's {@link #getParent parent} path in exactly the manner
     * specified by the {@link #resolveSibling(Path) resolveSibling} method.
     *
     * @param other the path string to resolve against this path's parent
     * @return the resulting path
     * @throws InvalidPathException if the path string cannot be converted to a Path.
     * @see FileSystem#getPath
     */
    @Override
    public Path resolveSibling(String other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        return resolveSibling(parsePath(other));
    }

    /**
     * Constructs a relative path between this path and a given path.
     * <p>
     * <p> Relativization is the inverse of {@link #resolve(Path) resolution}.
     * This method attempts to construct a {@link #isAbsolute relative} path
     * that when {@link #resolve(Path) resolved} against this path, yields a
     * path that locates the same file as the given path. For example, on UNIX,
     * if this path is {@code "/a/b"} and the given path is {@code "/a/b/c/d"}
     * then the resulting relative path would be {@code "c/d"}. Where this
     * path and the given path do not have a {@link #getRoot root} component,
     * then a relative path can be constructed. A relative path cannot be
     * constructed if only one of the paths have a root component. Where both
     * paths have a root component then it is implementation dependent if a
     * relative path can be constructed. If this path and the given path are
     * {@link #equals equal} then an <i>empty path</i> is returned.
     * <p>
     * <p> For any two {@link #normalize normalized} paths <i>p</i> and
     * <i>q</i>, where <i>q</i> does not have a root component,
     * <blockquote>
     * <i>p</i><tt>.relativize(</tt><i>p</i><tt>.resolve(</tt><i>q</i><tt>)).equals(</tt><i>q</i><tt>)</tt>
     * </blockquote>
     * <p>
     * <p> When symbolic links are supported, then whether the resulting path,
     * when resolved against this path, yields a path that can be used to locate
     * the {@link Files#isSameFile same} file as {@code other} is implementation
     * dependent. For example, if this path is  {@code "/a/b"} and the given
     * path is {@code "/a/x"} then the resulting relative path may be {@code
     * "../x"}. If {@code "b"} is a symbolic link then is implementation
     * dependent if {@code "a/b/../x"} would locate the same file as {@code "/a/x"}.
     *
     * @param other the path to relativize against this path
     * @return the resulting relative path, or an empty path if both paths are
     * equal
     * @throws IllegalArgumentException if {@code other} is not a {@code Path} that can be relativized
     *                                  against this path
     */
    @Override
    public Path relativize(Path other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        if (equals(other)) {
            return fileSystem.getEmpty();
        }
        ObjectStoragePath path2 = (ObjectStoragePath) other;
        if (isAbsolute() != path2.isAbsolute()) {
            throw new IllegalArgumentException("other");
        }
        String[] names1 = getNames();
        String[] names2 = path2.getNames();

        for (int i = 0; i < names1.length; i++) {
            if (i >= names2.length || !names1[i].equals(names2[i])) {
                return path2;
            }
        }

        return path2.subpath(names1.length, path2.getNameCount());
    }

    /**
     * Returns a URI to represent this path.
     * <p>
     * <p> This method constructs an absolute {@link URI} with a {@link
     * URI#getScheme() scheme} equal to the URI scheme that identifies the
     * provider. The exact form of the scheme specific part is highly provider
     * dependent.
     * <p>
     * <p> In the case of the default provider, the URI is hierarchical with
     * a {@link URI#getPath() path} component that is absolute. The query and
     * fragment components are undefined. Whether the authority component is
     * defined or not is implementation dependent. There is no guarantee that
     * the {@code URI} may be used to construct a {@link File java.io.File}.
     * In particular, if this path represents a Universal Naming Convention (UNC)
     * path, then the UNC server name may be encoded in the authority component
     * of the resulting URI. In the case of the default provider, and the file
     * exists, and it can be determined that the file is a directory, then the
     * resulting {@code URI} will end with a slash.
     * <p>
     * <p> The default provider provides a similar <em>round-trip</em> guarantee
     * to the {@link File} class. For a given {@code Path} <i>p</i> it
     * is guaranteed that
     * <blockquote><tt>
     * {@link Paths#get(URI) Paths.getItem}(</tt><i>p</i><tt>.toUri()).equals(</tt><i>p</i>
     * <tt>.{@link #toAbsolutePath() toAbsolutePath}())</tt>
     * </blockquote>
     * so long as the original {@code Path}, the {@code URI}, and the new {@code
     * Path} are all created in (possibly different invocations of) the same
     * Java virtual machine. Whether other providers make any guarantees is
     * provider specific and therefore unspecified.
     * <p>
     * <p> When a file system is constructed to access the contents of a file
     * as a file system then it is highly implementation specific if the returned
     * URI represents the given path in the file system or it represents a
     * <em>compound</em> URI that encodes the URI of the enclosing file system.
     * A format for compound URIs is not defined in this release; such a scheme
     * may be added in a future release.
     *
     * @return the URI representing this path
     * @throws IOError           if an I/O error occurs obtaining the absolute path, or where a
     *                           file system is constructed to access the contents of a file as
     *                           a file system, and the URI of the enclosing file system cannot be
     *                           obtained
     * @throws SecurityException In the case of the default provider, and a security manager
     *                           is installed, the {@link #toAbsolutePath toAbsolutePath} method
     *                           throws a security exception.
     */
    @Override
    public URI toUri() {
        String pathName = this.pathName;
        if (directory) {
            pathName += fileSystem.getSeparator();
        }
        try {
            return new URI(fileSystem.provider().getScheme(), fileSystem.getAddress() + "/" + pathName, null);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns a {@code Path} object representing the absolute path of this
     * path.
     * <p>
     * <p> If this path is already {@link Path#isAbsolute absolute} then this
     * method simply returns this path. Otherwise, this method resolves the path
     * in an implementation dependent manner, typically by resolving the path
     * against a file system default directory. Depending on the implementation,
     * this method may throw an I/O error if the file system is not accessible.
     *
     * @return a {@code Path} object representing the absolute path
     * @throws IOError           if an I/O error occurs
     * @throws SecurityException In the case of the default provider, a security manager
     *                           is installed, and this path is not absolute, then the security
     *                           manager's {@link SecurityManager#checkPropertyAccess(String)
     *                           checkPropertyAccess} method is invoked to check access to the
     *                           system property {@code user.dir}
     */
    @Override
    public Path toAbsolutePath() {
        if (isAbsolute()) {
            return this;
        }
        throw new IOError(new IOException("default directory to resolve against are not supported"));
    }

    /**
     * Returns the <em>real</em> path of an existing file.
     * <p>
     * <p> The precise definition of this method is implementation dependent but
     * in general it derives from this path, an {@link #isAbsolute absolute}
     * path that locates the {@link Files#isSameFile same} file as this path, but
     * with name elements that represent the actual name of the directories
     * and the file. For example, where filename comparisons on a file system
     * are case insensitive then the name elements represent the names in their
     * actual case. Additionally, the resulting path has redundant name
     * elements removed.
     * <p>
     * <p> If this path is relative then its absolute path is first obtained,
     * as if by invoking the {@link #toAbsolutePath toAbsolutePath} method.
     * <p>
     * <p> The {@code options} array may be used to indicate how symbolic links
     * are handled. By default, symbolic links are resolved to their final
     * target. If the option {@link LinkOption#NOFOLLOW_LINKS NOFOLLOW_LINKS} is
     * present then this method does not resolve symbolic links.
     * <p>
     * Some implementations allow special names such as "{@code ..}" to refer to
     * the parent directory. When deriving the <em>real path</em>, and a
     * "{@code ..}" (or equivalent) is preceded by a non-"{@code ..}" name then
     * an implementation will typically cause both names to be removed. When
     * not resolving symbolic links and the preceding name is a symbolic link
     * then the names are only removed if it guaranteed that the resulting path
     * will locate the same file as this path.
     *
     * @param options options indicating how symbolic links are handled
     * @return an absolute path represent the <em>real</em> path of the file
     * located by this object
     * @throws IOException       if the file does not exist or an I/O error occurs
     * @throws SecurityException In the case of the default provider, and a security manager
     *                           is installed, its {@link SecurityManager#checkRead(String) checkRead}
     *                           method is invoked to check read access to the file, and where
     *                           this path is not absolute, its {@link SecurityManager#checkPropertyAccess(String)
     *                           checkPropertyAccess} method is invoked to check access to the
     *                           system property {@code user.dir}
     */
    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        // we don't support links as well as the directories "." and ".."
        return toAbsolutePath();
    }

    /**
     * Returns a {@link File} object representing this path. Where this {@code
     * Path} is associated with the default provider, then this method is
     * equivalent to returning a {@code File} object constructed with the
     * {@code String} representation of this path.
     * <p>
     * <p> If this path was created by invoking the {@code File} {@link
     * File#toPath toPath} method then there is no guarantee that the {@code
     * File} object returned by this method is {@link #equals equal} to the
     * original {@code File}.
     *
     * @return a {@code File} object representing this path
     * @throws UnsupportedOperationException if this {@code Path} is not associated with the default provider
     */
    @Override
    public File toFile() {
        throw new UnsupportedOperationException();
    }

    /**
     * Registers the file located by this path with a watch service.
     * <p>
     * <p> In this release, this path locates a directory that exists. The
     * directory is registered with the watch service so that entries in the
     * directory can be watched. The {@code events} parameter is the events to
     * register and may contain the following events:
     * <ul>
     * <li>{@link StandardWatchEventKinds#ENTRY_CREATE ENTRY_CREATE} -
     * entry created or moved into the directory</li>
     * <li>{@link StandardWatchEventKinds#ENTRY_DELETE ENTRY_DELETE} -
     * entry deleted or moved out of the directory</li>
     * <li>{@link StandardWatchEventKinds#ENTRY_MODIFY ENTRY_MODIFY} -
     * entry in directory was modified</li>
     * </ul>
     * <p>
     * <p> The {@link WatchEvent#context context} for these events is the
     * relative path between the directory located by this path, and the path
     * that locates the directory entry that is created, deleted, or modified.
     * <p>
     * <p> The set of events may include additional implementation specific
     * event that are not defined by the enum {@link StandardWatchEventKinds}
     * <p>
     * <p> The {@code modifiers} parameter specifies <em>modifiers</em> that
     * qualify how the directory is registered. This release does not define any
     * <em>standard</em> modifiers. It may contain implementation specific
     * modifiers.
     * <p>
     * <p> Where a file is registered with a watch service by means of a symbolic
     * link then it is implementation specific if the watch continues to depend
     * on the existence of the symbolic link after it is registered.
     *
     * @param watcher   the watch service to which this object is to be registered
     * @param events    the events for which this object should be registered
     * @param modifiers the modifiers, if any, that modify how the object is registered
     * @return a key representing the registration of this object with the
     * given watch service
     * @throws UnsupportedOperationException if unsupported events or modifiers are specified
     * @throws IllegalArgumentException      if an invalid combination of events or modifiers is specified
     * @throws ClosedWatchServiceException   if the watch service is closed
     * @throws NotDirectoryException         if the file is registered to watch the entries in a directory
     *                                       and the file is not a directory  <i>(optional specific exception)</i>
     * @throws IOException                   if an I/O error occurs
     * @throws SecurityException             In the case of the default provider, and a security manager is
     *                                       installed, the {@link SecurityManager#checkRead(String) checkRead}
     *                                       method is invoked to check read access to the file.
     */
    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Registers the file located by this path with a watch service.
     * <p>
     * <p> An invocation of this method behaves in exactly the same way as the
     * invocation
     * <pre>
     *     watchable.{@link #register(WatchService, WatchEvent.Kind[], WatchEvent.Modifier[]) register}(watcher, events, new WatchEvent.Modifier[0]);
     * </pre>
     * <p>
     * <p> <b>Usage Example:</b>
     * Suppose we wish to register a directory for entry create, delete, and modify
     * events:
     * <pre>
     *     Path dir = ...
     *     WatchService watcher = ...
     *
     *     WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
     * </pre>
     *
     * @param watcher The watch service to which this object is to be registered
     * @param events  The events for which this object should be registered
     * @return A key representing the registration of this object with the
     * given watch service
     * @throws UnsupportedOperationException If unsupported events are specified
     * @throws IllegalArgumentException      If an invalid combination of events is specified
     * @throws ClosedWatchServiceException   If the watch service is closed
     * @throws NotDirectoryException         If the file is registered to watch the entries in a directory
     *                                       and the file is not a directory  <i>(optional specific exception)</i>
     * @throws IOException                   If an I/O error occurs
     * @throws SecurityException             In the case of the default provider, and a security manager is
     *                                       installed, the {@link SecurityManager#checkRead(String) checkRead}
     *                                       method is invoked to check read access to the file.
     */
    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns an iterator over the name elements of this path.
     * <p>
     * <p> The first element returned by the iterator represents the name
     * element that is closest to the root in the directory hierarchy, the
     * second element is the next closest, and so on. The last element returned
     * is the name of the file or directory denoted by this path. The {@link
     * #getRoot root} component, if present, is not returned by the iterator.
     *
     * @return an iterator over the name elements of this path.
     */
    @Override
    public Iterator<Path> iterator() {
        ArrayList<Path> list = new ArrayList<>();
        int nameCount = getNameCount();
        for (int i = 0; i < nameCount; i++) {
            list.add(getName(i));
        }
        return list.iterator();
    }

    /**
     * Compares two abstract paths lexicographically. The ordering defined by
     * this method is provider specific, and in the case of the default
     * provider, platform specific. This method does not access the file system
     * and neither file is required to exist.
     * <p>
     * <p> This method may not be used to compare paths that are associated
     * with different file system providers.
     *
     * @param other the path compared to this path.
     * @return zero if the argument is {@link #equals equal} to this path, a
     * value less than zero if this path is lexicographically less than
     * the argument, or a value greater than zero if this path is
     * lexicographically greater than the argument
     * @throws ClassCastException if the paths are associated with different providers
     */
    @Override
    public int compareTo(Path other) {
        ObjectStoragePath otherPath = (ObjectStoragePath) other;
        int n = Math.min(getNameCount(), otherPath.getNameCount());
        for (int i = 0; i < n; i++) {
            String name1 = getNames()[i];
            String name2 = otherPath.getNames()[i];
            int delta = name1.compareTo(name2);
            if (delta != 0) {
                return delta;
            }
        }
        String name1 = n < getNameCount() ? getNames()[n] : null;
        String name2 = n < otherPath.getNameCount() ? otherPath.getNames()[n] : null;
        if (name1 == null && name2 == null) {
            return 0;
        }
        if (name1 != null) {
            return name1.compareTo("");
        } else {
            return "".compareTo(name2);
        }
    }

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        String pathName = this.pathName;
        String separator = fileSystem.getSeparator();
        if (pathName.isEmpty()) {
            if (absolute) {
                pathName = separator;
            }
        } else {
            if (absolute) {
                pathName = separator + pathName;
            }
            if (directory) {
                pathName += separator;
            }
        }
        return pathName;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ObjectStoragePath)) {
            return false;
        }
        ObjectStoragePath other = (ObjectStoragePath) o;
        return absolute == other.absolute
                && directory == other.directory
                && fileSystem == other.fileSystem
                && pathName.equals(other.pathName);
    }

    @Override
    public int hashCode() {
        int result = fileSystem.hashCode();
        result = 31 * result + (absolute ? 1 : 0);
        result = 31 * result + (directory ? 1 : 0);
        result = 31 * result + pathName.hashCode();
        return result;
    }
}
