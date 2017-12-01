package org.esa.snap.objectstoragefs;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Provides an interface to a file system and is the factory for objects to
 * access files and other objects in the file system.
 */
@SuppressWarnings("WeakerAccess")
public class ObjectStorageFileSystem extends FileSystem {
    private final ObjectStorageFileSystemProvider provider;
    private final String address;
    private final ObjectStoragePath root;
    private final ObjectStoragePath empty;
    private String separator;
    private boolean closed;
    private List<ObjectStorageByteChannel> openChannels;
    private ObjectStorageWalker walker;

    public ObjectStorageFileSystem(ObjectStorageFileSystemProvider provider, String address, String separator) {
        if (provider == null) {
            throw new NullPointerException("provider");
        }
        if (address == null) {
            throw new NullPointerException("address");
        }
        if (address.isEmpty()) {
            throw new IllegalArgumentException("address is empty");
        }
        if (separator.isEmpty()) {
            throw new IllegalArgumentException("separator is empty");
        }
        this.provider = provider;
        this.address = address;
        this.separator = separator;
        this.closed = false;
        this.openChannels = new ArrayList<>();
        this.root = new ObjectStoragePath(this, true, true, "", new ObjectStorageFileAttributes("/", true,null, 0));
        this.empty = new ObjectStoragePath(this, false, false, "", new ObjectStorageFileAttributes("", false,null, 0));
    }

    /**
     * Returns the provider that created this file system.
     *
     * @return The provider that created this file system.
     */
    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    /**
     * Returns the address URL as string.
     *
     * @return The address
     */
    public String getAddress() {
        return address;
    }

    /**
     * Returns the root path.
     *
     * @return The root path
     */
    ObjectStoragePath getRoot() {
        return root;
    }

    /**
     * Returns the empty path.
     *
     * @return The empty path
     */
    ObjectStoragePath getEmpty() {
        return empty;
    }

    /**
     * Closes this file system.
     *
     * @throws IOException                   If an I/O error occurs
     * @throws UnsupportedOperationException Thrown in the case of the default file system
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        Channel[] channels = openChannels.toArray(new Channel[openChannels.size()]);
        for (Channel channel : channels) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
        }
        provider.unlinkFileSystem(this);
    }

    /**
     * Tells whether or not this file system is open.
     *
     * @return {@code true} if, and only if, this file system is open
     */
    @Override
    public boolean isOpen() {
        return !closed;
    }

    /**
     * Tells whether or not this file system allows only read-only access to
     * its file stores.
     *
     * @return {@code true} if, and only if, this file system provides
     * read-only access
     */
    @Override
    public boolean isReadOnly() {
        return true;
    }

    /**
     * Returns the name separator, represented as a string.
     *
     * @return The name separator
     */
    @Override
    public String getSeparator() {
        return separator;
    }

    /**
     * Returns an object to iterate over the paths of the root directories.
     *
     * @return An object to iterate over the root directories
     */
    @Override
    public Iterable<Path> getRootDirectories() {
        return walkDir(getRoot(), path -> ((ObjectStoragePath) path).isDirectory());
    }

    /**
     * Returns an object to iterate over the underlying file stores.
     *
     * @return An object to iterate over the backing file stores
     */
    @Override
    public Iterable<FileStore> getFileStores() {
        // TODO - implement me
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the set of the {@link FileAttributeView#name names} of the file
     * attribute views supported by this {@code FileSystem}.
     *
     * @return An unmodifiable set of the names of the supported file attribute
     * views
     */
    @Override
    public Set<String> supportedFileAttributeViews() {
        // TODO - implement me
        throw new UnsupportedOperationException();
    }

    /**
     * Converts a path string, or a sequence of strings that when joined form
     * a path string, to a {@code Path}. If {@code more} does not specify any
     * elements then the value of the {@code first} parameter is the path string
     * to convert. If {@code more} specifies one or more elements then each
     * non-empty string, including {@code first}, is considered to be a sequence
     * of name elements (see {@link Path}) and is joined to form a path string.
     * The details as to how the Strings are joined is provider specific but
     * typically they will be joined using the {@link #getSeparator
     * name-separator} as the separator. For example, if the name separator is
     * "{@code /}" and {@code getPath("/foo","bar","gus")} is invoked, then the
     * path string {@code "/foo/bar/gus"} is converted to a {@code Path}.
     * A {@code Path} representing an empty path is returned if {@code first}
     * is the empty string and {@code more} does not contain any non-empty
     * strings.
     *
     * @param first the path string or initial part of the path string
     * @param more  additional strings to be joined to form the path string
     * @return the resulting {@code Path}
     * @throws InvalidPathException If the path string cannot be converted
     */
    @Override
    public Path getPath(String first, String... more) {
        assertOpen();
        String pathName = first;
        if (more.length > 0) {
            String separator = getSeparator();
            pathName += separator + String.join(separator, more);
        }
        return ObjectStoragePath.parsePath(this, pathName);
    }

    /**
     * Returns a {@code PathMatcher} that performs match operations on the
     * {@code String} representation of {@link Path} objects by interpreting a
     * given pattern.
     *
     * @param syntaxAndPattern The syntax and pattern
     * @return A path matcher that may be used to match paths against the pattern
     * @throws IllegalArgumentException      If the parameter does not take the form: {@code syntax:pattern}
     * @throws PatternSyntaxException        If the pattern is invalid
     * @throws UnsupportedOperationException If the pattern syntax is not known to the implementation
     * @see Files#newDirectoryStream(Path, String)
     */
    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        // TODO - implement me
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the {@code UserPrincipalLookupService} for this file system
     * <i>(optional operation)</i>. The resulting lookup service may be used to
     * lookup user or group names.
     *
     * @return The {@code UserPrincipalLookupService} for this file system
     * @throws UnsupportedOperationException If this {@code FileSystem} does not does have a lookup service
     */
    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        // TODO - implement me
        throw new UnsupportedOperationException();
    }

    /**
     * Constructs a new {@link WatchService} <i>(optional operation)</i>.
     *
     * @return a new watch service
     * @throws UnsupportedOperationException If this {@code FileSystem} does not support watching file system
     *                                       objects for changes and events. This exception is not thrown
     *                                       by {@code FileSystems} created by the default provider.
     * @throws IOException                   If an I/O error occurs
     */
    @Override
    public WatchService newWatchService() throws IOException {
        // TODO - implement me
        throw new UnsupportedOperationException();
    }

    private void assertOpen() throws ClosedFileSystemException {
        if (closed) {
            throw new ClosedFileSystemException();
        }
    }

    SeekableByteChannel openByteChannel(ObjectStoragePath path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs) throws IOException {
        boolean plainReadMode = options.isEmpty() || options.size() == 1 && options.contains(StandardOpenOption.READ);
        boolean noCreateAttributes = attrs.length == 0;
        if (plainReadMode && noCreateAttributes) {
            return addByteChannel(new ObjectStorageByteChannel(path));
        }
        throw new UnsupportedOperationException();
    }

    ObjectStorageByteChannel addByteChannel(ObjectStorageByteChannel channel) {
        openChannels.add(channel);
        return channel;
    }

    void removeByteChannel(ObjectStorageByteChannel channel) {
        openChannels.remove(channel);
    }

    Iterable<Path> walkDir(Path dir, DirectoryStream.Filter<? super Path> filter) {
        assertOpen();
        Path path = dir.toAbsolutePath();
        String prefix = path.toString().substring(1);
        List<ObjectStorageFileAttributes> files;
        if (walker == null) {
            walker = provider.newObjectStorageWalker();
        }
        try {
            files = walker.walk(address, prefix, getSeparator());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return files.stream()
                .map(f -> ObjectStoragePath.fromFileAttributes(this, f))
                .filter(p -> filterPath(p, filter))
                .collect(Collectors.toList());
    }

    private boolean filterPath(ObjectStoragePath path, DirectoryStream.Filter<? super Path> filter) {
        try {
            return filter.accept(path);
        } catch (IOException e) {
            return false;
        }
    }
}
