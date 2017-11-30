package org.esa.snap.objectstoragefs;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;

/**
 * A byte channel that maintains a current <i>position</i> and allows the
 * position to be changed.
 */
class ObjectStorageByteChannel implements SeekableByteChannel {

    private final URL url;
    private final long contentLength;
    private final String contentType;
    private final byte[] buffer;
    private HttpURLConnection connection;
    private long position;

    ObjectStorageByteChannel(ObjectStoragePath path) throws IOException {
        this(path, 1024 * 16);
    }

    ObjectStorageByteChannel(ObjectStoragePath path, int bufferSize) throws IOException {
        url = new URL(path.getLocation());
        this.connection = connect();
        this.contentLength = connection.getContentLengthLong();
        this.contentType = connection.getContentType();
        this.position = 0;
        this.buffer = new byte[bufferSize];
    }

    /**
     * Returns the current size of entity to which this channel is connected.
     *
     * @return The current size, measured in bytes
     * @throws ClosedChannelException If this channel is closed
     * @throws IOException            If some other I/O error occurs
     */
    @Override
    public long size() throws IOException {
        return contentLength;
    }

    /**
     * Returns this channel's position.
     *
     * @return This channel's position, a non-negative integer counting the number of bytes
     * from the beginning of the entity to the current position
     * @throws ClosedChannelException If this channel is closed
     * @throws IOException            If some other I/O error occurs
     */
    @Override
    public long position() throws IOException {
        return position;
    }

    /**
     * Sets this channel's position.
     *
     * @param newPosition The new position, a non-negative integer counting
     *                    the number of bytes from the beginning of the entity
     * @return This channel
     * @throws ClosedChannelException   If this channel is closed
     * @throws IllegalArgumentException If the new position is negative
     * @throws IOException              If some other I/O error occurs
     */
    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        assertOpen();
        if (newPosition < 0) {
            throw new IllegalArgumentException("newPosition is negative");
        }
        if (newPosition > contentLength) {
            throw new EOFException(url.toString());
        }
        long delta = newPosition - position;
        if (delta == 0) {
            // If no delta, return immediately.
            return this;
        } else if (delta > 0 && delta < buffer.length) {
            // If the delta is positive and less than the internal buffer perform optimisation:
            // reuse existing connection and download bytes until the seek position is reached.
            skipBytes((int) delta);
            position += delta;
        } else {
            // ... otherwise establish new connection utilizing the "Range" request header parameter.
            close();
            position += delta;
            this.connection = connect();
        }
        return this;
    }

    /**
     * Tells whether or not this channel is open.
     *
     * @return <tt>true</tt> if, and only if, this channel is open
     */
    @Override
    public boolean isOpen() {
        return connection != null;
    }

    /**
     * Closes this channel.
     *
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        connection.disconnect();
        connection = null;
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     *
     * @param dst The buffer
     * @return the number of bytes actually read
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        assertOpen();
        int numRemaining = dst.remaining();
        if (dst.hasArray()) {
            byte[] bytes = dst.array();
            readBytes(bytes, dst.arrayOffset(), numRemaining);
            dst.position(dst.position() + numRemaining);
        } else {
            int length = numRemaining;
            while (length > 0) {
                int n = readBytes(buffer, 0, Math.min(buffer.length, length));
                dst.put(buffer, 0, n);
                length -= n;
            }
        }
        this.position += numRemaining;
        return numRemaining;
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * @param src The buffer
     * @return the number of bytes actually written
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        assertOpen();
        // TODO - implement me
        throw new NonWritableChannelException();
    }

    /**
     * Truncates the entity, to which this channel is connected, to the given
     * size.
     *
     * @param size The new size, a non-negative byte count
     * @return This channel
     * @throws NonWritableChannelException If this channel was not opened for writing
     * @throws ClosedChannelException      If this channel is closed
     * @throws IllegalArgumentException    If the new size is negative
     * @throws IOException                 If some other I/O error occurs
     */
    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException();
    }

    private void assertOpen() throws ClosedChannelException {
        if (connection == null) {
            throw new ClosedChannelException();
        }
    }

    private void skipBytes(int length) throws IOException {
        readBytes(buffer, 0, length);
    }

    private int readBytes(byte[] array, int offset, int length) throws IOException {
        if (position >= contentLength) {
            throw new EOFException(url.toString());
        }
        InputStream stream = connection.getInputStream();
        int off = offset;
        int len = length;
        while (len > 0) {
            int n = stream.read(array, off, len);
            if (n < 0) {
                throw new EOFException(url.toString());
            }
            len -= n;
            off += n;
        }
        return length;
    }

    private HttpURLConnection connect() throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setDoInput(true);
        if (position > 0) {
            String rangeSpec = "bytes=" + position + "-" + (contentLength - 1);
            connection.setRequestProperty("Range", rangeSpec);
        }
        connection.connect();
        int responseCode = connection.getResponseCode();
        if (responseCode < 200 || responseCode >= 300) {
            throw new IOException(url + ": response code " + responseCode + ": " + connection.getResponseMessage());
        }
        return connection;
    }
}
