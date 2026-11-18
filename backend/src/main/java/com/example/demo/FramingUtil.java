package com.example.demo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.function.LongConsumer;
import java.util.zip.CRC32C;

public class FramingUtil {

    public static final int BUFFER_SIZE = 65536; // 64 KB buffer

    // Thread-local zero-allocation buffer pools to eliminate per-window GC pressure
    private static final ThreadLocal<byte[]> HEAP_BUFFER_POOL =
            ThreadLocal.withInitial(() -> new byte[BUFFER_SIZE]);

    // Send: 4-byte length prefix followed by raw UTF-8 payload bytes
    public static void writeFrame(DataOutputStream dos, String message) throws IOException {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
        dos.flush();
    }

    // Read: 4-byte length prefix followed by reading exactly that many bytes
    public static String readFrame(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        if (length < 0 || length > 32 * 1024 * 1024) {
            throw new IOException("Invalid frame length: " + length);
        }
        byte[] buffer = new byte[length];
        dis.readFully(buffer);
        return new String(buffer, StandardCharsets.UTF_8);
    }

    /**
     * Strictly reads exactly `length` raw binary bytes from `in` using a pooled 64 KB buffer
     * and writes them directly to `channel` at positional offset (fileOffset + totalBytesReadSoFar)
     * without moving the channel's cursor (OS positional pwrite).
     */
    public static void pipeRawBytes(InputStream in, FileChannel channel, long fileOffset, long length, LongConsumer onProgress) throws IOException {
        pipeRawBytesWithCrc(in, channel, fileOffset, length, onProgress);
    }

    /**
     * Strictly reads `length` raw binary bytes from `in`, writes them positionally via `pwrite` into `channel`,
     * and computes the hardware-accelerated CRC32C checksum in a single zero-copy pass.
     *
     * @return Computed CRC32C value for the transferred byte range
     */
    public static long pipeRawBytesWithCrc(InputStream in, FileChannel channel, long fileOffset, long length, LongConsumer onProgress) throws IOException {
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative: " + length);
        }
        if (length == 0) {
            return 0L;
        }

        byte[] buffer = HEAP_BUFFER_POOL.get();
        CRC32C crc32c = new CRC32C();
        long remaining = length;
        long totalBytesReadSoFar = 0;

        while (remaining > 0) {
            int toRead = (int) Math.min(buffer.length, remaining);
            int bytesRead = in.read(buffer, 0, toRead);
            if (bytesRead < 0) {
                throw new EOFException("Premature end of stream while reading raw bytes. Expected "
                        + remaining + " more bytes (read " + totalBytesReadSoFar + "/" + length + ")");
            }

            crc32c.update(buffer, 0, bytesRead);

            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesRead);
            long writePos = fileOffset + totalBytesReadSoFar;
            while (byteBuffer.hasRemaining()) {
                int written = channel.write(byteBuffer, writePos);
                writePos += written;
            }

            totalBytesReadSoFar += bytesRead;
            remaining -= bytesRead;

            if (onProgress != null) {
                onProgress.accept(bytesRead);
            }
        }

        return crc32c.getValue();
    }

    /**
     * Reads `length` bytes from `channel` at `fileOffset` using a pooled 64 KB buffer
     * and streams them directly into `out`, flushing at completion.
     */
    public static void pipeChannelToStream(FileChannel channel, OutputStream out, long fileOffset, long length, LongConsumer onProgress) throws IOException {
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative: " + length);
        }
        if (length == 0) {
            out.flush();
            return;
        }

        byte[] buffer = HEAP_BUFFER_POOL.get();
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        long remaining = length;
        long currentOffset = fileOffset;

        while (remaining > 0) {
            int toRead = (int) Math.min(buffer.length, remaining);
            byteBuffer.clear();
            byteBuffer.limit(toRead);

            int bytesRead = channel.read(byteBuffer, currentOffset);
            if (bytesRead < 0) {
                throw new EOFException("Premature EOF on FileChannel: expected " + remaining
                        + " more bytes at offset " + currentOffset);
            }
            if (bytesRead == 0) {
                throw new EOFException("Premature EOF on FileChannel: read 0 bytes at offset " + currentOffset);
            }

            out.write(buffer, 0, bytesRead);
            currentOffset += bytesRead;
            remaining -= bytesRead;

            if (onProgress != null) {
                onProgress.accept(bytesRead);
            }
        }

        out.flush();
    }
}
