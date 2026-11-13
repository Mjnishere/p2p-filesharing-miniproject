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

public class FramingUtil {

    private static final int BUFFER_SIZE = 65536; // 64 KB buffer

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
        byte[] buffer = new byte[length];
        dis.readFully(buffer);
        return new String(buffer, StandardCharsets.UTF_8);
    }

    /**
     * Strictly reads exactly `length` raw binary bytes from `in` and writes them directly
     * to `channel` at positional offset (fileOffset + totalBytesReadSoFar) without moving the
     * channel's cursor (OS positional pwrite).
     *
     * @param in Input stream from socket
     * @param channel Destination FileChannel
     * @param fileOffset Starting file offset
     * @param length Number of raw bytes to read
     * @param onProgress Callback receiving count of bytes read per chunk
     * @throws IOException If I/O error or premature stream termination occurs
     */
    public static void pipeRawBytes(InputStream in, FileChannel channel, long fileOffset, long length, LongConsumer onProgress) throws IOException {
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative: " + length);
        }
        if (length == 0) {
            return;
        }

        byte[] buffer = new byte[BUFFER_SIZE];
        long remaining = length;
        long totalBytesReadSoFar = 0;

        while (remaining > 0) {
            int toRead = (int) Math.min(buffer.length, remaining);
            int bytesRead = in.read(buffer, 0, toRead);
            if (bytesRead < 0) {
                throw new EOFException("Premature end of stream while reading raw bytes. Expected "
                        + remaining + " more bytes (read " + totalBytesReadSoFar + "/" + length + ")");
            }

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
    }

    /**
     * Reads exactly `length` bytes from `channel` at `fileOffset` using a 64KB buffer
     * and streams them directly into `out`, flushing at completion.
     *
     * @param channel Source FileChannel
     * @param out Destination OutputStream (socket)
     * @param fileOffset Starting offset in source file
     * @param length Number of bytes to transfer
     * @param onProgress Optional progress callback
     * @throws IOException If channel terminates early or socket error occurs
     */
    public static void pipeChannelToStream(FileChannel channel, OutputStream out, long fileOffset, long length, LongConsumer onProgress) throws IOException {
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative: " + length);
        }
        if (length == 0) {
            out.flush();
            return;
        }

        byte[] buffer = new byte[BUFFER_SIZE];
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
