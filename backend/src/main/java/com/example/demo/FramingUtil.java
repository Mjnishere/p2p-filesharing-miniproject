package com.example.demo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FramingUtil {

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
}