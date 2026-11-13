package com.example.demo;

import com.example.demo.persistence.DatabaseInitializer;
import modell.PeerEndpoint;
import modell.StreamRequest;
import modell.StreamResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class ProtocolAndDiscoveryTest {

    @Autowired
    private NetworkService networkService;

    @Autowired
    private DatabaseInitializer databaseInitializer;

    @Value("${db.url:jdbc:sqlite:peer.db}")
    private String dbUrl;

    @Value("${peer.tcp.port:9001}")
    private int tcpPort;

    @Value("${peer.id:peer-1}")
    private String myPeerId;

    @Test
    public void testDatabasePragmasAndSchema() throws Exception {
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {

            // 1. Verify journal_mode is WAL (persisted in DB file)
            try (ResultSet rs = stmt.executeQuery("PRAGMA journal_mode;")) {
                assertTrue(rs.next());
                assertEquals("wal", rs.getString(1).toLowerCase());
            }

            // 2. Verify busy_timeout pragma executes properly
            stmt.execute("PRAGMA busy_timeout=5000;");
            try (ResultSet rs = stmt.executeQuery("PRAGMA busy_timeout;")) {
                assertTrue(rs.next());
                assertEquals(5000, rs.getInt(1));
            }

            // 3. Verify peers columns
            Set<String> peerCols = new HashSet<>();
            try (ResultSet rs = stmt.executeQuery("PRAGMA table_info(peers);")) {
                while (rs.next()) {
                    peerCols.add(rs.getString("name").toLowerCase());
                }
            }
            assertTrue(peerCols.contains("peer_id"));
            assertTrue(peerCols.contains("host"));
            assertTrue(peerCols.contains("ip"));
            assertTrue(peerCols.contains("port"));
            assertTrue(peerCols.contains("status"));
            assertTrue(peerCols.contains("rtt_ms"));
            assertTrue(peerCols.contains("share_ready"));
            assertTrue(peerCols.contains("data_ports"));
            assertTrue(peerCols.contains("last_seen"));

            // 4. Verify files table has file_hash
            Set<String> filesCols = new HashSet<>();
            try (ResultSet rs = stmt.executeQuery("PRAGMA table_info(files);")) {
                while (rs.next()) {
                    filesCols.add(rs.getString("name").toLowerCase());
                }
            }
            assertTrue(filesCols.contains("file_hash"));

            // 5. Verify downloads table exists
            Set<String> downloadCols = new HashSet<>();
            try (ResultSet rs = stmt.executeQuery("PRAGMA table_info(downloads);")) {
                while (rs.next()) {
                    downloadCols.add(rs.getString("name").toLowerCase());
                }
            }
            assertTrue(downloadCols.contains("file_id"));
            assertTrue(downloadCols.contains("file_name"));
            assertTrue(downloadCols.contains("total_size"));
            assertTrue(downloadCols.contains("bytes_downloaded"));
            assertTrue(downloadCols.contains("status"));
            assertTrue(downloadCols.contains("last_updated"));
        }
    }

    @Test
    public void testFramingUtilPipeOperations() throws Exception {
        // 1. Create a dummy file with 150,000 bytes
        Path sourcePath = Files.createTempFile("framing-src-", ".bin");
        Path destPath = Files.createTempFile("framing-dst-", ".bin");

        byte[] original = new byte[150000];
        new Random(42).nextBytes(original);
        Files.write(sourcePath, original);

        // Pre-allocate dest file with 200,000 zero bytes
        try (FileChannel destFc = FileChannel.open(destPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            destFc.truncate(200000);
        }

        // Pipe from source FileChannel to OutputStream
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        AtomicLong sendProgress = new AtomicLong();
        try (FileChannel srcFc = FileChannel.open(sourcePath, StandardOpenOption.READ)) {
            FramingUtil.pipeChannelToStream(srcFc, baos, 10000, 100000, sendProgress::addAndGet);
        }
        assertEquals(100000, sendProgress.get());
        assertEquals(100000, baos.size());

        // Pipe from ByteArrayInputStream to dest FileChannel at offset 50000
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        AtomicLong recvProgress = new AtomicLong();
        try (FileChannel destFc = FileChannel.open(destPath, StandardOpenOption.WRITE)) {
            FramingUtil.pipeRawBytes(bais, destFc, 50000, 100000, recvProgress::addAndGet);
        }
        assertEquals(100000, recvProgress.get());

        // Verify written bytes match original slice
        byte[] destBytes = Files.readAllBytes(destPath);
        byte[] expectedSlice = Arrays.copyOfRange(original, 10000, 110000);
        byte[] actualSlice = Arrays.copyOfRange(destBytes, 50000, 150000);
        assertArrayEquals(expectedSlice, actualSlice);

        // Verify EOFException on truncated stream
        ByteArrayInputStream shortStream = new ByteArrayInputStream(new byte[500]);
        try (FileChannel destFc = FileChannel.open(destPath, StandardOpenOption.WRITE)) {
            assertThrows(EOFException.class, () -> {
                FramingUtil.pipeRawBytes(shortStream, destFc, 0, 1000, null);
            });
        }

        Files.deleteIfExists(sourcePath);
        Files.deleteIfExists(destPath);
    }

    @Test
    public void testStreamRequestOverSocket() throws Exception {
        // Create a temporary file and register it in SQLite files table
        Path testFile = Files.createTempFile("stream-test-", ".dat");
        byte[] testBytes = "P2P Low-Latency Zero-Chunking Binary Stream Protocol Test Data!".getBytes();
        Files.write(testFile, testBytes);

        String testFileId = "stream-file-" + UUID.randomUUID();
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT OR REPLACE INTO files (file_id, name, size_bytes, owner_peer_id, original_path, file_hash) " +
                    "VALUES ('" + testFileId + "', 'stream.dat', " + testBytes.length + ", '" + myPeerId + "', '" +
                    testFile.toAbsolutePath().toString().replace("'", "''") + "', 'hash123')");
        }

        // Connect directly to local TCP server port
        try (Socket socket = new Socket("127.0.0.1", tcpPort)) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(5000);

            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());

            // 1. Send STREAM_REQ for range [4, 20)
            long offset = 4;
            long length = 16;
            String reqJson = String.format("{\"type\":\"STREAM_REQ\",\"fileId\":\"%s\",\"offset\":%d,\"length\":%d}",
                    testFileId, offset, length);
            FramingUtil.writeFrame(dos, reqJson);

            // Read STREAM_RES frame
            String resJson = FramingUtil.readFrame(dis);
            assertTrue(resJson.contains("\"status\":\"OK\""));
            assertTrue(resJson.contains("\"STREAM_RES\""));

            // Read raw binary bytes immediately following
            byte[] receivedBytes = new byte[(int) length];
            dis.readFully(receivedBytes);

            byte[] expected = Arrays.copyOfRange(testBytes, (int) offset, (int) (offset + length));
            assertArrayEquals(expected, receivedBytes);

            // 2. Test PERSISTENT SOCKET REUSE: Send another STREAM_REQ on the SAME connection!
            long offset2 = 0;
            long length2 = testBytes.length;
            String reqJson2 = String.format("{\"type\":\"STREAM_REQ\",\"fileId\":\"%s\",\"offset\":%d,\"length\":%d}",
                    testFileId, offset2, length2);
            FramingUtil.writeFrame(dos, reqJson2);

            String resJson2 = FramingUtil.readFrame(dis);
            assertTrue(resJson2.contains("\"status\":\"OK\""));

            byte[] receivedBytes2 = new byte[(int) length2];
            dis.readFully(receivedBytes2);
            assertArrayEquals(testBytes, receivedBytes2);
        } finally {
            Files.deleteIfExists(testFile);
        }
    }

    @Test
    public void testHelloHandshakeAndSelfFiltering() throws Exception {
        // Probe local server with our own peerId -> should return HELLO_ACK
        try (Socket socket = new Socket("127.0.0.1", tcpPort)) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(5000);

            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());

            String helloJson = "{\"type\":\"HELLO\",\"peerId\":\"remote-test-peer\",\"listenPort\":9999,\"shareReady\":true}";
            FramingUtil.writeFrame(dos, helloJson);

            String ackJson = FramingUtil.readFrame(dis);
            assertTrue(ackJson.contains("HELLO_ACK"));
            assertTrue(ackJson.contains(myPeerId));
        }

        // Verify that scanSubnetAndPorts ignores local self
        List<PeerEndpoint> peers = networkService.scanSubnetAndPorts();
        assertNotNull(peers);
        for (PeerEndpoint p : peers) {
            assertNotEquals(myPeerId, p.getPeerId(), "Self peerId must be filtered out during scanning!");
        }
    }
}
