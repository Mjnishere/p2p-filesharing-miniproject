package com.example.demo;

import com.example.demo.persistence.DatabaseInitializer;
import com.example.demo.service.ChunkManagerService;
import com.example.demo.service.DownloadManagerService;
import com.example.demo.service.SearchManagerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import modell.PeerEndpoint;
import modell.StreamRequest;
import modell.StreamResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class MidTransferFailoverVerificationTest {

    @Autowired
    private DownloadManagerService downloadManagerService;

    @Autowired
    private ChunkManagerService chunkManagerService;

    @Autowired
    private SearchManagerService searchManagerService;

    @Autowired
    private DatabaseInitializer databaseInitializer;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private ServerSocket seederServerA;
    private ServerSocket seederServerB;
    private int portA;
    private int portB;
    private Path originalSourceFile;
    private String expectedHash;
    private long testFileSize = 16 * 1024 * 1024L; // 16 MB test file (four 4 MB virtual windows)

    @BeforeEach
    void setUp() throws Exception {
        databaseInitializer.init();

        // Generate 8 MB test file with pseudo-random content
        Path testDir = Paths.get("peer-data", "test-failover").toAbsolutePath();
        Files.createDirectories(testDir);
        originalSourceFile = testDir.resolve("failover-source-8mb.bin");

        byte[] block = new byte[65536];
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (OutputStream fos = new BufferedOutputStream(Files.newOutputStream(originalSourceFile))) {
            for (int i = 0; i < (testFileSize / block.length); i++) {
                for (int b = 0; b < block.length; b++) {
                    block[b] = (byte) ((i * 31 + b) & 0xFF);
                }
                md.update(block);
                fos.write(block);
            }
        }
        expectedHash = HexFormat.of().formatHex(md.digest());

        seederServerA = new ServerSocket(0);
        seederServerB = new ServerSocket(0);
        portA = seederServerA.getLocalPort();
        portB = seederServerB.getLocalPort();
    }

    @AfterEach
    void tearDown() {
        try {
            if (seederServerA != null && !seederServerA.isClosed()) seederServerA.close();
        } catch (Exception ignored) {}
        try {
            if (seederServerB != null && !seederServerB.isClosed()) seederServerB.close();
        } catch (Exception ignored) {}
        executor.shutdownNow();
    }

    @Test
    void testMidTransferFailoverSeederTermination() throws Exception {
        // Seeder A drops/closes socket abruptly after transferring 1 MB to simulate sudden failure at 50%
        AtomicBoolean seederAFailed = new AtomicBoolean(false);
        AtomicInteger seederBRequestsServed = new AtomicInteger(0);

        // Run Seeder A (The Failing Seeder)
        executor.submit(() -> {
            try {
                while (!seederServerA.isClosed()) {
                    Socket socket = seederServerA.accept();
                    executor.submit(() -> {
                        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                             DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

                            while (!socket.isClosed()) {
                                String rawReq = FramingUtil.readFrame(dis);
                                StreamRequest req = objectMapper.readValue(rawReq, StreamRequest.class);

                                // Send STREAM_RES OK
                                StreamResponse res = StreamResponse.ok(req.getFileId(), req.getOffset(), req.getLength());
                                FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(res));

                                // Stream partial bytes (1 MB), then abruptly terminate socket/server!
                                int partialBytesToSend = 1024 * 1024;
                                try (FileChannel srcChannel = FileChannel.open(originalSourceFile, StandardOpenOption.READ)) {
                                    ByteBuffer buf = ByteBuffer.allocateDirect(65536);
                                    long curPos = req.getOffset();
                                    long sent = 0;
                                    while (sent < partialBytesToSend) {
                                        buf.clear();
                                        int read = srcChannel.read(buf, curPos);
                                        if (read <= 0) break;
                                        buf.flip();
                                        byte[] bytes = new byte[read];
                                        buf.get(bytes);
                                        dos.write(bytes);
                                        sent += read;
                                        curPos += read;
                                    }
                                    dos.flush();
                                    System.out.println(">>> Seeder A flushed partial bytes, now terminating seeder A!");
                                }

                                // Terminate seeder A mid-transfer
                                seederAFailed.set(true);
                                socket.close();
                                seederServerA.close();
                                break;
                            }
                        } catch (Exception ex) {
                            System.err.println("Seeder A connection exception: " + ex);
                        }
                    });
                }
            } catch (Exception ex) {
                System.err.println("Seeder A accept loop exception: " + ex);
            }
        });

        // Run Seeder B (Surviving Seeder)
        executor.submit(() -> {
            try {
                while (!seederServerB.isClosed()) {
                    Socket socket = seederServerB.accept();
                    executor.submit(() -> {
                        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                             DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

                            while (!socket.isClosed()) {
                                String rawReq = FramingUtil.readFrame(dis);
                                StreamRequest req = objectMapper.readValue(rawReq, StreamRequest.class);
                                seederBRequestsServed.incrementAndGet();

                                StreamResponse res = StreamResponse.ok(req.getFileId(), req.getOffset(), req.getLength());
                                FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(res));

                                try (FileChannel srcChannel = FileChannel.open(originalSourceFile, StandardOpenOption.READ)) {
                                    FramingUtil.pipeChannelToStream(srcChannel, dos, req.getOffset(), req.getLength(), null);
                                }
                            }
                        } catch (Exception ignored) {}
                    });
                }
            } catch (Exception ignored) {}
        });

        String fileId = UUID.randomUUID().toString();
        String downloadFileName = "failover-verified-output.bin";

        // Register both peers in swarm: Seeder A (rtt 10ms) and Seeder B (rtt 15ms)
        PeerEndpoint peerA = new PeerEndpoint("seeder-A", "127.0.0.1", portA, 10L, true);
        PeerEndpoint peerB = new PeerEndpoint("seeder-B", "127.0.0.1", portB, 15L, true);
        List<PeerEndpoint> swarm = List.of(peerA, peerB);

        // Initiate parallel work-stealing download
        DownloadManagerService.DownloadJob job = downloadManagerService.startDownload(
                fileId,
                downloadFileName,
                testFileSize,
                expectedHash,
                0,
                "127.0.0.1",
                portA,
                swarm
        );

        // Wait for download completion (up to 15 seconds)
        long deadline = System.currentTimeMillis() + 15000;
        while (!"COMPLETED".equals(job.getStatus()) && !"FAILED".equals(job.getStatus()) && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }

        assertEquals("COMPLETED", job.getStatus(), "Job should complete via failover. Error: " + job.getErrorMessage());
        assertTrue(seederAFailed.get(), "Seeder A must have failed mid-transfer to trigger failover.");
        assertTrue(seederBRequestsServed.get() > 0, "Seeder B must have served stolen requests.");

        // Verify reconstructed file exists and passes whole-file SHA-256 check
        File reassembled = Paths.get("peer-data", "reconstructed", downloadFileName).toFile();
        assertTrue(reassembled.exists(), "Reassembled file must exist on disk.");
        assertEquals(testFileSize, reassembled.length(), "Reassembled file size must match original.");

        // Calculate hash of final file
        String finalHash;
        try (InputStream in = Files.newInputStream(reassembled.toPath())) {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[65536];
            int r;
            while ((r = in.read(buf)) != -1) {
                sha256.update(buf, 0, r);
            }
            finalHash = HexFormat.of().formatHex(sha256.digest());
        }

        assertEquals(expectedHash, finalHash, "Final SHA-256 hash must match original source file exactly!");
        System.out.println(">>> Failover verification test passed! Whole-file SHA-256 checksum matched: " + finalHash);
    }
}
