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
    private ServerSocket seederServerC;
    private int portA;
    private int portB;
    private int portC;
    private Path originalSourceFile;
    private String expectedHash;
    private long testFileSize = 16 * 1024 * 1024L; // 16 MB test file (four 4 MB virtual windows)

    @BeforeEach
    void setUp() throws Exception {
        databaseInitializer.init();

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
        seederServerC = new ServerSocket(0);
        portA = seederServerA.getLocalPort();
        portB = seederServerB.getLocalPort();
        portC = seederServerC.getLocalPort();
    }

    @AfterEach
    void tearDown() {
        try {
            if (seederServerA != null && !seederServerA.isClosed()) seederServerA.close();
        } catch (Exception ignored) {}
        try {
            if (seederServerB != null && !seederServerB.isClosed()) seederServerB.close();
        } catch (Exception ignored) {}
        try {
            if (seederServerC != null && !seederServerC.isClosed()) seederServerC.close();
        } catch (Exception ignored) {}
        executor.shutdownNow();
    }

    @Test
    void testMidTransferFailoverSeederTermination() throws Exception {
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
                                }

                                seederAFailed.set(true);
                                socket.close();
                                seederServerA.close();
                                break;
                            }
                        } catch (Exception ignored) {}
                    });
                }
            } catch (Exception ignored) {}
        });

        // Run Seeder B (Surviving Seeder)
        startHealthySeederServer(seederServerB, seederBRequestsServed);

        String fileId = UUID.randomUUID().toString();
        String downloadFileName = "failover-verified-output.bin";

        PeerEndpoint peerA = new PeerEndpoint("seeder-A", "127.0.0.1", portA, 10L, true);
        PeerEndpoint peerB = new PeerEndpoint("seeder-B", "127.0.0.1", portB, 15L, true);
        List<PeerEndpoint> swarm = List.of(peerA, peerB);

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

        long deadline = System.currentTimeMillis() + 15000;
        while (!"COMPLETED".equals(job.getStatus()) && !"FAILED".equals(job.getStatus()) && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        assertEquals("COMPLETED", job.getStatus(), "Job should complete via failover. Error: " + job.getErrorMessage());
        assertTrue(seederAFailed.get(), "Seeder A must have failed mid-transfer to trigger failover.");
        assertTrue(seederBRequestsServed.get() > 0, "Seeder B must have served stolen requests.");

        File reassembled = Paths.get("peer-data", "reconstructed", downloadFileName).toFile();
        assertTrue(reassembled.exists(), "Reassembled file must exist on disk.");
        assertEquals(testFileSize, reassembled.length(), "Reassembled file size must match original.");

        String finalHash = chunkManagerService.calculateFileHash(reassembled);
        assertEquals(expectedHash, finalHash, "Final SHA-256 hash must match original source file exactly!");
        reassembled.delete();
    }

    @Test
    void testParallelMultiPeerSwarmStreamingViaSwarmDirectoryCache() throws Exception {
        AtomicInteger countA = new AtomicInteger(0);
        AtomicInteger countB = new AtomicInteger(0);
        AtomicInteger countC = new AtomicInteger(0);

        startHealthySeederServer(seederServerA, countA);
        startHealthySeederServer(seederServerB, countB);
        startHealthySeederServer(seederServerC, countC);

        String fileId = "swarm-multi-peer-" + UUID.randomUUID();
        String downloadFileName = "multi-peer-parallel-output.bin";

        // Register all 3 peers in SwarmDirectoryCache (simulating cached distributed search results)
        PeerEndpoint epA = new PeerEndpoint("peer-swarm-A", "127.0.0.1", portA, 8L, true);
        PeerEndpoint epB = new PeerEndpoint("peer-swarm-B", "127.0.0.1", portB, 11L, true);
        PeerEndpoint epC = new PeerEndpoint("peer-swarm-C", "127.0.0.1", portC, 14L, true);

        searchManagerService.registerSwarmPeer(expectedHash, fileId, downloadFileName, epA);
        searchManagerService.registerSwarmPeer(expectedHash, fileId, downloadFileName, epB);
        searchManagerService.registerSwarmPeer(expectedHash, fileId, downloadFileName, epC);

        // Start download passing null initialPeers so DownloadManagerService pulls the 3 peers from SwarmDirectoryCache!
        DownloadManagerService.DownloadJob job = downloadManagerService.startDownload(
                fileId,
                downloadFileName,
                testFileSize,
                expectedHash,
                0,
                "127.0.0.1",
                portA,
                null
        );

        long deadline = System.currentTimeMillis() + 15000;
        while (!"COMPLETED".equals(job.getStatus()) && !"FAILED".equals(job.getStatus()) && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        assertEquals("COMPLETED", job.getStatus(), "Multi-peer parallel stream must complete. Error: " + job.getErrorMessage());
        assertEquals(3, job.getSwarmPeersCount(), "All 3 cached swarm peers must be registered on the download job");
        assertTrue(countA.get() > 0 && countB.get() > 0 && countC.get() > 0,
                "All 3 seeders must concurrently stream windows to the single receiver (A=" + countA.get() + ", B=" + countB.get() + ", C=" + countC.get() + ")");

        File reassembled = Paths.get("peer-data", "reconstructed", downloadFileName).toFile();
        assertTrue(reassembled.exists());
        assertEquals(expectedHash, chunkManagerService.calculateFileHash(reassembled));
        reassembled.delete();
    }

    private void startHealthySeederServer(ServerSocket server, AtomicInteger requestCounter) {
        executor.submit(() -> {
            try {
                while (!server.isClosed()) {
                    Socket socket = server.accept();
                    executor.submit(() -> {
                        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                             DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

                            while (!socket.isClosed()) {
                                String rawReq = FramingUtil.readFrame(dis);
                                StreamRequest req = objectMapper.readValue(rawReq, StreamRequest.class);
                                requestCounter.incrementAndGet();

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
    }
}
