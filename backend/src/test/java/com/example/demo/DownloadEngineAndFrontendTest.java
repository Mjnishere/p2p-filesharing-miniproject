package com.example.demo;

import com.example.demo.controller.FileSharingController;
import com.example.demo.service.ChunkManagerService;
import com.example.demo.service.DownloadManagerService;
import com.example.demo.service.SearchManagerService;
import modell.CacheStats;
import modell.PeerEndpoint;
import modell.SharedFile;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class DownloadEngineAndFrontendTest {

    @Autowired
    private FileSharingController fileSharingController;

    @Autowired
    private ChunkManagerService chunkManagerService;

    @Autowired
    private DownloadManagerService downloadManagerService;

    @Autowired
    private SearchManagerService searchManagerService;

    @Value("${peer.tcp.port:9001}")
    private int tcpPort;

    @Test
    public void testZeroChunkingParallelDownloadAndIntegrity() throws Exception {
        // 1. Create a 5 MB test file to trigger multi-window & multi-lane parallel transfer
        int testFileSize = 5 * 1024 * 1024; // 5 MB > 4 MB WINDOW_SIZE
        File sourceFile = new File("test-zero-chunk-5mb.bin");
        byte[] payload = new byte[testFileSize];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) ((i * 31) ^ (i >> 8));
        }
        try (FileOutputStream fos = new FileOutputStream(sourceFile)) {
            fos.write(payload);
        }

        try {
            // 2. Register file under zero-chunking architecture
            SharedFile shared = chunkManagerService.registerSharedFile(sourceFile, "peer-1");
            assertNotNull(shared);
            assertNotNull(shared.getFileHash(), "File hash must be computed");
            assertEquals(testFileSize, shared.getSizeBytes());

            // 3. Initiate Zero-Chunking Parallel Download via Controller
            PeerEndpoint seeder = new PeerEndpoint("seeder-node", "127.0.0.1", tcpPort, 2L, true);
            DownloadManagerService.DownloadJob job = fileSharingController.startDownload(
                    shared.getFileId(),
                    "downloaded-5mb-stream.bin",
                    shared.getSizeBytes(),
                    shared.getFileHash(),
                    2,
                    "127.0.0.1",
                    tcpPort,
                    null
            );

            assertNotNull(job);
            assertEquals(shared.getFileId(), job.getFileId());

            // 4. Await completion
            long startMs = System.currentTimeMillis();
            while (!"COMPLETED".equals(job.getStatus()) && !"FAILED".equals(job.getStatus()) && (System.currentTimeMillis() - startMs < 10000)) {
                Thread.sleep(50);
            }

            assertEquals("COMPLETED", job.getStatus(), "Zero-chunking download must complete successfully. Error: " + job.getErrorMessage());
            assertEquals(testFileSize, job.getBytesDownloaded());
            assertEquals(100.0, job.getProgressPercentage(), 0.01);

            // 5. Verify reconstructed output file matches original binary payload byte-for-byte
            Path reconstructedPath = Paths.get("peer-data", "reconstructed", "downloaded-5mb-stream.bin");
            assertTrue(Files.exists(reconstructedPath), "Reconstructed file must exist on disk");
            assertEquals(testFileSize, Files.size(reconstructedPath));
            byte[] reconstructedBytes = Files.readAllBytes(reconstructedPath);
            assertArrayEquals(payload, reconstructedBytes, "Reconstructed binary stream must match source bytes exactly");

            // Verify no temporary .part file was left behind
            Path partPath = Paths.get("peer-data", "downloads", "downloaded-5mb-stream.bin.part");
            assertFalse(Files.exists(partPath), "Temporary .part file must be atomically moved upon completion");

            // Clean up reconstructed file
            Files.deleteIfExists(reconstructedPath);

        } finally {
            if (sourceFile.exists()) sourceFile.delete();
        }
    }

    @Test
    public void testDiscoveryAndCacheTelemetryEndpoints() {
        // 1. Test POST /api/peers/discover endpoint
        Map<String, Object> discoveryResponse = fileSharingController.discoverPeers();
        assertNotNull(discoveryResponse);
        assertEquals("DISCOVERY_COMPLETED", discoveryResponse.get("status"));
        assertNotNull(discoveryResponse.get("peers"));
        assertTrue(discoveryResponse.containsKey("count"));

        // 2. Test GET /api/cache/stats endpoint
        CacheStats stats = fileSharingController.getCacheStats();
        assertNotNull(stats);
        assertTrue(stats.getQueryCacheHits() >= 0);
        assertTrue(stats.getQueryCacheMisses() >= 0);
        assertNotNull(stats.getSwarms());
    }

    @Test
    public void testDownloadStatusPollingExposesZeroChunkingTelemetry() {
        DownloadManagerService.DownloadJob job = new DownloadManagerService.DownloadJob(
                "job-telemetry-test",
                "sample.dat",
                8 * 1024 * 1024L,
                "dummyhash123",
                "127.0.0.1",
                9001
        );

        // Verify live zero-chunking metrics
        assertEquals(8 * 1024 * 1024L, job.getTotalBytes());
        assertEquals(0L, job.getBytesDownloaded());
        job.getBytesDownloadedCounter().addAndGet(4 * 1024 * 1024L);
        assertEquals(50.0, job.getProgressPercentage(), 0.01);
        job.getActiveLanesCounter().set(4);
        assertEquals(4, job.getActiveLanes());
    }
}
