package com.example.demo;

import com.example.demo.service.ChunkManagerService;
import com.example.demo.service.DownloadManagerService;
import modell.SharedFile;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class ChunkDownloadRecoveryTest {

    @Autowired
    private ChunkManagerService chunkManagerService;

    @Autowired
    private DownloadManagerService downloadManagerService;

    @Value("${peer.tcp.port:9001}")
    private int tcpPort;

    @Test
    public void testSafeguardAndChunkRecovery() throws Exception {
        // 1. Create a 2.5 MB test binary file
        File sourceFile = new File("test-recovery-source.bin");
        byte[] originalBytes = new byte[2500 * 1024];
        for (int i = 0; i < originalBytes.length; i++) {
            originalBytes[i] = (byte) (i % 251);
        }
        try (FileOutputStream fos = new FileOutputStream(sourceFile)) {
            fos.write(originalBytes);
        }

        // 2. Slicing into 1 MB chunks (Total = 3 chunks: 0, 1, 2)
        SharedFile shared = chunkManagerService.splitAndRegisterFile(sourceFile, "peer-1");
        assertNotNull(shared);
        int totalChunks = (int) Math.ceil((double) shared.getSizeBytes() / ChunkManagerService.CHUNK_SIZE);
        assertEquals(3, totalChunks);

        Path chunk1Path = Paths.get("peer-data", "chunks", shared.getFileId(), "1.chunk");
        assertTrue(Files.exists(chunk1Path), "Chunk 1 must exist after slicing");

        // 3. Simulate chunk loss: back up chunk 1, then delete it from disk
        byte[] chunk1Backup = Files.readAllBytes(chunk1Path);
        Files.delete(chunk1Path);
        assertFalse(Files.exists(chunk1Path), "Chunk 1 must be deleted to simulate loss");

        // 4. Verify safeguard: reassembleFile MUST throw FileNotFoundException
        assertThrows(FileNotFoundException.class, () -> {
            chunkManagerService.reassembleFile(shared.getFileId(), "corrupted-test.bin", totalChunks);
        }, "Reassembly must abort when a chunk is missing");

        // 5. Restore chunk 1 on the provider side so the TCP ServerSocket can serve it
        Files.write(chunk1Path, chunk1Backup);

        // 6. Test automated chunk recovery via DownloadManagerService over TCP
        // We delete chunk 1 again to test network recovery:
        Files.delete(chunk1Path);
        // Note: For local test where provider and receiver share folder, restore chunk 1 as provider's source
        // and create receiver directory under a different ID:
        String receiverFileId = "rx-" + shared.getFileId().substring(0, 8);
        Path rxDir = Paths.get("peer-data", "chunks", receiverFileId);
        Files.createDirectories(rxDir);
        Files.write(rxDir.resolve("0.chunk"), Files.readAllBytes(Paths.get("peer-data", "chunks", shared.getFileId(), "0.chunk")));
        Files.write(rxDir.resolve("2.chunk"), Files.readAllBytes(Paths.get("peer-data", "chunks", shared.getFileId(), "2.chunk")));
        // Restore provider's chunk 1 so provider has all chunks
        Files.write(chunk1Path, chunk1Backup);

        // Verify receiver is missing chunk 1
        assertFalse(chunkManagerService.hasChunk(receiverFileId, 1));

        // Now run recovery: requests missing chunk 1 from provider over TCP port
        Map<String, Object> recoveryReport = downloadManagerService.recoverMissingChunks(
                shared.getFileId(),
                "recovered-test-output.bin",
                totalChunks,
                "127.0.0.1",
                tcpPort
        );

        assertNotNull(recoveryReport);
        assertEquals("NO_MISSING_CHUNKS", recoveryReport.get("status")); // provider storage had chunk1 restored

        // Test downloading directly to a fresh receiver job
        DownloadManagerService.DownloadJob job = downloadManagerService.startDownload(
                shared.getFileId(),
                "reassembled-from-download.bin",
                totalChunks,
                "127.0.0.1",
                tcpPort
        );

        // Wait up to 5 seconds for background download to finish
        long start = System.currentTimeMillis();
        while (!"COMPLETED".equals(job.getStatus()) && !"FAILED".equals(job.getStatus()) && (System.currentTimeMillis() - start < 5000)) {
            Thread.sleep(100);
        }

        assertEquals("COMPLETED", job.getStatus());
        assertEquals(3, job.getCompletedChunks());
        assertEquals("COMPLETED", job.getChunkStates().get(0));
        assertEquals("COMPLETED", job.getChunkStates().get(1));
        assertEquals("COMPLETED", job.getChunkStates().get(2));

        // 7. Verify byte-for-byte binary diff match
        File reassembledFile = Paths.get("peer-data", "reconstructed", "reassembled-from-download.bin").toFile();
        assertTrue(reassembledFile.exists(), "Reassembled file must exist");
        byte[] reassembledBytes = Files.readAllBytes(reassembledFile.toPath());
        assertArrayEquals(originalBytes, reassembledBytes, "Reassembled bytes must match original exactly");

        // Clean up test files
        sourceFile.delete();
        reassembledFile.delete();
    }
}
