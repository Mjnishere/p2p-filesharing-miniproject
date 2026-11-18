package com.example.demo;

import com.example.demo.service.ChunkManagerService;
import com.example.demo.service.ErasureCodingRecoveryEngine;
import com.example.demo.service.SearchManagerService;
import modell.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class CacheAndStorageTest {

    @Autowired
    private ChunkManagerService chunkManagerService;

    @Autowired
    private SearchManagerService searchManagerService;

    @Autowired
    private ErasureCodingRecoveryEngine erasureCodingRecoveryEngine;

    private File tempFile;

    @BeforeEach
    public void setup() throws IOException {
        tempFile = new File("test-zero-chunk-source.txt");
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write("Hello Zero-Chunking P2P Distributed Storage!".getBytes(StandardCharsets.UTF_8));
        }
    }

    @AfterEach
    public void tearDown() {
        if (tempFile != null && tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Test
    public void testFileHashCalculationAndZeroChunkingRegistration() throws Exception {
        // 1. Calculate file hash
        String hash = chunkManagerService.calculateFileHash(tempFile);
        assertNotNull(hash);
        assertEquals(64, hash.length()); // SHA-256 hex is 64 characters

        long savedBefore = chunkManagerService.getSqliteQueriesSaved();

        // 2. Register shared file using zero-chunking architecture
        SharedFile shared = chunkManagerService.registerSharedFile(tempFile, "peer-test-1");
        assertNotNull(shared);
        assertEquals(hash, shared.getFileHash());
        assertEquals(tempFile.length(), shared.getSizeBytes());

        // Verify NO .chunk files were created on disk for this file!
        Path chunkDir = Paths.get("peer-data", "chunks", shared.getFileId());
        assertFalse(Files.exists(chunkDir), "Zero-chunking architecture must NOT create .chunk files on disk");

        // 3. Test getFileChannel retrieval (served from O(1) in-memory catalog)
        try (FileChannel channel = chunkManagerService.getFileChannel(shared.getFileId())) {
            assertNotNull(channel);
            assertEquals(tempFile.length(), channel.size());
            ByteBuffer buf = ByteBuffer.allocate((int) channel.size());
            channel.read(buf);
            String content = new String(buf.array(), StandardCharsets.UTF_8);
            assertEquals("Hello Zero-Chunking P2P Distributed Storage!", content);
        }

        // 4. Test getFileChannel by fileHash fallback (served from O(1) in-memory catalog)
        try (FileChannel channelByHash = chunkManagerService.getFileChannel(hash)) {
            assertNotNull(channelByHash);
            assertEquals(tempFile.length(), channelByHash.size());
        }

        // 5. Test getFileChannel by fileName fallback (served from O(1) in-memory catalog)
        try (FileChannel channelByName = chunkManagerService.getFileChannel(tempFile.getName())) {
            assertNotNull(channelByName);
            assertEquals(tempFile.length(), channelByName.size());
        }

        assertTrue(chunkManagerService.getSqliteQueriesSaved() >= savedBefore + 3,
                "In-memory catalog must bypass SQLite queries on hot path");
    }

    @Test
    public void testSwarmDirectoryCacheAndOrdering() {
        String testHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        PeerEndpoint slowPeer = new PeerEndpoint("peer-slow", "192.168.1.50", 9000, 150, true);
        PeerEndpoint fastPeer = new PeerEndpoint("peer-fast", "192.168.1.20", 9000, 15, true);
        PeerEndpoint midPeer = new PeerEndpoint("peer-mid", "192.168.1.30", 9000, 45, true);

        searchManagerService.registerSwarmPeer(testHash, slowPeer);
        searchManagerService.registerSwarmPeer(testHash, fastPeer);
        searchManagerService.registerSwarmPeer(testHash, midPeer);

        Set<PeerEndpoint> swarm = searchManagerService.getSwarmPeers(testHash);
        assertEquals(3, swarm.size());

        // Verify sorted order: lowest rttMs first
        PeerEndpoint[] ordered = swarm.toArray(new PeerEndpoint[0]);
        assertEquals("peer-fast", ordered[0].getPeerId());
        assertEquals(15, ordered[0].getRttMs());
        assertEquals("peer-mid", ordered[1].getPeerId());
        assertEquals(45, ordered[1].getRttMs());
        assertEquals("peer-slow", ordered[2].getPeerId());
        assertEquals(150, ordered[2].getRttMs());

        // Test peer eviction from swarm
        searchManagerService.evictPeerFromSwarm("peer-mid");
        Set<PeerEndpoint> afterEvict = searchManagerService.getSwarmPeers(testHash);
        assertEquals(2, afterEvict.size());
        assertFalse(afterEvict.stream().anyMatch(p -> "peer-mid".equals(p.getPeerId())));
    }

    @Test
    public void testTwoTierQueryCacheAndPrefixFiltering() throws Exception {
        File ub24File = new File("ubuntu-24.04-desktop.iso");
        File ub22File = new File("ubuntu-22.04-server.iso");
        try (FileOutputStream fos1 = new FileOutputStream(ub24File);
             FileOutputStream fos2 = new FileOutputStream(ub22File)) {
            fos1.write("Ubuntu 24.04 Desktop ISO Content".getBytes(StandardCharsets.UTF_8));
            fos2.write("Ubuntu 22.04 Server ISO Content".getBytes(StandardCharsets.UTF_8));
        }

        try {
            SharedFile sf24 = chunkManagerService.registerSharedFile(ub24File, "peer-1");
            SharedFile sf22 = chunkManagerService.registerSharedFile(ub22File, "peer-1");

            // Initial search for "ubuntu" creates cache entry using in-memory trigram index (0 SQLite queries)
            List<SearchResult> results = searchManagerService.executeDistributedSearch("ubuntu", 0);
            assertNotNull(results);
            assertTrue(results.size() >= 2);

            long hitsBefore = searchManagerService.getCacheStats().getQueryCacheHits();

            // 1. Exact Cache Hit Test for "ubuntu"
            long startNs = System.nanoTime();
            List<SearchResult> exactHit = searchManagerService.checkQueryCache("ubuntu");
            long durationNs = System.nanoTime() - startNs;

            assertNotNull(exactHit);
            assertTrue(durationNs < 5_000_000L, "Cache hit must return in < 5 ms");
            assertEquals(hitsBefore + 1, searchManagerService.getCacheStats().getQueryCacheHits());

            // 2. Prefix Filtering Test: Search for "ubuntu 24" should hit prefix filter in memory
            List<SearchResult> prefixHit = searchManagerService.checkQueryCache("ubuntu 24");
            assertNotNull(prefixHit);
            assertFalse(prefixHit.isEmpty());
            assertTrue(prefixHit.stream().allMatch(sr -> sr.getFileName().toLowerCase().contains("24")));

            // 3. Verify CacheStats including in-memory index & SQLite bypass metrics
            CacheStats stats = searchManagerService.getCacheStats();
            assertNotNull(stats);
            assertTrue(stats.getQueryCacheHits() >= 2);
            assertTrue(stats.getHitRate() > 0.0);
            assertTrue(stats.getCachedQueryCount() >= 1);
            assertTrue(stats.getInMemoryIndexedFiles() >= 2);
            assertTrue(stats.getInvertedIndexTokens() > 0);
            assertTrue(stats.getSqliteQueriesSaved() > 0);
        } finally {
            if (ub24File.exists()) ub24File.delete();
            if (ub22File.exists()) ub22File.delete();
        }
    }

    @Test
    public void testReedSolomonGf256ErasureCodingAndSubBlockMerkleVerification() throws Exception {
        // 1. Verify GF(2^8) Reed-Solomon multi-erasure recovery (4 data blocks + 2 parity blocks)
        int k = 4;
        int m = 2;
        int blockSize = 32768;
        byte[][] original = new byte[k][blockSize];
        byte[][] damaged = new byte[k][blockSize];
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < blockSize; j++) {
                byte v = (byte) ((i * 73 + j * 19 + 5) & 0xFF);
                original[i][j] = v;
                damaged[i][j] = v;
            }
        }

        byte[][] parity = erasureCodingRecoveryEngine.encodeReedSolomonParity(original, m);
        // Erase two data blocks simultaneously (blocks 0 and 2)
        damaged[0] = null;
        damaged[2] = null;

        erasureCodingRecoveryEngine.reconstructReedSolomonErasures(damaged, parity, blockSize);
        assertArrayEquals(original[0], damaged[0], "Shard 0 must be mathematically reconstructed via GF(2^8) Reed-Solomon");
        assertArrayEquals(original[2], damaged[2], "Shard 2 must be mathematically reconstructed via GF(2^8) Reed-Solomon");

        // 2. Verify 64 KB Sub-Block CRC32C Merkle corruption pinpoint detection
        Path tmp = Files.createTempFile("merkle-crc-", ".bin");
        byte[] fileData = new byte[ErasureCodingRecoveryEngine.SUB_BLOCK_SIZE * 3]; // 192 KB (3 sub-blocks)
        for (int i = 0; i < fileData.length; i++) {
            fileData[i] = (byte) (i & 0xFF);
        }
        Files.write(tmp, fileData);

        try (FileChannel fc = FileChannel.open(tmp, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ErasureCodingRecoveryEngine.WindowIntegrityMetadata meta =
                    erasureCodingRecoveryEngine.computeWindowMetadata(fc, 0, fileData.length);
            assertEquals(3, meta.getSubBlockCrcs().size());

            // Corrupt 1 byte inside sub-block #1 (at offset 70,000)
            ByteBuffer corruptByte = ByteBuffer.wrap(new byte[]{(byte) 0xFF});
            fc.write(corruptByte, 70000);

            List<Integer> corruptedSubBlocks = erasureCodingRecoveryEngine.findCorruptedSubBlocks(
                    fc, 0, fileData.length, meta.getSubBlockCrcs());
            assertEquals(List.of(1), corruptedSubBlocks, "Sub-block Merkle CRC32C must pinpoint exact corrupted sub-block #1");
        } finally {
            Files.deleteIfExists(tmp);
        }
    }
}
