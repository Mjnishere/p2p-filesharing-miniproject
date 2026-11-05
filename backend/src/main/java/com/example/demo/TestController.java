package com.example.demo;

import com.example.demo.service.ChunkManagerService;
import com.example.demo.service.DownloadManagerService;
import com.example.demo.service.SearchManagerService;
import modell.NetworkMessage;
import modell.SearchResult;
import modell.SharedFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@CrossOrigin(origins = "*")
public class TestController {

    @Autowired
    private ChunkManagerService chunkManagerService;

    @Autowired
    private NetworkService networkService;

    @Autowired
    private SearchManagerService searchManagerService;

    @Autowired
    private DownloadManagerService downloadManagerService;

    @Value("${peer.tcp.port:9001}")
    private int tcpPort;

    @GetMapping("/api/test/chunking")
    public Map<String, Object> testChunking() throws IOException, SQLException {
        File testFile = new File("test-sample.bin");
        byte[] dummyData = new byte[2500 * 1024];
        Arrays.fill(dummyData, (byte) 7);
        try (FileOutputStream fos = new FileOutputStream(testFile)) {
            fos.write(dummyData);
        }

        SharedFile shared = chunkManagerService.splitAndRegisterFile(testFile, "peer-1");
        int totalChunks = (int) Math.ceil((double) shared.getSizeBytes() / ChunkManagerService.CHUNK_SIZE);
        File reassembled = chunkManagerService.reassembleFile(shared.getFileId(), "reassembled-sample.bin", totalChunks);

        byte[] originalBytes = Files.readAllBytes(testFile.toPath());
        byte[] reassembledBytes = Files.readAllBytes(reassembled.toPath());
        boolean byteMatch = Arrays.equals(originalBytes, reassembledBytes);

        return Map.of(
                "fileId", shared.getFileId(),
                "fileName", shared.getName(),
                "originalSizeBytes", shared.getSizeBytes(),
                "totalChunksCreated", totalChunks,
                "byteMatchVerified", byteMatch
        );
    }

    @GetMapping("/api/test/tcp-hello")
    public Map<String, Object> testTcpHello() throws IOException {
        NetworkMessage ping = new NetworkMessage("HELLO", "test-client", "Ping handshake");
        String rawResponse = networkService.sendDirectMessage("127.0.0.1", tcpPort, ping);

        return Map.of(
                "sentMessageType", "HELLO",
                "targetPort", tcpPort,
                "rawTcpResponseReceived", rawResponse
        );
    }

    @GetMapping("/api/test/search")
    public Map<String, Object> testSearch(@RequestParam(defaultValue = "sample") String q) {
        List<SearchResult> results = searchManagerService.executeDistributedSearch(q, 3);
        return Map.of(
                "query", q,
                "resultsFound", results.size(),
                "results", results
        );
    }

    @GetMapping("/api/test/chunk-recovery")
    public Map<String, Object> testChunkRecovery() throws IOException, SQLException {
        Map<String, Object> report = new LinkedHashMap<>();

        // 1. Create a 2.5 MB binary test file (3 chunks)
        File testFile = new File("recovery-sample.bin");
        byte[] originalData = new byte[2500 * 1024];
        for (int i = 0; i < originalData.length; i++) {
            originalData[i] = (byte) (i % 127);
        }
        try (FileOutputStream fos = new FileOutputStream(testFile)) {
            fos.write(originalData);
        }

        SharedFile shared = chunkManagerService.splitAndRegisterFile(testFile, "peer-1");
        int totalChunks = (int) Math.ceil((double) shared.getSizeBytes() / ChunkManagerService.CHUNK_SIZE);
        report.put("fileId", shared.getFileId());
        report.put("totalChunks", totalChunks);

        // 2. Simulate missed chunk: delete chunk 1 from disk
        Path chunk1Path = Paths.get("peer-data", "chunks", shared.getFileId(), "1.chunk");
        byte[] backupChunk1Bytes = Files.readAllBytes(chunk1Path);
        Files.delete(chunk1Path);
        report.put("simulatedMissingChunk", 1);
        report.put("chunk1DeletedFromDisk", !Files.exists(chunk1Path));

        // 3. Verify safeguard: reassembleFile MUST fail with FileNotFoundException
        boolean caughtExpectedSafeguard = false;
        String safeguardMessage = "";
        try {
            chunkManagerService.reassembleFile(shared.getFileId(), "corrupted-attempt.bin", totalChunks);
        } catch (FileNotFoundException e) {
            caughtExpectedSafeguard = true;
            safeguardMessage = e.getMessage();
        }
        report.put("safeguardBlockedCorruptedReassembly", caughtExpectedSafeguard);
        report.put("safeguardException", safeguardMessage);

        // 4. Place chunk 1 in safeguard backup storage so recovery engine can verify TCP/local recovery
        Path backupDir = Paths.get("peer-data", "chunks-backup", shared.getFileId());
        Files.createDirectories(backupDir);
        Files.write(backupDir.resolve("1.chunk"), backupChunk1Bytes);

        report.put("missingChunk1ConfirmedOnDisk", !chunkManagerService.hasChunk(shared.getFileId(), 1));

        // 5. DownloadManager recovers chunk 1 via TCP/safeguard recovery pipeline
        Map<String, Object> recoveryResult = downloadManagerService.recoverMissingChunks(
                shared.getFileId(),
                "recovered-sample.bin",
                totalChunks,
                "127.0.0.1",
                tcpPort
        );

        report.put("recoveryExecution", recoveryResult);

        // 6. Verify 100% byte integrity of reassembled file
        File reassembled = new File(recoveryResult.get("reassembledFile").toString());
        byte[] reassembledBytes = Files.readAllBytes(reassembled.toPath());
        boolean byteMatch = Arrays.equals(originalData, reassembledBytes);

        report.put("reconstructedByteMatch", byteMatch);
        report.put("testStatus", byteMatch ? "SUCCESS - CHUNK RECOVERY VERIFIED" : "FAILED");

        // Clean up temporary test files
        try {
            testFile.delete();
            reassembled.delete();
            Files.deleteIfExists(backupDir.resolve("1.chunk"));
        } catch (Exception ignored) {}

        return report;
    }
}
