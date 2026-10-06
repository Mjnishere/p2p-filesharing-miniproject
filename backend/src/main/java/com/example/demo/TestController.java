package com.example.demo;

import com.example.demo.service.ChunkManagerService;
import com.example.demo.service.SearchManagerService;
import modell.NetworkMessage;
import modell.SearchResult;
import modell.SharedFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestController
public class TestController {

    @Autowired
    private ChunkManagerService chunkManagerService;

    @Autowired
    private NetworkService networkService;

    @Autowired
    private SearchManagerService searchManagerService;

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
        String rawResponse = networkService.sendDirectMessage("127.0.0.1", 9001, ping);

        return Map.of(
                "sentMessageType", "HELLO",
                "targetPort", 9001,
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
}