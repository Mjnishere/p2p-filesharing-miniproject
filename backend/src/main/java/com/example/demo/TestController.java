package com.example.demo;

import com.example.demo.service.ChunkManagerService;

import modell.NetworkMessage;
import modell.SharedFile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

@RestController
public class TestController {

    private final ChunkManagerService chunkManagerService;
    private final NetworkService networkService;

    public TestController(ChunkManagerService chunkManagerService, NetworkService networkService) {
        this.chunkManagerService = chunkManagerService;
        this.networkService = networkService;
    }

    @GetMapping("/api/test/chunking")
    public Map<String, Object> testChunking() throws IOException, SQLException {
        File testFile = new File("test-sample.bin");
        byte[] dummyData = new byte[2500 * 1024];
        Arrays.fill(dummyData, (byte) 7);
        try (FileOutputStream fos = new FileOutputStream(testFile)) {
            fos.write(dummyData);
        }

        SharedFile shared = chunkManagerService.splitAndRegisterFile(testFile, "peer-node-1");
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
}