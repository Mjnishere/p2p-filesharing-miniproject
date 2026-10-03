package com.example.demo;

import com.example.demo.service.ChunkManagerService;
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

    public TestController(ChunkManagerService chunkManagerService) {
        this.chunkManagerService = chunkManagerService;
    }

    @GetMapping("/api/test/chunking")
    public Map<String, Object> testChunking() throws IOException, SQLException {
        // 1. Create a dummy test file of 2.5 MB (should produce 3 chunks)
        File testFile = new File("test-sample.bin");
        byte[] dummyData = new byte[2500 * 1024];
        Arrays.fill(dummyData, (byte) 7);
        try (FileOutputStream fos = new FileOutputStream(testFile)) {
            fos.write(dummyData);
        }

        // 2. Split file into chunks and save metadata
        SharedFile shared = chunkManagerService.splitAndRegisterFile(testFile, "peer-node-1");

        // 3. Reconstruct file from chunks
        int totalChunks = (int) Math.ceil((double) shared.getSizeBytes() / ChunkManagerService.CHUNK_SIZE);
        File reassembled = chunkManagerService.reassembleFile(shared.getFileId(), "reassembled-sample.bin", totalChunks);

        // 4. Verify byte equality
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
}
