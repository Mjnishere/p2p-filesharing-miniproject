package com.example.demo.service;

import modell.FileChunk;
import modell.SharedFile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class ChunkManagerService {

    public static final int CHUNK_SIZE = 1024 * 1024; // 1 MB

    @Value("${db.url:jdbc:sqlite:peer.db}")
    private String dbUrl;

    private final Path storageRoot = Paths.get("peer-data", "chunks");

    public ChunkManagerService() {
        try {
            Files.createDirectories(storageRoot);
        } catch (IOException e) {
            System.err.println("Could not create chunk storage root: " + e.getMessage());
        }
    }

    public SharedFile splitAndRegisterFile(File sourceFile, String ownerPeerId) throws IOException, SQLException {
        if (!sourceFile.exists()) {
            throw new FileNotFoundException("File does not exist: " + sourceFile.getAbsolutePath());
        }

        String fileId = UUID.randomUUID().toString();
        long totalSize = sourceFile.length();
        SharedFile sharedFile = new SharedFile(
                fileId,
                sourceFile.getName(),
                totalSize,
                ownerPeerId,
                sourceFile.getAbsolutePath()
        );

        Path fileChunkDir = storageRoot.resolve(fileId);
        Files.createDirectories(fileChunkDir);

        List<FileChunk> chunks = new ArrayList<>();
        byte[] buffer = new byte[CHUNK_SIZE];

        try (InputStream in = new BufferedInputStream(new FileInputStream(sourceFile))) {
            int chunkNo = 0;
            int bytesRead;

            while ((bytesRead = in.read(buffer)) > 0) {
                String chunkId = UUID.randomUUID().toString();
                Path chunkPath = fileChunkDir.resolve(chunkNo + ".chunk");

                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(chunkPath.toFile()))) {
                    out.write(buffer, 0, bytesRead);
                }

                chunks.add(new FileChunk(
                        chunkId,
                        fileId,
                        chunkNo,
                        bytesRead,
                        chunkPath.toString()
                ));
                chunkNo++;
            }
        }

        persistMetadata(sharedFile, chunks);
        return sharedFile;
    }

    private void persistMetadata(SharedFile file, List<FileChunk> chunks) throws SQLException {
        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            conn.setAutoCommit(false);

            String insertFileSql = "INSERT INTO files (file_id, name, size_bytes, owner_peer_id, original_path) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertFileSql)) {
                ps.setString(1, file.getFileId());
                ps.setString(2, file.getName());
                ps.setLong(3, file.getSizeBytes());
                ps.setString(4, file.getOwnerPeerId());
                ps.setString(5, file.getOriginalPath());
                ps.executeUpdate();
            }

            String insertChunkSql = "INSERT INTO chunks (chunk_id, file_id, chunk_no, size_bytes, path) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertChunkSql)) {
                for (FileChunk chunk : chunks) {
                    ps.setString(1, chunk.getChunkId());
                    ps.setString(2, chunk.getFileId());
                    ps.setInt(3, chunk.getChunkNo());
                    ps.setLong(4, chunk.getSizeBytes());
                    ps.setString(5, chunk.getPath());
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            conn.commit();
        }
    }

    public File reassembleFile(String fileId, String outputFileName, int totalChunks) throws IOException {
        Path reconstructedDir = Paths.get("peer-data", "reconstructed");
        Files.createDirectories(reconstructedDir);
        File outputFile = reconstructedDir.resolve(outputFileName).toFile();

        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(outputFile))) {
            for (int i = 0; i < totalChunks; i++) {
                Path chunkPath = storageRoot.resolve(fileId).resolve(i + ".chunk");
                if (!Files.exists(chunkPath)) {
                    throw new FileNotFoundException("Missing chunk index: " + i + " at " + chunkPath);
                }
                Files.copy(chunkPath, out);
            }
        }

        return outputFile;
    }
}