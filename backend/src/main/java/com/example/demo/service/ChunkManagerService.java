package com.example.demo.service;

import modell.FileChunk;
import modell.SharedFile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
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

    public byte[] getChunkBytes(String fileId, int chunkNo) throws IOException {
        Path chunkPath = storageRoot.resolve(fileId).resolve(chunkNo + ".chunk");
        if (!Files.exists(chunkPath)) {
            return null;
        }
        return Files.readAllBytes(chunkPath);
    }

    public void saveChunkBytes(String fileId, int chunkNo, byte[] data) throws IOException {
        Path fileChunkDir = storageRoot.resolve(fileId);
        Files.createDirectories(fileChunkDir);
        Path chunkPath = fileChunkDir.resolve(chunkNo + ".chunk");
        Files.write(chunkPath, data);
    }

    public boolean hasChunk(String fileId, int chunkNo) {
        Path chunkPath = storageRoot.resolve(fileId).resolve(chunkNo + ".chunk");
        return Files.exists(chunkPath);
    }

    public List<SharedFile> getAllSharedFiles() {
        List<SharedFile> list = new ArrayList<>();
        String sql = "SELECT file_id, name, size_bytes, owner_peer_id, original_path FROM files";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                list.add(new SharedFile(
                        rs.getString("file_id"),
                        rs.getString("name"),
                        rs.getLong("size_bytes"),
                        rs.getString("owner_peer_id"),
                        rs.getString("original_path")
                ));
            }
        } catch (SQLException e) {
            System.err.println("Error fetching shared files: " + e.getMessage());
        }
        return list;
    }

    public int getTotalChunksForFile(String fileId) {
        String sql = "SELECT COUNT(*) FROM chunks WHERE file_id = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, fileId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            System.err.println("Error fetching chunk count: " + e.getMessage());
        }
        return 0;
    }

    public SharedFile getSharedFile(String fileId) {
        String sql = "SELECT file_id, name, size_bytes, owner_peer_id, original_path FROM files WHERE file_id = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, fileId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new SharedFile(
                            rs.getString("file_id"),
                            rs.getString("name"),
                            rs.getLong("size_bytes"),
                            rs.getString("owner_peer_id"),
                            rs.getString("original_path")
                    );
                }
            }
        } catch (SQLException e) {
            System.err.println("Error fetching shared file: " + e.getMessage());
        }
        return null;
    }

    public SharedFile getSharedFileByName(String fileName) {
        String sql = "SELECT file_id, name, size_bytes, owner_peer_id, original_path FROM files WHERE name = ? ORDER BY rowid DESC LIMIT 1";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, fileName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new SharedFile(
                            rs.getString("file_id"),
                            rs.getString("name"),
                            rs.getLong("size_bytes"),
                            rs.getString("owner_peer_id"),
                            rs.getString("original_path")
                    );
                }
            }
        } catch (SQLException e) {
            System.err.println("Error fetching shared file by name: " + e.getMessage());
        }
        return null;
    }

    public boolean hasAllChunks(String fileId, int totalChunks) {
        if (totalChunks <= 0) return false;
        for (int i = 0; i < totalChunks; i++) {
            if (!hasChunk(fileId, i)) {
                return false;
            }
        }
        return true;
    }

    public File resolveFileForDownload(String fileId, String fileName, String jobFileName, int jobTotalChunks) throws IOException {
        Path reconstructedDir = Paths.get("peer-data", "reconstructed");
        Path uploadsDir = Paths.get("peer-data", "uploads");

        // 1. Try fileId lookup in database
        if (fileId != null && !fileId.isBlank()) {
            SharedFile shared = getSharedFile(fileId);
            if (shared != null) {
                if (fileName == null || fileName.isBlank()) {
                    fileName = shared.getName();
                }
                // Check original path
                if (shared.getOriginalPath() != null) {
                    File orig = new File(shared.getOriginalPath());
                    if (orig.exists() && orig.isFile()) {
                        return orig;
                    }
                }
                // Check reconstructed directory
                File recon = reconstructedDir.resolve(shared.getName()).toFile();
                if (recon.exists() && recon.isFile()) {
                    return recon;
                }
                // Reassemble from chunks if available
                int totalChunks = getTotalChunksForFile(fileId);
                if (totalChunks > 0 && hasAllChunks(fileId, totalChunks)) {
                    return reassembleFile(fileId, shared.getName(), totalChunks);
                }
            }

            // Check if job fileName matches reconstructed file
            if (jobFileName != null && !jobFileName.isBlank()) {
                File recon = reconstructedDir.resolve(jobFileName).toFile();
                if (recon.exists() && recon.isFile()) {
                    return recon;
                }
                if (jobTotalChunks > 0 && hasAllChunks(fileId, jobTotalChunks)) {
                    return reassembleFile(fileId, jobFileName, jobTotalChunks);
                }
            }

            // Check chunks directory directly for fileId
            Path chunkDir = storageRoot.resolve(fileId);
            if (Files.isDirectory(chunkDir)) {
                try (var stream = Files.list(chunkDir)) {
                    long chunkCount = stream.filter(p -> p.getFileName().toString().endsWith(".chunk")).count();
                    if (chunkCount > 0) {
                        String targetName = (fileName != null && !fileName.isBlank())
                                ? fileName
                                : ((jobFileName != null && !jobFileName.isBlank()) ? jobFileName : fileId + ".bin");
                        return reassembleFile(fileId, targetName, (int) chunkCount);
                    }
                }
            }
        }

        // 2. Try fileName lookup
        if (fileName != null && !fileName.isBlank()) {
            // Check reconstructed directory
            File recon = reconstructedDir.resolve(fileName).toFile();
            if (recon.exists() && recon.isFile()) {
                return recon;
            }
            // Check uploads directory
            File uploaded = uploadsDir.resolve(fileName).toFile();
            if (uploaded.exists() && uploaded.isFile()) {
                return uploaded;
            }
            // Check database by fileName
            SharedFile shared = getSharedFileByName(fileName);
            if (shared != null) {
                if (shared.getOriginalPath() != null) {
                    File orig = new File(shared.getOriginalPath());
                    if (orig.exists() && orig.isFile()) {
                        return orig;
                    }
                }
                int totalChunks = getTotalChunksForFile(shared.getFileId());
                if (totalChunks > 0 && hasAllChunks(shared.getFileId(), totalChunks)) {
                    return reassembleFile(shared.getFileId(), shared.getName(), totalChunks);
                }
            }
            // Check project directory directly
            File localFile = new File(fileName);
            if (localFile.exists() && localFile.isFile()) {
                return localFile;
            }
        }

        return null;
    }

    public boolean deleteSharedFile(String fileId) {
        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps1 = conn.prepareStatement("DELETE FROM chunks WHERE file_id = ?");
                 PreparedStatement ps2 = conn.prepareStatement("DELETE FROM file_peers WHERE file_id = ?");
                 PreparedStatement ps3 = conn.prepareStatement("DELETE FROM files WHERE file_id = ?")) {
                ps1.setString(1, fileId);
                ps1.executeUpdate();
                ps2.setString(1, fileId);
                ps2.executeUpdate();
                ps3.setString(1, fileId);
                int rows = ps3.executeUpdate();
                conn.commit();

                // Delete chunks on disk
                Path chunkDir = storageRoot.resolve(fileId);
                if (Files.exists(chunkDir)) {
                    try (var stream = Files.walk(chunkDir)) {
                        stream.sorted(Comparator.reverseOrder())
                              .forEach(p -> {
                                  try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                              });
                    }
                }
                return rows > 0;
            }
        } catch (Exception e) {
            System.err.println("Error deleting file " + fileId + ": " + e.getMessage());
            return false;
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
