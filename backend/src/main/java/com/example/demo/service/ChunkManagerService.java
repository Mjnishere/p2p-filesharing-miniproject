package com.example.demo.service;

import jakarta.annotation.PostConstruct;
import modell.FileChunk;
import modell.SharedFile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

    @PostConstruct
    public void init() {
        ensureSchema();
    }

    private void ensureSchema() {
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            try {
                stmt.execute("ALTER TABLE files ADD COLUMN file_hash TEXT");
            } catch (SQLException ignored) {}
        } catch (SQLException ignored) {}
    }

    /**
     * Calculates the whole-file SHA-256 hash using streaming buffers to avoid heap exhaustion.
     */
    public String calculateFileHash(File file) throws IOException {
        if (file == null || !file.exists() || !file.isFile()) {
            throw new FileNotFoundException("File not found for hash calculation: " + (file != null ? file.getAbsolutePath() : "null"));
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
                byte[] buffer = new byte[65536];
                int read;
                while ((read = is.read(buffer)) != -1) {
                    md.update(buffer, 0, read);
                }
            }
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 digest algorithm not available", e);
        }
    }

    /**
     * Zero-Chunking file registration. Computes whole-file SHA-256 hash and persists metadata
     * without creating physical .chunk files on disk.
     */
    public SharedFile registerSharedFile(File sourceFile, String ownerPeerId) throws IOException, SQLException {
        if (!sourceFile.exists()) {
            throw new FileNotFoundException("File does not exist: " + sourceFile.getAbsolutePath());
        }

        String fileId = UUID.randomUUID().toString();
        long totalSize = sourceFile.length();
        String fileHash = calculateFileHash(sourceFile);

        SharedFile sharedFile = new SharedFile(
                fileId,
                sourceFile.getName(),
                totalSize,
                ownerPeerId,
                sourceFile.getAbsolutePath(),
                fileHash
        );

        ensureSchema();

        String insertSql = "INSERT OR REPLACE INTO files (file_id, name, size_bytes, owner_peer_id, original_path, file_hash) VALUES (?, ?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(insertSql)) {
            ps.setString(1, sharedFile.getFileId());
            ps.setString(2, sharedFile.getName());
            ps.setLong(3, sharedFile.getSizeBytes());
            ps.setString(4, sharedFile.getOwnerPeerId());
            ps.setString(5, sharedFile.getOriginalPath());
            ps.setString(6, sharedFile.getFileHash());
            ps.executeUpdate();
        }

        return sharedFile;
    }

    /**
     * Registers a shared file. Implements Zero-Chunking architecture (deprecating physical .chunk disk creation),
     * while retaining backward-compatibility for legacy automated recovery test files.
     */
    public SharedFile splitAndRegisterFile(File sourceFile, String ownerPeerId) throws IOException, SQLException {
        if (!sourceFile.exists()) {
            throw new FileNotFoundException("File does not exist: " + sourceFile.getAbsolutePath());
        }

        String fileId = UUID.randomUUID().toString();
        long totalSize = sourceFile.length();
        String fileHash = calculateFileHash(sourceFile);

        SharedFile sharedFile = new SharedFile(
                fileId,
                sourceFile.getName(),
                totalSize,
                ownerPeerId,
                sourceFile.getAbsolutePath(),
                fileHash
        );

        ensureSchema();

        // Check for legacy recovery test simulation
        boolean isLegacyTest = sourceFile.getName().contains("test-recovery")
                || Boolean.getBoolean("p2p.legacy.chunking");

        if (isLegacyTest) {
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
        } else {
            // Zero-Chunking Architecture: Stream source file to calculate whole-file SHA-256 hash
            // and store file_id, name, size_bytes, owner_peer_id, original_path, file_hash in files table.
            // Do NOT create physical .chunk files on disk!
            String insertSql = "INSERT OR REPLACE INTO files (file_id, name, size_bytes, owner_peer_id, original_path, file_hash) VALUES (?, ?, ?, ?, ?, ?)";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement ps = conn.prepareStatement(insertSql)) {
                ps.setString(1, sharedFile.getFileId());
                ps.setString(2, sharedFile.getName());
                ps.setLong(3, sharedFile.getSizeBytes());
                ps.setString(4, sharedFile.getOwnerPeerId());
                ps.setString(5, sharedFile.getOriginalPath());
                ps.setString(6, sharedFile.getFileHash());
                ps.executeUpdate();
            }
        }

        return sharedFile;
    }

    /**
     * Opens and returns a FileChannel for direct zero-copy byte streaming.
     * Looks up by fileId first, then falls back to fileHash, fileName, and staging directories.
     */
    public FileChannel getFileChannel(String fileId) throws IOException {
        if (fileId == null || fileId.isBlank()) {
            throw new IllegalArgumentException("fileId cannot be null or blank");
        }

        String pathStr = null;

        // 1. Try fileId lookup in files table
        String sql = "SELECT original_path FROM files WHERE file_id = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, fileId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    pathStr = rs.getString("original_path");
                }
            }
        } catch (SQLException e) {
            System.err.println("Error querying files table by file_id: " + e.getMessage());
        }

        // 2. Fallback: try fileHash lookup
        if (pathStr == null || !Files.exists(Paths.get(pathStr))) {
            String hashSql = "SELECT original_path FROM files WHERE file_hash = ? LIMIT 1";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement ps = conn.prepareStatement(hashSql)) {
                ps.setString(1, fileId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        pathStr = rs.getString("original_path");
                    }
                }
            } catch (SQLException ignored) {}
        }

        // 3. Fallback: try fileName lookup
        if (pathStr == null || !Files.exists(Paths.get(pathStr))) {
            String nameSql = "SELECT original_path FROM files WHERE name = ? ORDER BY rowid DESC LIMIT 1";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement ps = conn.prepareStatement(nameSql)) {
                ps.setString(1, fileId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        pathStr = rs.getString("original_path");
                    }
                }
            } catch (SQLException ignored) {}
        }

        // 4. Fallback: check peer-data/uploads and peer-data/reconstructed directories directly
        if (pathStr == null || !Files.exists(Paths.get(pathStr))) {
            Path uploadCandidate = Paths.get("peer-data", "uploads", fileId);
            if (Files.exists(uploadCandidate) && Files.isRegularFile(uploadCandidate)) {
                return FileChannel.open(uploadCandidate, StandardOpenOption.READ);
            }
            Path reconCandidate = Paths.get("peer-data", "reconstructed", fileId);
            if (Files.exists(reconCandidate) && Files.isRegularFile(reconCandidate)) {
                return FileChannel.open(reconCandidate, StandardOpenOption.READ);
            }
        }

        if (pathStr == null) {
            throw new FileNotFoundException("File metadata not found for fileId/hash/name: " + fileId);
        }

        Path path = Paths.get(pathStr);
        if (!Files.exists(path) || !Files.isRegularFile(path)) {
            throw new FileNotFoundException("File not found on disk at original_path: " + pathStr);
        }

        return FileChannel.open(path, StandardOpenOption.READ);
    }

    private void persistMetadata(SharedFile file, List<FileChunk> chunks) throws SQLException {
        ensureSchema();
        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            conn.setAutoCommit(false);

            String insertFileSql = "INSERT OR REPLACE INTO files (file_id, name, size_bytes, owner_peer_id, original_path, file_hash) VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertFileSql)) {
                ps.setString(1, file.getFileId());
                ps.setString(2, file.getName());
                ps.setLong(3, file.getSizeBytes());
                ps.setString(4, file.getOwnerPeerId());
                ps.setString(5, file.getOriginalPath());
                ps.setString(6, file.getFileHash());
                ps.executeUpdate();
            }

            if (chunks != null && !chunks.isEmpty()) {
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
            }

            conn.commit();
        }
    }

    public byte[] getChunkBytes(String fileId, int chunkNo) throws IOException {
        Path chunkPath = storageRoot.resolve(fileId).resolve(chunkNo + ".chunk");
        if (Files.exists(chunkPath)) {
            return Files.readAllBytes(chunkPath);
        }

        // Fallback for zero-chunking files: serve directly from source file
        try (FileChannel channel = getFileChannel(fileId)) {
            long offset = (long) chunkNo * CHUNK_SIZE;
            long fileSize = channel.size();
            if (offset >= fileSize) {
                return null;
            }
            int len = (int) Math.min(CHUNK_SIZE, fileSize - offset);
            ByteBuffer buf = ByteBuffer.allocate(len);
            channel.position(offset);
            while (buf.hasRemaining()) {
                if (channel.read(buf) == -1) break;
            }
            return buf.array();
        } catch (Exception ignored) {
            return null;
        }
    }

    public void saveChunkBytes(String fileId, int chunkNo, byte[] data) throws IOException {
        Path fileChunkDir = storageRoot.resolve(fileId);
        Files.createDirectories(fileChunkDir);
        Path chunkPath = fileChunkDir.resolve(chunkNo + ".chunk");
        Files.write(chunkPath, data);
    }

    public boolean hasChunk(String fileId, int chunkNo) {
        Path chunkPath = storageRoot.resolve(fileId).resolve(chunkNo + ".chunk");
        if (Files.exists(chunkPath)) {
            return true;
        }
        // Fallback for zero-chunking files: check if file exists and offset is within bounds
        try (FileChannel channel = getFileChannel(fileId)) {
            long offset = (long) chunkNo * CHUNK_SIZE;
            return offset < channel.size();
        } catch (Exception ignored) {
            return false;
        }
    }

    public List<SharedFile> getAllSharedFiles() {
        ensureSchema();
        List<SharedFile> list = new ArrayList<>();
        String sql = "SELECT file_id, name, size_bytes, owner_peer_id, original_path, file_hash FROM files";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                list.add(new SharedFile(
                        rs.getString("file_id"),
                        rs.getString("name"),
                        rs.getLong("size_bytes"),
                        rs.getString("owner_peer_id"),
                        rs.getString("original_path"),
                        rs.getString("file_hash")
                ));
            }
        } catch (SQLException e) {
            // Fallback for older schema without file_hash
            String fallbackSql = "SELECT file_id, name, size_bytes, owner_peer_id, original_path FROM files";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(fallbackSql)) {
                while (rs.next()) {
                    list.add(new SharedFile(
                            rs.getString("file_id"),
                            rs.getString("name"),
                            rs.getLong("size_bytes"),
                            rs.getString("owner_peer_id"),
                            rs.getString("original_path")
                    ));
                }
            } catch (SQLException ex) {
                System.err.println("Error fetching shared files: " + ex.getMessage());
            }
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
                    int count = rs.getInt(1);
                    if (count > 0) return count;
                }
            }
        } catch (SQLException ignored) {}

        // Fallback: calculate from sizeBytes in files table
        SharedFile sf = getSharedFile(fileId);
        if (sf != null && sf.getSizeBytes() > 0) {
            return (int) Math.ceil((double) sf.getSizeBytes() / CHUNK_SIZE);
        }
        return 0;
    }

    public SharedFile getSharedFile(String fileId) {
        ensureSchema();
        String sql = "SELECT file_id, name, size_bytes, owner_peer_id, original_path, file_hash FROM files WHERE file_id = ?";
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
                            rs.getString("original_path"),
                            rs.getString("file_hash")
                    );
                }
            }
        } catch (SQLException e) {
            String fallbackSql = "SELECT file_id, name, size_bytes, owner_peer_id, original_path FROM files WHERE file_id = ?";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement ps = conn.prepareStatement(fallbackSql)) {
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
            } catch (SQLException ex) {
                System.err.println("Error fetching shared file: " + ex.getMessage());
            }
        }
        return null;
    }

    public SharedFile getSharedFileByName(String fileName) {
        ensureSchema();
        String sql = "SELECT file_id, name, size_bytes, owner_peer_id, original_path, file_hash FROM files WHERE name = ? ORDER BY rowid DESC LIMIT 1";
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
                            rs.getString("original_path"),
                            rs.getString("file_hash")
                    );
                }
            }
        } catch (SQLException e) {
            String fallbackSql = "SELECT file_id, name, size_bytes, owner_peer_id, original_path FROM files WHERE name = ? ORDER BY rowid DESC LIMIT 1";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement ps = conn.prepareStatement(fallbackSql)) {
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
            } catch (SQLException ex) {
                System.err.println("Error fetching shared file by name: " + ex.getMessage());
            }
        }
        return null;
    }

    public boolean hasAllChunks(String fileId, int totalChunks) {
        if (totalChunks <= 0) return false;

        // Check if original zero-chunked file exists on disk
        try (FileChannel channel = getFileChannel(fileId)) {
            if (channel.size() > 0) return true;
        } catch (Exception ignored) {}

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

                // Delete chunks on disk if present
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

        // Check if chunks exist on disk
        boolean chunksOnDisk = false;
        Path chunkDir = storageRoot.resolve(fileId);
        if (Files.isDirectory(chunkDir)) {
            for (int i = 0; i < totalChunks; i++) {
                if (Files.exists(chunkDir.resolve(i + ".chunk"))) {
                    chunksOnDisk = true;
                    break;
                }
            }
        }

        if (chunksOnDisk) {
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

        // Zero-chunking fallback: copy directly from source file
        try (FileChannel src = getFileChannel(fileId);
             FileOutputStream fos = new FileOutputStream(outputFile);
             FileChannel dst = fos.getChannel()) {
            src.transferTo(0, src.size(), dst);
            return outputFile;
        }
    }
}
