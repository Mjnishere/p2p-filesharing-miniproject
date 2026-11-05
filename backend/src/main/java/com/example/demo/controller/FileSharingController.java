package com.example.demo.controller;

import com.example.demo.service.ChunkManagerService;
import com.example.demo.service.DownloadManagerService;
import com.example.demo.service.SearchManagerService;
import modell.PeerInfo;
import modell.SearchResult;
import modell.SharedFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class FileSharingController {

    @Autowired
    private ChunkManagerService chunkManagerService;

    @Autowired
    private SearchManagerService searchManagerService;

    @Autowired
    private DownloadManagerService downloadManagerService;

    // List all files shared by this peer node
    @GetMapping("/files")
    public List<SharedFile> listSharedFiles() {
        return chunkManagerService.getAllSharedFiles();
    }

    // Share a local file: slices into 1 MB chunks and registers in SQLite
    @PostMapping("/files/share")
    public Map<String, Object> shareFile(@RequestParam String filePath,
                                         @RequestParam(defaultValue = "peer-1") String ownerPeerId) throws IOException, SQLException {
        File file = new File(filePath);
        SharedFile shared = chunkManagerService.splitAndRegisterFile(file, ownerPeerId);
        int totalChunks = (int) Math.ceil((double) shared.getSizeBytes() / ChunkManagerService.CHUNK_SIZE);

        return Map.of(
                "status", "SHARED",
                "fileId", shared.getFileId(),
                "fileName", shared.getName(),
                "sizeBytes", shared.getSizeBytes(),
                "totalChunks", totalChunks
        );
    }

    // Upload a file directly from browser drag-and-drop: writes to staging, slices into 1 MB chunks, registers in SQLite
    @PostMapping(value = "/files/upload", consumes = {"multipart/form-data"})
    public Map<String, Object> uploadFile(@RequestParam("file") MultipartFile multipartFile,
                                          @RequestParam(defaultValue = "peer-1") String ownerPeerId) throws IOException, SQLException {
        if (multipartFile.isEmpty()) {
            throw new IllegalArgumentException("Uploaded file is empty");
        }
        Path uploadDir = Paths.get("peer-data", "uploads").toAbsolutePath();
        Files.createDirectories(uploadDir);
        String originalFilename = multipartFile.getOriginalFilename();
        String safeName = (originalFilename != null && !originalFilename.isBlank()) ? originalFilename : "uploaded-file.bin";
        Path targetPath = uploadDir.resolve(safeName);
        Files.copy(multipartFile.getInputStream(), targetPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        SharedFile shared = chunkManagerService.splitAndRegisterFile(targetPath.toFile(), ownerPeerId);
        int totalChunks = (int) Math.ceil((double) shared.getSizeBytes() / ChunkManagerService.CHUNK_SIZE);

        return Map.of(
                "status", "SHARED",
                "fileId", shared.getFileId(),
                "fileName", shared.getName(),
                "sizeBytes", shared.getSizeBytes(),
                "totalChunks", totalChunks
        );
    }

    // Delete shared file metadata and chunk storage
    @DeleteMapping(value = {"/files/{fileId}", "/files/delete"})
    public Map<String, Object> deleteFile(@PathVariable(required = false) String fileId,
                                          @RequestParam(required = false) String fileIdParam) {
        String targetId = (fileId != null && !fileId.isBlank()) ? fileId : fileIdParam;
        if (targetId == null || targetId.isBlank()) {
            throw new IllegalArgumentException("fileId must be provided");
        }
        boolean deleted = chunkManagerService.deleteSharedFile(targetId);
        if (deleted) {
            downloadManagerService.resetJob(targetId);
        }
        return Map.of(
            "status", deleted ? "DELETED" : "NOT_FOUND",
            "fileId", targetId
        );
    }

    @ExceptionHandler(FileNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, Object> handleFileNotFound(FileNotFoundException e) {
        return Map.of(
            "status", "ERROR",
            "message", "File not found on host filesystem: " + e.getMessage() + ". For files outside the project root, please drag & drop the file directly."
        );
    }

    // Distributed search with hop-by-hop TTL decrement and loop suppression
    @GetMapping("/search")
    public Map<String, Object> search(@RequestParam String q,
                                      @RequestParam(defaultValue = "3") int ttl) {
        List<SearchResult> results = searchManagerService.executeDistributedSearch(q, ttl);
        return Map.of(
                "query", q,
                "ttl", ttl,
                "resultsFound", results.size(),
                "results", results
        );
    }

    // Trigger chunked download with per-chunk state machine and timeout retry
    @PostMapping("/download/start")
    public DownloadManagerService.DownloadJob startDownload(@RequestParam String fileId,
                                                            @RequestParam String fileName,
                                                            @RequestParam int totalChunks,
                                                            @RequestParam String sourceHost,
                                                            @RequestParam int sourcePort) {
        return downloadManagerService.startDownload(fileId, fileName, totalChunks, sourceHost, sourcePort);
    }

    // Poll download status and per-chunk states for Web UI progress bar
    @GetMapping("/download/status")
    public DownloadManagerService.DownloadJob getDownloadStatus(@RequestParam String fileId) {
        return downloadManagerService.getJob(fileId);
    }

    // List all active and completed downloads
    @GetMapping("/download/all")
    public Collection<DownloadManagerService.DownloadJob> listAllDownloads() {
        return downloadManagerService.getAllJobs();
    }

    // Stream file directly to client browser/device as an attachment download
    @GetMapping(value = {"/files/download", "/download/file"})
    public ResponseEntity<Resource> downloadFileToDevice(
            @RequestParam(required = false) String fileId,
            @RequestParam(required = false) String fileName) throws IOException {

        String jobFileName = null;
        int jobTotalChunks = 0;
        if (fileId != null && !fileId.isBlank()) {
            DownloadManagerService.DownloadJob job = downloadManagerService.getJob(fileId);
            if (job != null) {
                jobFileName = job.getFileName();
                jobTotalChunks = job.getTotalChunks();
            }
        }

        File file = chunkManagerService.resolveFileForDownload(fileId, fileName, jobFileName, jobTotalChunks);
        if (file == null || !file.exists() || !file.isFile()) {
            throw new FileNotFoundException("Requested file was not found on node storage.");
        }

        String downloadName = (fileName != null && !fileName.isBlank())
                ? fileName
                : ((jobFileName != null && !jobFileName.isBlank()) ? jobFileName : file.getName());

        Resource resource = new FileSystemResource(file);

        ContentDisposition disposition = ContentDisposition.attachment()
                .filename(downloadName, StandardCharsets.UTF_8)
                .build();

        String mimeType = null;
        try {
            mimeType = Files.probeContentType(file.toPath());
        } catch (Exception ignored) {}
        if (mimeType == null || mimeType.isBlank()) {
            mimeType = MediaType.APPLICATION_OCTET_STREAM_VALUE;
        }

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, disposition.toString())
                .contentType(MediaType.parseMediaType(mimeType))
                .contentLength(file.length())
                .body(resource);
    }

    // Recover missing chunks and reassemble file with byte integrity verification
    @PostMapping("/download/recover")
    public Map<String, Object> recoverDownload(@RequestParam String fileId,
                                               @RequestParam String fileName,
                                               @RequestParam int totalChunks,
                                               @RequestParam(defaultValue = "127.0.0.1") String sourceHost,
                                               @RequestParam(defaultValue = "9001") int sourcePort) throws IOException {
        return downloadManagerService.recoverMissingChunks(fileId, fileName, totalChunks, sourceHost, sourcePort);
    }

    // Diagnostic simulation: artificially drop specific chunks to test recovery
    @PostMapping("/test/simulate-chunk-fault")
    public Map<String, Object> simulateChunkFault(@RequestParam String fileId,
                                                  @RequestParam(defaultValue = "5,15") String chunkNos) throws IOException {
        Path simulatedDir = Paths.get("peer-data", "simulated-missing", fileId).toAbsolutePath();
        Path chunksDir = Paths.get("peer-data", "chunks", fileId).toAbsolutePath();
        Path backupDir = Paths.get("peer-data", "chunks-backup", fileId).toAbsolutePath();
        Files.createDirectories(simulatedDir);
        List<Integer> dropped = new java.util.ArrayList<>();

        for (String cStr : chunkNos.split(",")) {
            try {
                int c = Integer.parseInt(cStr.trim());
                Path chunkPath = chunksDir.resolve(c + ".chunk");
                Path backupChunk = backupDir.resolve(c + ".chunk");
                Path destSim = simulatedDir.resolve(c + ".chunk");

                // Ensure we have backup before removing from chunksDir
                if (!Files.exists(backupChunk) && Files.exists(chunkPath)) {
                    Files.createDirectories(backupDir);
                    Files.copy(chunkPath, backupChunk, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }

                if (Files.exists(chunkPath)) {
                    Files.move(chunkPath, destSim, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    dropped.add(c);
                } else if (Files.exists(destSim)) {
                    dropped.add(c);
                }
            } catch (NumberFormatException ignored) {}
        }

        // Clean up previously reconstructed output file so recovery can be proven fresh
        SharedFile shared = chunkManagerService.getAllSharedFiles().stream()
                .filter(f -> f.getFileId().equals(fileId))
                .findFirst().orElse(null);
        if (shared != null) {
            Path reconstructed = Paths.get("peer-data", "reconstructed", shared.getName()).toAbsolutePath();
            Files.deleteIfExists(reconstructed);
        }

        // Reset the active download job so user can download again fresh
        downloadManagerService.resetJob(fileId);

        return Map.of(
            "status", "CHUNKS_CORRUPTED",
            "fileId", fileId,
            "droppedChunks", dropped,
            "message", "Dropped chunks " + dropped + " from node storage. Active download job reset."
        );
    }

    // Diagnostic simulation: restore all dropped chunks back to peer storage
    @PostMapping("/test/restore-chunk-fault")
    public Map<String, Object> restoreChunkFault(@RequestParam String fileId) throws IOException {
        Path simulatedDir = Paths.get("peer-data", "simulated-missing", fileId).toAbsolutePath();
        Path backupDir = Paths.get("peer-data", "chunks-backup", fileId).toAbsolutePath();
        Path chunksDir = Paths.get("peer-data", "chunks", fileId).toAbsolutePath();
        List<String> restored = new java.util.ArrayList<>();

        if (Files.exists(simulatedDir)) {
            try (var stream = Files.list(simulatedDir)) {
                for (Path p : stream.toList()) {
                    Files.move(p, chunksDir.resolve(p.getFileName()), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    restored.add(p.getFileName().toString());
                }
            }
        } else if (Files.exists(backupDir)) {
            try (var stream = Files.list(backupDir)) {
                for (Path p : stream.toList()) {
                    Files.copy(p, chunksDir.resolve(p.getFileName()), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    restored.add(p.getFileName().toString());
                }
            }
        }

        return Map.of(
            "status", "CHUNKS_RESTORED",
            "fileId", fileId,
            "restoredChunks", restored,
            "message", "All chunks restored back into peer node storage."
        );
    }

    // List all known peer endpoints
    @GetMapping("/peers")
    public List<PeerInfo> listPeers() {
        return searchManagerService.getKnownPeers();
    }

    // Register a new peer endpoint
    @PostMapping("/peers/register")
    public Map<String, Object> registerPeer(@RequestParam String peerId,
                                            @RequestParam String host,
                                            @RequestParam int port) {
        PeerInfo peer = new PeerInfo(peerId, host, port);
        searchManagerService.registerPeer(peer);
        return Map.of(
                "status", "REGISTERED",
                "peer", peer
        );
    }

    // Remove / unregister a peer endpoint
    @DeleteMapping(value = {"/peers/{peerId}", "/peers/remove", "/peers"})
    public Map<String, Object> removePeer(@PathVariable(required = false) String peerId,
                                          @RequestParam(required = false) String peerIdParam) {
        String targetPeerId = (peerId != null && !peerId.isBlank()) ? peerId : peerIdParam;
        if (targetPeerId == null || targetPeerId.isBlank()) {
            throw new IllegalArgumentException("peerId must be provided");
        }
        boolean removed = searchManagerService.removePeer(targetPeerId);
        return Map.of(
                "status", removed ? "REMOVED" : "NOT_FOUND",
                "peerId", targetPeerId
        );
    }
}
