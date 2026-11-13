package com.example.demo.controller;

import com.example.demo.service.ChunkManagerService;
import com.example.demo.service.DownloadManagerService;
import com.example.demo.service.SearchManagerService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import modell.*;
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
import java.util.*;

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

    @Autowired
    private com.example.demo.NetworkService networkService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    // List all files shared by this peer node
    @GetMapping("/files")
    public List<SharedFile> listSharedFiles() {
        return chunkManagerService.getAllSharedFiles();
    }

    // Share a local file: slices/registers and records in SQLite
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
                "fileHash", shared.getFileHash() != null ? shared.getFileHash() : "",
                "totalChunks", totalChunks
        );
    }

    // Upload a file directly from browser drag-and-drop: writes to staging, registers in SQLite
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
                "fileHash", shared.getFileHash() != null ? shared.getFileHash() : "",
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

    // Zero-Config Discovery: concurrent subnet and port scanning with live RTT measurement
    @RequestMapping(value = "/peers/discover", method = {RequestMethod.POST, RequestMethod.GET})
    public Map<String, Object> discoverPeers() {
        List<PeerEndpoint> discovered = networkService.scanSubnetAndPorts();
        return Map.of(
                "status", "DISCOVERY_COMPLETED",
                "count", discovered != null ? discovered.size() : 0,
                "peers", discovered != null ? discovered : Collections.emptyList()
        );
    }

    // Cache & Mesh Telemetry: returns QueryCache hits/misses, hit rate, active swarms, and average RTT
    @GetMapping("/cache/stats")
    public CacheStats getCacheStats() {
        if (searchManagerService != null) {
            return searchManagerService.getCacheStats();
        }
        return new CacheStats(0, 0, 0.0, 0, 0, 0, 0.0, Collections.emptyMap());
    }

    // Trigger download: supports zero-chunking streaming, swarm peers list, and legacy chunk parameters
    @PostMapping(value = "/download/start")
    public DownloadManagerService.DownloadJob startDownload(
            @RequestParam(required = false) String fileId,
            @RequestParam(required = false) String fileName,
            @RequestParam(required = false) Long fileSize,
            @RequestParam(required = false) String fileHash,
            @RequestParam(required = false) Integer totalChunks,
            @RequestParam(required = false) String sourceHost,
            @RequestParam(required = false) Integer sourcePort,
            @RequestBody(required = false) Map<String, Object> requestBody) {

        String targetFileId = fileId;
        String targetFileName = fileName;
        Long targetFileSize = fileSize;
        String targetFileHash = fileHash;
        Integer targetTotalChunks = totalChunks;
        String targetSourceHost = sourceHost;
        Integer targetSourcePort = sourcePort;
        List<PeerEndpoint> targetPeers = null;

        // Extract from JSON request body if provided
        if (requestBody != null && !requestBody.isEmpty()) {
            if (targetFileId == null && requestBody.containsKey("fileId")) {
                targetFileId = Objects.toString(requestBody.get("fileId"), null);
            }
            if (targetFileName == null && requestBody.containsKey("fileName")) {
                targetFileName = Objects.toString(requestBody.get("fileName"), null);
            }
            if (targetFileSize == null && requestBody.containsKey("fileSize")) {
                try {
                    targetFileSize = Long.parseLong(Objects.toString(requestBody.get("fileSize")));
                } catch (Exception ignored) {}
            }
            if (targetFileHash == null && requestBody.containsKey("fileHash")) {
                targetFileHash = Objects.toString(requestBody.get("fileHash"), null);
            }
            if (targetTotalChunks == null && requestBody.containsKey("totalChunks")) {
                try {
                    targetTotalChunks = Integer.parseInt(Objects.toString(requestBody.get("totalChunks")));
                } catch (Exception ignored) {}
            }
            if (targetSourceHost == null && requestBody.containsKey("sourceHost")) {
                targetSourceHost = Objects.toString(requestBody.get("sourceHost"), null);
            }
            if (targetSourcePort == null && requestBody.containsKey("sourcePort")) {
                try {
                    targetSourcePort = Integer.parseInt(Objects.toString(requestBody.get("sourcePort")));
                } catch (Exception ignored) {}
            }
            if (requestBody.containsKey("peers")) {
                try {
                    targetPeers = objectMapper.convertValue(
                            requestBody.get("peers"),
                            new TypeReference<List<PeerEndpoint>>() {}
                    );
                } catch (Exception ignored) {}
            }
        }

        if (targetFileId == null || targetFileId.isBlank()) {
            throw new IllegalArgumentException("fileId must be provided");
        }
        if (targetFileName == null || targetFileName.isBlank()) {
            targetFileName = "download-" + targetFileId + ".bin";
        }
        if (targetTotalChunks == null || targetTotalChunks <= 0) {
            targetTotalChunks = 1;
        }
        if (targetSourceHost == null || targetSourceHost.isBlank()) {
            targetSourceHost = "127.0.0.1";
        }
        if (targetSourcePort == null || targetSourcePort <= 0) {
            targetSourcePort = 9001;
        }

        return downloadManagerService.startDownload(
                targetFileId,
                targetFileName,
                targetFileSize,
                targetFileHash,
                targetTotalChunks,
                targetSourceHost,
                targetSourcePort,
                targetPeers
        );
    }

    // Poll download status and live zero-chunking progress (bytesDownloaded, speedBytesPerSec, activeLanes)
    @GetMapping("/download/status")
    public DownloadManagerService.DownloadJob getDownloadStatus(@RequestParam String fileId) {
        return downloadManagerService.getJob(fileId);
    }

    // List all active and completed downloads with live progress
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
                if (job.getDestinationPath() != null) {
                    File dest = new File(job.getDestinationPath());
                    if (dest.exists() && dest.isFile()) {
                        return createResourceResponse(dest, fileName != null ? fileName : jobFileName);
                    }
                }
            }
        }

        File file = chunkManagerService.resolveFileForDownload(fileId, fileName, jobFileName, jobTotalChunks);
        if (file == null || !file.exists() || !file.isFile()) {
            throw new FileNotFoundException("Requested file was not found on node storage.");
        }

        String downloadName = (fileName != null && !fileName.isBlank())
                ? fileName
                : ((jobFileName != null && !jobFileName.isBlank()) ? jobFileName : file.getName());

        return createResourceResponse(file, downloadName);
    }

    private ResponseEntity<Resource> createResourceResponse(File file, String downloadName) throws IOException {
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
        List<Integer> dropped = new ArrayList<>();

        for (String cStr : chunkNos.split(",")) {
            try {
                int c = Integer.parseInt(cStr.trim());
                Path chunkPath = chunksDir.resolve(c + ".chunk");
                Path backupChunk = backupDir.resolve(c + ".chunk");
                Path destSim = simulatedDir.resolve(c + ".chunk");

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

        SharedFile shared = chunkManagerService.getAllSharedFiles().stream()
                .filter(f -> f.getFileId().equals(fileId))
                .findFirst().orElse(null);
        if (shared != null) {
            Path reconstructed = Paths.get("peer-data", "reconstructed", shared.getName()).toAbsolutePath();
            Files.deleteIfExists(reconstructed);
        }

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
        List<String> restored = new ArrayList<>();

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

    // Ping a remote peer over TCP to verify network connectivity
    @GetMapping("/peers/ping")
    public Map<String, Object> pingPeer(@RequestParam String host,
                                        @RequestParam int port,
                                        @RequestParam(defaultValue = "test-client") String peerId) throws IOException {
        modell.NetworkMessage ping = new modell.NetworkMessage("HELLO", peerId, "Ping handshake");
        String rawResponse = networkService.sendDirectMessage(host, port, ping);
        return Map.of(
                "status", "PONG",
                "targetHost", host,
                "targetPort", port,
                "rawTcpResponseReceived", rawResponse
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
