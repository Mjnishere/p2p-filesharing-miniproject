package com.example.demo.controller;

import com.example.demo.service.ChunkManagerService;
import com.example.demo.service.DownloadManagerService;
import com.example.demo.service.SearchManagerService;
import modell.PeerInfo;
import modell.SearchResult;
import modell.SharedFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
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
}
