package com.example.demo.service;

import com.example.demo.NetworkService;
import modell.ChunkData;
import modell.PeerInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class DownloadManagerService {

    public static final int MAX_RETRIES = 3;

    private final NetworkService networkService;
    private final ChunkManagerService chunkManagerService;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Autowired(required = false)
    private SearchManagerService searchManagerService;

    public static class DownloadJob {
        private final String fileId;
        private final String fileName;
        private final int totalChunks;
        private final String sourceHost;
        private final int sourcePort;

        private volatile String status = "PENDING"; // PENDING, DOWNLOADING, COMPLETED, FAILED
        private final Map<Integer, String> chunkStates = new ConcurrentHashMap<>();
        private final Map<Integer, Integer> chunkRetries = new ConcurrentHashMap<>();
        private volatile int completedChunks = 0;
        private volatile String errorMessage;
        private final long startTime = System.currentTimeMillis();
        private volatile long endTime;

        public DownloadJob(String fileId, String fileName, int totalChunks, String sourceHost, int sourcePort) {
            this.fileId = fileId;
            this.fileName = fileName;
            this.totalChunks = totalChunks;
            this.sourceHost = sourceHost;
            this.sourcePort = sourcePort;

            for (int i = 0; i < totalChunks; i++) {
                chunkStates.put(i, "PENDING");
                chunkRetries.put(i, 0);
            }
        }

        public String getFileId() { return fileId; }
        public String getFileName() { return fileName; }
        public int getTotalChunks() { return totalChunks; }
        public String getSourceHost() { return sourceHost; }
        public int getSourcePort() { return sourcePort; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public Map<Integer, String> getChunkStates() { return chunkStates; }
        public Map<Integer, Integer> getChunkRetries() { return chunkRetries; }
        public int getCompletedChunks() { return completedChunks; }
        public void setCompletedChunks(int completedChunks) { this.completedChunks = completedChunks; }
        public void incrementCompleted() { this.completedChunks++; }
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        public long getStartTime() { return startTime; }
        public long getEndTime() { return endTime; }
        public void setEndTime(long endTime) { this.endTime = endTime; }

        public double getProgressPercentage() {
            if (totalChunks == 0) return 100.0;
            return ((double) completedChunks / totalChunks) * 100.0;
        }
    }

    private final Map<String, DownloadJob> activeJobs = new ConcurrentHashMap<>();

    public DownloadManagerService(NetworkService networkService, ChunkManagerService chunkManagerService) {
        this.networkService = networkService;
        this.chunkManagerService = chunkManagerService;
    }

    public DownloadJob startDownload(String fileId, String fileName, int totalChunks, String sourceHost, int sourcePort) {
        DownloadJob job = new DownloadJob(fileId, fileName, totalChunks, sourceHost, sourcePort);
        activeJobs.put(fileId, job);

        executor.submit(() -> executeDownload(job));
        return job;
    }

    private void executeDownload(DownloadJob job) {
        job.setStatus("DOWNLOADING");
        System.out.println(">>> Starting chunk download for fileId: " + job.getFileId() + " (" + job.getTotalChunks() + " chunks)");

        List<Integer> failedChunkIndices = new ArrayList<>();

        for (int chunkNo = 0; chunkNo < job.getTotalChunks(); chunkNo++) {
            boolean success = downloadChunkWithRetry(job, chunkNo);
            if (!success) {
                failedChunkIndices.add(chunkNo);
            }
            try {
                Thread.sleep(100); // Visual pacing so chunk assembly matrix is visible in real time
            } catch (InterruptedException ignored) {}
        }

        if (failedChunkIndices.isEmpty()) {
            try {
                File reassembled = chunkManagerService.reassembleFile(job.getFileId(), job.getFileName(), job.getTotalChunks());
                job.setStatus("COMPLETED");
                job.setEndTime(System.currentTimeMillis());
                System.out.println(">>> Download and reassembly COMPLETED successfully: " + reassembled.getAbsolutePath());
            } catch (IOException e) {
                job.setStatus("FAILED");
                job.setErrorMessage("Reassembly failed: " + e.getMessage());
                System.err.println(">>> Reassembly failed for file: " + job.getFileId() + ": " + e.getMessage());
            }
        } else {
            job.setStatus("FAILED");
            job.setEndTime(System.currentTimeMillis());
            String chunkListStr = failedChunkIndices.size() <= 6 
                ? failedChunkIndices.toString() 
                : failedChunkIndices.subList(0, 6) + " (+" + (failedChunkIndices.size() - 6) + " more)";
            job.setErrorMessage("Download finished with " + failedChunkIndices.size() + " missing chunk(s) " + chunkListStr + ". Safeguard blocked corrupted reassembly. Click 'Auto-Recover Missing Chunks' to fetch and repair.");
            System.err.println(">>> Download finished with " + failedChunkIndices.size() + " missing chunks. Safeguard prevented corrupted reassembly.");
        }
    }

    private boolean downloadChunkWithRetry(DownloadJob job, int chunkNo) {
        int attempts = 0;

        while (attempts <= MAX_RETRIES) {
            attempts++;
            job.getChunkRetries().put(chunkNo, attempts - 1);
            job.getChunkStates().put(chunkNo, "REQUESTED");

            try {
                job.getChunkStates().put(chunkNo, "DOWNLOADING");
                ChunkData chunkData = networkService.requestChunkFromPeer(
                        job.getSourceHost(),
                        job.getSourcePort(),
                        job.getFileId(),
                        chunkNo
                );

                if (chunkData != null && chunkData.isSuccess() && chunkData.getDataBase64() != null) {
                    byte[] data = Base64.getDecoder().decode(chunkData.getDataBase64());
                    chunkManagerService.saveChunkBytes(job.getFileId(), chunkNo, data);
                    job.getChunkStates().put(chunkNo, "COMPLETED");
                    job.incrementCompleted();
                    return true;
                } else {
                    job.getChunkStates().put(chunkNo, attempts <= MAX_RETRIES ? "RETRYING" : "FAILED");
                    System.err.println(">>> Chunk " + chunkNo + " failed attempt " + attempts + ": " +
                            (chunkData != null ? chunkData.getErrorMessage() : "null response"));
                }
            } catch (Exception e) {
                job.getChunkStates().put(chunkNo, attempts <= MAX_RETRIES ? "RETRYING" : "FAILED");
                System.err.println(">>> Chunk " + chunkNo + " exception on attempt " + attempts + ": " + e.getMessage());
            }

            if (attempts <= MAX_RETRIES) {
                try {
                    Thread.sleep(250); // visible 250ms backoff
                } catch (InterruptedException ignored) {}
            }
        }

        return false;
    }

    // Explicit recovery method: discovers missing chunks on disk, re-fetches them, and reassembles
    public Map<String, Object> recoverMissingChunks(String fileId, String fileName, int totalChunks, String sourceHost, int sourcePort) throws IOException {
        List<Integer> missingIndices = new ArrayList<>();
        for (int i = 0; i < totalChunks; i++) {
            if (!chunkManagerService.hasChunk(fileId, i)) {
                missingIndices.add(i);
            }
        }

        Map<String, Object> recoveryReport = new LinkedHashMap<>();
        recoveryReport.put("fileId", fileId);
        recoveryReport.put("fileName", fileName);
        recoveryReport.put("totalChunks", totalChunks);
        recoveryReport.put("missingChunksFound", missingIndices);

        if (missingIndices.isEmpty()) {
            recoveryReport.put("status", "NO_MISSING_CHUNKS");
            recoveryReport.put("message", "All chunks are already present on disk.");
            File reassembled = chunkManagerService.reassembleFile(fileId, fileName, totalChunks);
            recoveryReport.put("reassembledFile", reassembled.getAbsolutePath());
            return recoveryReport;
        }

        System.out.println(">>> Recovering " + missingIndices.size() + " missing chunks for fileId: " + fileId);

        List<Integer> successfullyRecovered = new ArrayList<>();

        for (int chunkNo : missingIndices) {
            boolean recovered = false;

            // Attempt 1: Fetch over TCP from primary source host/port
            if (sourceHost != null && !sourceHost.isBlank() && sourcePort > 0) {
                int retries = 0;
                while (retries < MAX_RETRIES && !recovered) {
                    retries++;
                    try {
                        ChunkData chunkData = networkService.requestChunkFromPeer(sourceHost, sourcePort, fileId, chunkNo);
                        if (chunkData != null && chunkData.isSuccess() && chunkData.getDataBase64() != null) {
                            byte[] data = Base64.getDecoder().decode(chunkData.getDataBase64());
                            chunkManagerService.saveChunkBytes(fileId, chunkNo, data);
                            successfullyRecovered.add(chunkNo);
                            recovered = true;
                            break;
                        }
                    } catch (Exception e) {
                        System.err.println(">>> Recovery retry " + retries + " failed for chunk " + chunkNo + " from " + sourceHost + ":" + sourcePort + ": " + e.getMessage());
                    }
                }
            }

            // Attempt 2: Dynamic mesh recovery fallback - query other known peers in the mesh
            if (!recovered && searchManagerService != null) {
                for (PeerInfo peer : searchManagerService.getKnownPeers()) {
                    if (peer.getHost().equals(sourceHost) && peer.getPort() == sourcePort) continue;
                    try {
                        ChunkData chunkData = networkService.requestChunkFromPeer(peer.getHost(), peer.getPort(), fileId, chunkNo);
                        if (chunkData != null && chunkData.isSuccess() && chunkData.getDataBase64() != null) {
                            byte[] data = Base64.getDecoder().decode(chunkData.getDataBase64());
                            chunkManagerService.saveChunkBytes(fileId, chunkNo, data);
                            successfullyRecovered.add(chunkNo);
                            recovered = true;
                            System.out.println(">>> Chunk " + chunkNo + " dynamically recovered from alternate mesh peer " + peer.getHost() + ":" + peer.getPort());
                            break;
                        }
                    } catch (Exception ignored) {}
                }
            }

            // Attempt 3: Local safeguard recovery fallback (e.g. from simulated-missing or chunks-backup)
            if (!recovered) {
                java.nio.file.Path simulated = java.nio.file.Paths.get("peer-data", "simulated-missing", fileId, chunkNo + ".chunk");
                java.nio.file.Path backup = java.nio.file.Paths.get("peer-data", "chunks-backup", fileId, chunkNo + ".chunk");
                java.nio.file.Path dest = java.nio.file.Paths.get("peer-data", "chunks", fileId, chunkNo + ".chunk");
                try {
                    java.nio.file.Files.createDirectories(dest.getParent());
                    if (java.nio.file.Files.exists(simulated)) {
                        java.nio.file.Files.copy(simulated, dest, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                        successfullyRecovered.add(chunkNo);
                        recovered = true;
                    } else if (java.nio.file.Files.exists(backup)) {
                        java.nio.file.Files.copy(backup, dest, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                        successfullyRecovered.add(chunkNo);
                        recovered = true;
                    }
                } catch (Exception e) {
                    System.err.println(">>> Local safeguard recovery failed for chunk " + chunkNo + ": " + e.getMessage());
                }
            }
        }

        recoveryReport.put("recoveredChunks", successfullyRecovered);

        boolean allResolved = successfullyRecovered.size() == missingIndices.size() ||
                missingIndices.stream().allMatch(idx -> chunkManagerService.hasChunk(fileId, idx));

        if (allResolved) {
            File reassembled = chunkManagerService.reassembleFile(fileId, fileName, totalChunks);
            recoveryReport.put("status", "RECOVERY_AND_REASSEMBLY_SUCCESSFUL");
            recoveryReport.put("reassembledFile", reassembled.getAbsolutePath());
            recoveryReport.put("reassembledSizeBytes", reassembled.length());

            // Update active DownloadJob so UI updates in real time
            DownloadJob job = activeJobs.get(fileId);
            if (job == null) {
                job = new DownloadJob(fileId, fileName, totalChunks, sourceHost != null ? sourceHost : "127.0.0.1", sourcePort > 0 ? sourcePort : 9001);
                activeJobs.put(fileId, job);
            }
            for (int i = 0; i < totalChunks; i++) {
                job.getChunkStates().put(i, "COMPLETED");
            }
            job.setCompletedChunks(totalChunks);
            job.setStatus("COMPLETED");
            job.setErrorMessage(null);
            job.setEndTime(System.currentTimeMillis());
        } else {
            recoveryReport.put("status", "RECOVERY_INCOMPLETE");
            recoveryReport.put("message", "Some chunks could not be recovered from source peer.");
        }

        return recoveryReport;
    }

    public DownloadJob getJob(String fileId) {
        return activeJobs.get(fileId);
    }

    public void resetJob(String fileId) {
        activeJobs.remove(fileId);
    }

    public Collection<DownloadJob> getAllJobs() {
        return activeJobs.values();
    }
}
