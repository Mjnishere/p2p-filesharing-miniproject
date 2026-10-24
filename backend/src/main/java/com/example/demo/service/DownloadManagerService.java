package com.example.demo.service;

import com.example.demo.NetworkService;
import modell.ChunkData;
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

        boolean allSucceeded = true;

        for (int chunkNo = 0; chunkNo < job.getTotalChunks(); chunkNo++) {
            boolean success = downloadChunkWithRetry(job, chunkNo);
            if (!success) {
                allSucceeded = false;
                job.setStatus("FAILED");
                job.setErrorMessage("Failed to download chunk " + chunkNo + " after " + MAX_RETRIES + " retries");
                break;
            }
        }

        if (allSucceeded) {
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
                    job.getChunkStates().put(chunkNo, "FAILED");
                    System.err.println(">>> Chunk " + chunkNo + " failed attempt " + attempts + ": " +
                            (chunkData != null ? chunkData.getErrorMessage() : "null response"));
                }
            } catch (Exception e) {
                job.getChunkStates().put(chunkNo, "FAILED");
                System.err.println(">>> Chunk " + chunkNo + " exception on attempt " + attempts + ": " + e.getMessage());
            }

            if (attempts <= MAX_RETRIES) {
                try {
                    Thread.sleep(150); // brief backoff before retry
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
            int retries = 0;
            boolean recovered = false;
            while (retries < MAX_RETRIES && !recovered) {
                retries++;
                try {
                    ChunkData chunkData = networkService.requestChunkFromPeer(sourceHost, sourcePort, fileId, chunkNo);
                    if (chunkData != null && chunkData.isSuccess() && chunkData.getDataBase64() != null) {
                        byte[] data = Base64.getDecoder().decode(chunkData.getDataBase64());
                        chunkManagerService.saveChunkBytes(fileId, chunkNo, data);
                        successfullyRecovered.add(chunkNo);
                        recovered = true;
                    }
                } catch (Exception e) {
                    System.err.println(">>> Recovery retry " + retries + " failed for chunk " + chunkNo + ": " + e.getMessage());
                }
            }
        }

        recoveryReport.put("recoveredChunks", successfullyRecovered);

        if (successfullyRecovered.size() == missingIndices.size()) {
            File reassembled = chunkManagerService.reassembleFile(fileId, fileName, totalChunks);
            recoveryReport.put("status", "RECOVERY_AND_REASSEMBLY_SUCCESSFUL");
            recoveryReport.put("reassembledFile", reassembled.getAbsolutePath());
            recoveryReport.put("reassembledSizeBytes", reassembled.length());
        } else {
            recoveryReport.put("status", "RECOVERY_INCOMPLETE");
            recoveryReport.put("message", "Some chunks could not be recovered from source peer.");
        }

        return recoveryReport;
    }

    public DownloadJob getJob(String fileId) {
        return activeJobs.get(fileId);
    }

    public Collection<DownloadJob> getAllJobs() {
        return activeJobs.values();
    }
}
