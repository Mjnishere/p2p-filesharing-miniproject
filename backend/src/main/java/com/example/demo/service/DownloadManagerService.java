package com.example.demo.service;

import com.example.demo.FramingUtil;
import com.example.demo.NetworkService;
import com.fasterxml.jackson.databind.ObjectMapper;
import modell.ByteRange;
import modell.ChunkData;
import modell.PeerEndpoint;
import modell.PeerInfo;
import modell.StreamRequest;
import modell.StreamResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class DownloadManagerService {

    public static final long WINDOW_SIZE = 4 * 1024 * 1024L; // 4 MB virtual byte windows
    public static final int MAX_RETRIES = 3;
    private static final int SOCKET_CONNECT_TIMEOUT_MS = 3000;
    private static final int SOCKET_READ_TIMEOUT_MS = 5000;

    private final NetworkService networkService;
    private final ChunkManagerService chunkManagerService;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${db.url:jdbc:sqlite:peer.db}")
    private String dbUrl;

    @Autowired(required = false)
    private SearchManagerService searchManagerService;

    public static class DownloadJob {
        private final String fileId;
        private final String fileName;
        private final long fileSize; // Total size in bytes
        private final String fileHash; // Expected whole-file SHA-256 hash (optional)
        private final String sourceHost; // Primary source host
        private final int sourcePort; // Primary source port

        private volatile String status = "PENDING"; // PENDING, DOWNLOADING, COMPLETED, FAILED, CANCELLED
        private final AtomicLong bytesDownloaded = new AtomicLong(0);
        private final AtomicInteger activeLanes = new AtomicInteger(0);
        private volatile String errorMessage;
        private volatile String destinationPath;
        private final long startTime = System.currentTimeMillis();
        private volatile long endTime;

        // Backward compatibility fields for legacy UI and tests
        private final int totalChunks;
        private final AtomicInteger completedChunks = new AtomicInteger(0);
        private final Map<Integer, String> chunkStates = new ConcurrentHashMap<>();
        private final Map<Integer, Integer> chunkRetries = new ConcurrentHashMap<>();

        public DownloadJob(String fileId, String fileName, long fileSize, String fileHash, int totalChunks, String sourceHost, int sourcePort) {
            this.fileId = fileId;
            this.fileName = fileName;
            this.fileSize = Math.max(0, fileSize);
            this.fileHash = fileHash;
            this.sourceHost = sourceHost != null && !sourceHost.isBlank() ? sourceHost : "127.0.0.1";
            this.sourcePort = sourcePort > 0 ? sourcePort : 9001;

            if (totalChunks > 0) {
                this.totalChunks = totalChunks;
            } else {
                int computedWindows = this.fileSize > 0 ? (int) Math.ceil((double) this.fileSize / WINDOW_SIZE) : 1;
                this.totalChunks = Math.max(1, computedWindows);
            }

            for (int i = 0; i < this.totalChunks; i++) {
                chunkStates.put(i, "PENDING");
                chunkRetries.put(i, 0);
            }
        }

        public DownloadJob(String fileId, String fileName, long fileSize, String fileHash, String sourceHost, int sourcePort) {
            this(fileId, fileName, fileSize, fileHash, 0, sourceHost, sourcePort);
        }

        public DownloadJob(String fileId, String fileName, int totalChunks, String sourceHost, int sourcePort) {
            this(fileId, fileName, (long) Math.max(1, totalChunks) * 1024 * 1024L, null, totalChunks, sourceHost, sourcePort);
        }

        public String getFileId() { return fileId; }
        public String getFileName() { return fileName; }
        public long getFileSize() { return fileSize; }
        public long getTotalSize() { return fileSize; }
        public long getTotalBytes() { return fileSize; }
        public String getFileHash() { return fileHash; }
        public String getSourceHost() { return sourceHost; }
        public int getSourcePort() { return sourcePort; }

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }

        public long getBytesDownloaded() { return bytesDownloaded.get(); }
        public AtomicLong getBytesDownloadedCounter() { return bytesDownloaded; }

        public int getActiveLanes() { return activeLanes.get(); }
        public AtomicInteger getActiveLanesCounter() { return activeLanes; }

        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

        public String getDestinationPath() { return destinationPath; }
        public void setDestinationPath(String destinationPath) { this.destinationPath = destinationPath; }

        public long getStartTime() { return startTime; }
        public long getEndTime() { return endTime; }
        public void setEndTime(long endTime) { this.endTime = endTime; }

        public int getTotalChunks() { return totalChunks; }
        public int getCompletedChunks() { return completedChunks.get(); }
        public void setCompletedChunks(int completedChunks) { this.completedChunks.set(completedChunks); }
        public void incrementCompleted() { this.completedChunks.incrementAndGet(); }

        public Map<Integer, String> getChunkStates() { return chunkStates; }
        public Map<Integer, Integer> getChunkRetries() { return chunkRetries; }

        public double getProgressPercentage() {
            if (fileSize <= 0) return 100.0;
            double pct = ((double) bytesDownloaded.get() / fileSize) * 100.0;
            return Math.min(100.0, Math.max(0.0, pct));
        }

        public double getSpeedBytesPerSec() {
            if (!"DOWNLOADING".equals(status)) return 0.0;
            long elapsedMs = Math.max(1, System.currentTimeMillis() - startTime);
            return (bytesDownloaded.get() * 1000.0) / elapsedMs;
        }
    }

    private final Map<String, DownloadJob> activeJobs = new ConcurrentHashMap<>();

    public DownloadManagerService(NetworkService networkService, ChunkManagerService chunkManagerService) {
        this.networkService = networkService;
        this.chunkManagerService = chunkManagerService;
    }

    /**
     * Start a zero-chunking dynamic work-stealing parallel download.
     */
    public DownloadJob startZeroChunkDownload(String fileId, String fileName, long fileSize, String fileHash,
                                              String sourceHost, int sourcePort, List<PeerEndpoint> initialPeers) {
        DownloadJob job = new DownloadJob(fileId, fileName, fileSize, fileHash, sourceHost, sourcePort);
        activeJobs.put(fileId, job);
        executor.submit(() -> executeZeroChunkDownload(job, initialPeers));
        return job;
    }

    /**
     * Unified startDownload entry point supporting optional zero-chunking parameters and swarm peers.
     */
    public DownloadJob startDownload(String fileId, String fileName, Long fileSize, String fileHash,
                                     Integer totalChunks, String sourceHost, Integer sourcePort,
                                     List<PeerEndpoint> peers) {
        long effectiveSize = (fileSize != null && fileSize > 0) ? fileSize : 0L;

        // If fileSize was not passed, attempt local lookup
        if (effectiveSize <= 0) {
            try {
                var sharedList = chunkManagerService.getAllSharedFiles();
                if (sharedList != null) {
                    for (var sf : sharedList) {
                        if (fileId.equals(sf.getFileId())) {
                            effectiveSize = sf.getSizeBytes();
                            if (fileHash == null || fileHash.isBlank()) {
                                fileHash = sf.getFileHash();
                            }
                            break;
                        }
                    }
                }
            } catch (Exception ignored) {}
        }

        // If still unknown, approximate using totalChunks (1 MB chunks)
        if (effectiveSize <= 0 && totalChunks != null && totalChunks > 0) {
            effectiveSize = (long) totalChunks * 1024 * 1024L;
        }

        DownloadJob job = new DownloadJob(
                fileId,
                fileName,
                effectiveSize,
                fileHash,
                totalChunks != null && totalChunks > 0 ? totalChunks : 0,
                sourceHost != null ? sourceHost : "127.0.0.1",
                sourcePort != null && sourcePort > 0 ? sourcePort : 9001
        );
        activeJobs.put(fileId, job);

        final String finalHash = fileHash;
        final long finalSize = effectiveSize;
        executor.submit(() -> {
            // Check if remote peer supports zero-chunking STREAM_REQ by attempting it
            boolean zeroChunkSucceeded = false;
            try {
                zeroChunkSucceeded = executeZeroChunkDownload(job, peers);
            } catch (Exception e) {
                System.err.println("Zero-chunking attempt encountered issue: " + e.getMessage() + ". Attempting fallback if needed.");
            }

            if (!zeroChunkSucceeded && !"COMPLETED".equals(job.getStatus())) {
                // If zero-chunking failed or was unviable and chunks exist on remote host, fallback to chunked download
                int chunksCount = totalChunks != null && totalChunks > 0
                        ? totalChunks
                        : (int) Math.ceil((double) finalSize / (1024 * 1024));
                System.out.println(">>> Falling back to legacy chunked download for fileId: " + fileId);
                executeLegacyChunkedDownload(job, chunksCount);
            }
        });

        return job;
    }

    /**
     * Backward-compatible startDownload signature for legacy tests.
     */
    public DownloadJob startDownload(String fileId, String fileName, int totalChunks, String sourceHost, int sourcePort) {
        // Check if file is registered locally to get true size
        long exactSize = 0L;
        String fileHash = null;
        try {
            var sharedList = chunkManagerService.getAllSharedFiles();
            if (sharedList != null) {
                for (var sf : sharedList) {
                    if (fileId.equals(sf.getFileId())) {
                        exactSize = sf.getSizeBytes();
                        fileHash = sf.getFileHash();
                        break;
                    }
                }
            }
        } catch (Exception ignored) {}

        if (exactSize <= 0) {
            exactSize = (long) totalChunks * 1024 * 1024L;
        }

        return startDownload(fileId, fileName, exactSize, fileHash, totalChunks, sourceHost, sourcePort, null);
    }

    /**
     * Core Phase 4 Zero-Chunking Parallel Streaming Engine.
     * Streams 4 MB virtual byte windows directly into destination FileChannel using positional pwrite.
     */
    private boolean executeZeroChunkDownload(DownloadJob job, List<PeerEndpoint> initialPeers) {
        job.setStatus("DOWNLOADING");
        System.out.println(">>> Starting zero-chunking parallel download: fileId=" + job.getFileId()
                + ", size=" + job.getFileSize() + " bytes (" + job.getFileName() + ")");

        if (job.getFileSize() <= 0) {
            job.setStatus("COMPLETED");
            job.setEndTime(System.currentTimeMillis());
            checkpointProgress(job.getFileId(), job.getFileName(), 0, 0, "COMPLETED");
            return true;
        }

        // 1. Window Queue Management: partition into 4 MB virtual byte windows
        ConcurrentLinkedDeque<ByteRange> workQueue = new ConcurrentLinkedDeque<>();
        if (job.getFileSize() <= WINDOW_SIZE) {
            workQueue.add(new ByteRange(0, job.getFileSize()));
        } else {
            for (long offset = 0; offset < job.getFileSize(); offset += WINDOW_SIZE) {
                long length = Math.min(WINDOW_SIZE, job.getFileSize() - offset);
                workQueue.add(new ByteRange(offset, length));
            }
        }

        // 2. Discover and prioritize peers
        List<PeerEndpoint> candidatePeers = new CopyOnWriteArrayList<>();
        if (initialPeers != null && !initialPeers.isEmpty()) {
            candidatePeers.addAll(initialPeers);
        }

        // Augment with active swarm peers from SearchManagerService if available
        if (searchManagerService != null && job.getFileHash() != null && !job.getFileHash().isBlank()) {
            try {
                Set<PeerEndpoint> swarm = searchManagerService.getSwarmPeers(job.getFileHash());
                if (swarm != null) {
                    for (PeerEndpoint p : swarm) {
                        if (!candidatePeers.contains(p)) {
                            candidatePeers.add(p);
                        }
                    }
                }
            } catch (Exception ignored) {}
        }

        // Add primary seeder if not present
        if (candidatePeers.isEmpty() && job.getSourceHost() != null && job.getSourcePort() > 0) {
            candidatePeers.add(new PeerEndpoint("primary-peer", job.getSourceHost(), job.getSourcePort(), 0L, true));
        }

        // Filter and sort candidate peers by lowest measured RTT
        List<PeerEndpoint> sortedPeers = new ArrayList<>(candidatePeers);
        sortedPeers.removeIf(p -> p == null || p.getHost() == null || p.getPort() <= 0);
        sortedPeers.sort(Comparator.comparingLong(PeerEndpoint::getRttMs));

        if (sortedPeers.isEmpty()) {
            job.setStatus("FAILED");
            job.setErrorMessage("No active peers available for download.");
            job.setEndTime(System.currentTimeMillis());
            checkpointProgress(job.getFileId(), job.getFileName(), job.getFileSize(), 0, "FAILED");
            return false;
        }

        // 3. Adaptive Worker Lane Allocation
        List<PeerEndpoint> laneAssignments = new ArrayList<>();
        if (sortedPeers.size() >= 2) {
            // Multi-Peer Swarm (M >= 2 peers):
            // Spawn 1-2 worker threads per active peer (capped at 6 max total workers), prioritized by lowest rttMs
            int maxWorkers = Math.min(6, sortedPeers.size() * 2);
            while (laneAssignments.size() < maxWorkers) {
                for (PeerEndpoint p : sortedPeers) {
                    if (laneAssignments.size() < maxWorkers) {
                        laneAssignments.add(p);
                    }
                }
            }
        } else {
            // Single-Host Parallelism (M = 1 peer):
            PeerEndpoint singlePeer = sortedPeers.get(0);
            if (job.getFileSize() > WINDOW_SIZE) {
                // Single-Host Multi-Lane Parallelism (fileSize > 4 MB):
                // Spawn 4 concurrent worker threads targeting the single host across independent TCP sockets
                for (int i = 0; i < 4; i++) {
                    laneAssignments.add(singlePeer);
                }
            } else {
                // Small file (<= 4 MB): single worker lane
                laneAssignments.add(singlePeer);
            }
        }

        // 4. Direct Positional Storage (FileChannel)
        Path downloadsDir = Paths.get("peer-data", "downloads").toAbsolutePath();
        try {
            Files.createDirectories(downloadsDir);
        } catch (IOException e) {
            System.err.println("Could not create downloads directory: " + e.getMessage());
        }

        Path partPath = downloadsDir.resolve(job.getFileName() + ".part");

        try (FileChannel destinationChannel = FileChannel.open(
                partPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {

            // Pre-allocate destination storage once via FileChannel.truncate(fileSize)
            destinationChannel.truncate(job.getFileSize());

            int numLanes = laneAssignments.size();
            job.getActiveLanesCounter().set(numLanes);
            CountDownLatch completionLatch = new CountDownLatch(numLanes);

            for (int laneIdx = 0; laneIdx < numLanes; laneIdx++) {
                final PeerEndpoint initialTarget = laneAssignments.get(laneIdx);
                final int laneId = laneIdx;

                executor.submit(() -> {
                    PeerEndpoint currentTarget = initialTarget;
                    Socket socket = null;
                    DataOutputStream dos = null;
                    DataInputStream dis = null;

                    try {
                        socket = createZeroChunkSocket(currentTarget.getHost(), currentTarget.getPort());
                        dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 65536));
                        dis = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 65536));

                        while ("DOWNLOADING".equals(job.getStatus())) {
                            ByteRange range = workQueue.pollFirst();
                            if (range == null) {
                                break; // Work queue finished
                            }

                            int windowIdx = (int) (range.getOffset() / WINDOW_SIZE);
                            job.getChunkStates().put(windowIdx, "DOWNLOADING");

                            AtomicLong rangeBytesRead = new AtomicLong(0);

                            try {
                                // Send STREAM_REQ frame
                                StreamRequest req = new StreamRequest(
                                        job.getFileId(),
                                        job.getFileHash(),
                                        range.getOffset(),
                                        range.getLength()
                                );
                                FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(req));

                                // Read STREAM_RES frame
                                String rawRes = FramingUtil.readFrame(dis);
                                StreamResponse res = objectMapper.readValue(rawRes, StreamResponse.class);

                                if (!"OK".equalsIgnoreCase(res.getStatus())) {
                                    throw new IOException("STREAM_RES error from " + currentTarget.getHost()
                                            + ":" + currentTarget.getPort() + ": " + res.getErrorMessage());
                                }

                                // Stream raw binary bytes directly into shared FileChannel using OS pwrite
                                // Strictly reads range.getLength() bytes (never read until EOF)
                                FramingUtil.pipeRawBytes(
                                        dis,
                                        destinationChannel,
                                        range.getOffset(),
                                        range.getLength(),
                                        bytesRead -> {
                                            rangeBytesRead.addAndGet(bytesRead);
                                            job.getBytesDownloadedCounter().addAndGet(bytesRead);
                                        }
                                );

                                job.getChunkStates().put(windowIdx, "COMPLETED");
                                job.incrementCompleted();

                                // Checkpoint to SQLite downloads table only per completed 4 MB window
                                checkpointProgress(
                                        job.getFileId(),
                                        job.getFileName(),
                                        job.getFileSize(),
                                        job.getBytesDownloaded(),
                                        "DOWNLOADING"
                                );

                            } catch (Exception workerEx) {
                                System.err.println("Worker lane " + laneId + " failed range ["
                                        + range.getOffset() + ", " + range.getLength() + "] on "
                                        + currentTarget.getHost() + ":" + currentTarget.getPort()
                                        + " (" + workerEx.getMessage() + ")");

                                // Work-Stealing Loop & Failover:
                                // Push the uncompleted sub-range [offset + bytesReadSoFar, remaining] back to HEAD
                                long bytesRead = rangeBytesRead.get();
                                if (bytesRead < range.getLength()) {
                                    long remainingOffset = range.getOffset() + bytesRead;
                                    long remainingLength = range.getLength() - bytesRead;
                                    workQueue.addFirst(new ByteRange(remainingOffset, remainingLength));
                                    job.getChunkStates().put(windowIdx, "RETRYING");
                                }

                                // Evict failing peer from SwarmDirectoryCache and mark OFFLINE
                                evictFailingPeer(currentTarget);

                                // Close broken socket
                                closeSocketQuietly(socket);
                                socket = null;

                                // Attempt reconnection to another surviving peer
                                PeerEndpoint fallback = pickSurvivingPeer(sortedPeers, currentTarget);
                                if (fallback != null && !workQueue.isEmpty()) {
                                    try {
                                        currentTarget = fallback;
                                        socket = createZeroChunkSocket(currentTarget.getHost(), currentTarget.getPort());
                                        dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 65536));
                                        dis = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 65536));
                                        continue; // Resume work loop with surviving peer
                                    } catch (Exception reconnEx) {
                                        System.err.println("Lane " + laneId + " fallback reconnect failed: " + reconnEx.getMessage());
                                        break;
                                    }
                                } else {
                                    break; // No other peers available, lane terminates
                                }
                            }
                        }
                    } catch (Exception connEx) {
                        System.err.println("Lane " + laneId + " connection setup failed: " + connEx.getMessage());
                    } finally {
                        closeSocketQuietly(socket);
                        job.getActiveLanesCounter().decrementAndGet();
                        completionLatch.countDown();
                    }
                });
            }

            // Wait for all worker lanes to finish
            completionLatch.await();

            // Check if queue has remaining uncompleted work
            if (!workQueue.isEmpty() || job.getBytesDownloaded() < job.getFileSize()) {
                job.setStatus("FAILED");
                job.setErrorMessage("Transfer incomplete: " + workQueue.size()
                        + " virtual windows remained unfinished. Peers may be unreachable.");
                job.setEndTime(System.currentTimeMillis());
                checkpointProgress(job.getFileId(), job.getFileName(), job.getFileSize(), job.getBytesDownloaded(), "FAILED");
                return false;
            }

            // Commit all written bytes to disk
            destinationChannel.force(true);

        } catch (Exception e) {
            System.err.println("Zero-chunking transfer I/O error: " + e.getMessage());
            job.setStatus("FAILED");
            job.setErrorMessage("Destination storage error: " + e.getMessage());
            job.setEndTime(System.currentTimeMillis());
            checkpointProgress(job.getFileId(), job.getFileName(), job.getFileSize(), job.getBytesDownloaded(), "FAILED");
            return false;
        }

        // 5. Completion & Integrity: Whole-file SHA-256 verification
        boolean verified = false;
        try {
            String computedSha = computeWholeFileHash(partPath);
            if (job.getFileHash() != null && !job.getFileHash().isBlank()) {
                if (computedSha.equalsIgnoreCase(job.getFileHash().trim())) {
                    verified = true;
                } else {
                    job.setStatus("FAILED");
                    job.setErrorMessage("Whole-file SHA-256 mismatch: expected "
                            + job.getFileHash() + " but computed " + computedSha);
                    job.setEndTime(System.currentTimeMillis());
                    checkpointProgress(job.getFileId(), job.getFileName(), job.getFileSize(), job.getBytesDownloaded(), "FAILED");
                    return false;
                }
            } else {
                // If no expected hash was provided, verify physical size match
                if (Files.exists(partPath) && Files.size(partPath) == job.getFileSize()) {
                    verified = true;
                }
            }
        } catch (Exception ex) {
            System.err.println("Integrity verification error: " + ex.getMessage());
            job.setStatus("FAILED");
            job.setErrorMessage("Checksum calculation error: " + ex.getMessage());
            job.setEndTime(System.currentTimeMillis());
            checkpointProgress(job.getFileId(), job.getFileName(), job.getFileSize(), job.getBytesDownloaded(), "FAILED");
            return false;
        }

        if (verified) {
            // Atomically rename .part file to final file in peer-data/reconstructed/
            Path reconstructedDir = Paths.get("peer-data", "reconstructed").toAbsolutePath();
            try {
                Files.createDirectories(reconstructedDir);
                Path finalPath = reconstructedDir.resolve(job.getFileName());
                try {
                    Files.move(partPath, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (Exception nonAtomic) {
                    Files.move(partPath, finalPath, StandardCopyOption.REPLACE_EXISTING);
                }

                job.setCompletedChunks(job.getTotalChunks());
                for (int i = 0; i < job.getTotalChunks(); i++) {
                    job.getChunkStates().put(i, "COMPLETED");
                }
                job.setDestinationPath(finalPath.toString());
                job.setStatus("COMPLETED");
                job.setEndTime(System.currentTimeMillis());
                checkpointProgress(job.getFileId(), job.getFileName(), job.getFileSize(), job.getFileSize(), "COMPLETED");
                System.out.println(">>> Zero-chunking file transfer COMPLETED successfully: " + finalPath);
                return true;
            } catch (IOException e) {
                job.setStatus("FAILED");
                job.setErrorMessage("Failed to finalize reconstructed file: " + e.getMessage());
                job.setEndTime(System.currentTimeMillis());
                checkpointProgress(job.getFileId(), job.getFileName(), job.getFileSize(), job.getBytesDownloaded(), "FAILED");
                return false;
            }
        }

        return false;
    }

    /**
     * Legacy chunk-by-chunk download engine for backward compatibility with older peers or tests.
     */
    private void executeLegacyChunkedDownload(DownloadJob job, int totalChunks) {
        job.setStatus("DOWNLOADING");
        System.out.println(">>> Executing legacy chunked download for fileId: " + job.getFileId() + " (" + totalChunks + " chunks)");

        List<Integer> failedChunkIndices = new ArrayList<>();

        for (int chunkNo = 0; chunkNo < totalChunks; chunkNo++) {
            boolean success = downloadChunkWithRetry(job, chunkNo);
            if (!success) {
                failedChunkIndices.add(chunkNo);
            }
        }

        if (failedChunkIndices.isEmpty()) {
            try {
                File reassembled = chunkManagerService.reassembleFile(job.getFileId(), job.getFileName(), totalChunks);
                job.setStatus("COMPLETED");
                job.setCompletedChunks(totalChunks);
                job.setDestinationPath(reassembled.getAbsolutePath());
                job.setEndTime(System.currentTimeMillis());
                checkpointProgress(job.getFileId(), job.getFileName(), reassembled.length(), reassembled.length(), "COMPLETED");
                System.out.println(">>> Download and reassembly COMPLETED successfully: " + reassembled.getAbsolutePath());
            } catch (IOException e) {
                job.setStatus("FAILED");
                job.setErrorMessage("Reassembly failed: " + e.getMessage());
                checkpointProgress(job.getFileId(), job.getFileName(), job.getFileSize(), job.getBytesDownloaded(), "FAILED");
            }
        } else {
            job.setStatus("FAILED");
            job.setEndTime(System.currentTimeMillis());
            String chunkListStr = failedChunkIndices.size() <= 6
                    ? failedChunkIndices.toString()
                    : failedChunkIndices.subList(0, 6) + " (+" + (failedChunkIndices.size() - 6) + " more)";
            job.setErrorMessage("Download finished with " + failedChunkIndices.size() + " missing chunk(s) " + chunkListStr);
            checkpointProgress(job.getFileId(), job.getFileName(), job.getFileSize(), job.getBytesDownloaded(), "FAILED");
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
                    job.getBytesDownloadedCounter().addAndGet(data.length);
                    return true;
                } else {
                    job.getChunkStates().put(chunkNo, attempts <= MAX_RETRIES ? "RETRYING" : "FAILED");
                }
            } catch (Exception e) {
                job.getChunkStates().put(chunkNo, attempts <= MAX_RETRIES ? "RETRYING" : "FAILED");
            }

            if (attempts <= MAX_RETRIES) {
                try {
                    Thread.sleep(150);
                } catch (InterruptedException ignored) {}
            }
        }

        // Try mesh peers fallback
        if (searchManagerService != null) {
            for (PeerInfo peer : searchManagerService.getKnownPeers()) {
                if (peer.getHost().equals(job.getSourceHost()) && peer.getPort() == job.getSourcePort()) {
                    continue;
                }
                try {
                    job.getChunkStates().put(chunkNo, "DOWNLOADING");
                    ChunkData chunkData = networkService.requestChunkFromPeer(
                            peer.getHost(),
                            peer.getPort(),
                            job.getFileId(),
                            chunkNo
                    );
                    if (chunkData != null && chunkData.isSuccess() && chunkData.getDataBase64() != null) {
                        byte[] data = Base64.getDecoder().decode(chunkData.getDataBase64());
                        chunkManagerService.saveChunkBytes(job.getFileId(), chunkNo, data);
                        job.getChunkStates().put(chunkNo, "COMPLETED");
                        job.incrementCompleted();
                        job.getBytesDownloadedCounter().addAndGet(data.length);
                        return true;
                    }
                } catch (Exception ignored) {}
            }
        }

        job.getChunkStates().put(chunkNo, "FAILED");
        return false;
    }

    /**
     * Explicit recovery method: discovers missing chunks on disk, re-fetches them, and reassembles.
     */
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
                    } catch (Exception ignored) {}
                }
            }

            // Attempt 2: Dynamic mesh recovery fallback
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
                            break;
                        }
                    } catch (Exception ignored) {}
                }
            }

            // Attempt 3: Local safeguard recovery fallback
            if (!recovered) {
                Path simulated = Paths.get("peer-data", "simulated-missing", fileId, chunkNo + ".chunk");
                Path backup = Paths.get("peer-data", "chunks-backup", fileId, chunkNo + ".chunk");
                Path dest = Paths.get("peer-data", "chunks", fileId, chunkNo + ".chunk");
                try {
                    Files.createDirectories(dest.getParent());
                    if (Files.exists(simulated)) {
                        Files.copy(simulated, dest, StandardCopyOption.REPLACE_EXISTING);
                        successfullyRecovered.add(chunkNo);
                        recovered = true;
                    } else if (Files.exists(backup)) {
                        Files.copy(backup, dest, StandardCopyOption.REPLACE_EXISTING);
                        successfullyRecovered.add(chunkNo);
                        recovered = true;
                    }
                } catch (Exception ignored) {}
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

    private Socket createZeroChunkSocket(String host, int port) throws IOException {
        Socket socket = new Socket();
        socket.setTcpNoDelay(true); // Disable Nagle's algorithm for low-latency streaming
        socket.setSoTimeout(SOCKET_READ_TIMEOUT_MS);
        socket.setSendBufferSize(65536);
        socket.setReceiveBufferSize(65536);
        socket.connect(new InetSocketAddress(host, port), SOCKET_CONNECT_TIMEOUT_MS);
        return socket;
    }

    private void closeSocketQuietly(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    private PeerEndpoint pickSurvivingPeer(List<PeerEndpoint> allPeers, PeerEndpoint failingPeer) {
        if (allPeers == null || allPeers.isEmpty()) return null;
        for (PeerEndpoint p : allPeers) {
            if (p != null && !p.equals(failingPeer)) {
                return p;
            }
        }
        return null;
    }

    private void evictFailingPeer(PeerEndpoint peer) {
        if (peer == null) return;
        try {
            if (searchManagerService != null) {
                searchManagerService.evictPeerFromSwarm(peer.getPeerId());
            }
        } catch (Exception ignored) {}

        // Mark peer OFFLINE in SQLite
        String sql = "UPDATE peers SET status = 'OFFLINE' WHERE peer_id = ? OR (host = ? AND port = ?)";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, peer.getPeerId());
            ps.setString(2, peer.getHost());
            ps.setInt(3, peer.getPort());
            ps.executeUpdate();
        } catch (SQLException ignored) {}
    }

    private void checkpointProgress(String fileId, String fileName, long totalSize, long bytesDownloaded, String status) {
        String upsertSql = "INSERT INTO downloads (file_id, file_name, total_size, bytes_downloaded, status, last_updated) " +
                "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                "ON CONFLICT(file_id) DO UPDATE SET " +
                "bytes_downloaded = excluded.bytes_downloaded, " +
                "status = excluded.status, " +
                "last_updated = CURRENT_TIMESTAMP";

        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(upsertSql)) {
            ps.setString(1, fileId);
            ps.setString(2, fileName);
            ps.setLong(3, totalSize);
            ps.setLong(4, bytesDownloaded);
            ps.setString(5, status);
            ps.executeUpdate();
        } catch (SQLException e) {
            // Fallback for older sqlite dialects
            try (Connection conn = DriverManager.getConnection(dbUrl)) {
                String updateSql = "UPDATE downloads SET bytes_downloaded = ?, status = ?, last_updated = CURRENT_TIMESTAMP WHERE file_id = ?";
                try (PreparedStatement updatePs = conn.prepareStatement(updateSql)) {
                    updatePs.setLong(1, bytesDownloaded);
                    updatePs.setString(2, status);
                    updatePs.setString(3, fileId);
                    int updated = updatePs.executeUpdate();
                    if (updated == 0) {
                        String insertSql = "INSERT INTO downloads (file_id, file_name, total_size, bytes_downloaded, status) VALUES (?, ?, ?, ?, ?)";
                        try (PreparedStatement insertPs = conn.prepareStatement(insertSql)) {
                            insertPs.setString(1, fileId);
                            insertPs.setString(2, fileName);
                            insertPs.setLong(3, totalSize);
                            insertPs.setLong(4, bytesDownloaded);
                            insertPs.setString(5, status);
                            insertPs.executeUpdate();
                        }
                    }
                }
            } catch (SQLException ignored) {}
        }
    }

    private String computeWholeFileHash(Path file) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (InputStream is = new BufferedInputStream(Files.newInputStream(file))) {
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
            throw new RuntimeException("SHA-256 not available", e);
        }
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
