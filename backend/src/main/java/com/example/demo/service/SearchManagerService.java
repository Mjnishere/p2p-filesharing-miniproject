package com.example.demo.service;

import com.example.demo.FramingUtil;
import com.example.demo.NetworkService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import modell.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class SearchManagerService {

    private static final int MAX_QUERY_CACHE_SIZE = 500;
    private static final long SLIDING_TTL_MS = 60_000L; // 60 seconds

    @Value("${db.url:jdbc:sqlite:peer.db}")
    private String dbUrl;

    @Value("${peer.id:peer-1}")
    private String myPeerId;

    @Value("${peer.tcp.port:9001}")
    private int myTcpPort;

    @Value("${peer.host:}")
    private String configuredHost;

    @Autowired(required = false)
    private NetworkService networkService;

    @Autowired(required = false)
    private ChunkManagerService chunkManagerService;

    @Autowired(required = false)
    private ErasureCodingRecoveryEngine erasureCodingRecoveryEngine;

    private final ObjectMapper objectMapper = new ObjectMapper();

    // Tier 1: QueryCache (LRU, max 500 entries, 60s sliding TTL)
    private final ConcurrentHashMap<String, CachedQueryResult> queryCache = new ConcurrentHashMap<>();
    private final AtomicLong queryCacheHits = new AtomicLong(0);
    private final AtomicLong queryCacheMisses = new AtomicLong(0);

    // Tier 2: SwarmDirectoryCache (fileHash -> PeerEndpoints sorted by lowest rttMs & highest EWMA bandwidth)
    private final ConcurrentHashMap<String, ConcurrentSkipListSet<PeerEndpoint>> swarmDirectory = new ConcurrentHashMap<>();
    // Secondary Swarm Index by fileId and fileName for instant multi-peer lookup even when fileHash is omitted
    private final ConcurrentHashMap<String, ConcurrentSkipListSet<PeerEndpoint>> swarmByFileKey = new ConcurrentHashMap<>();

    // Zero-SQLite Hot-Path Peer Registry in RAM
    private final ConcurrentHashMap<String, PeerInfo> knownPeersMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PeerInfo> onlinePeers = new ConcurrentHashMap<>();
    private final Set<String> offlinePeers = ConcurrentHashMap.newKeySet();
    private volatile String cachedAdvertisedHost = null;

    // Thread pools for non-blocking parallel distributed searches, heartbeats, auto-discovery, and async DB writes
    private final ExecutorService searchExecutor = Executors.newCachedThreadPool();
    private final ExecutorService dbWriteBehindExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "SearchDbWriteBehind");
        t.setDaemon(true);
        return t;
    });
    private final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "MeshHeartbeatAndAutoDiscovery");
        t.setDaemon(true);
        return t;
    });

    public SearchManagerService() {}

    public SearchManagerService(NetworkService networkService) {
        this.networkService = networkService;
    }

    @PostConstruct
    public void init() {
        ensureDbSchema();
        warmUpPeersFromDb();
        // 15-second heartbeat sweep to probe known peers, update rolling RTT, and evict dead peers
        scheduledExecutor.scheduleAtFixedRate(this::heartbeatSweep, 5, 15, TimeUnit.SECONDS);
        // Automatic zero-config peer discovery: UDP beacon broadcast + local port probing on startup and every 25s
        scheduledExecutor.scheduleWithFixedDelay(this::autoDiscoverySweep, 2, 25, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        scheduledExecutor.shutdownNow();
        searchExecutor.shutdownNow();
        dbWriteBehindExecutor.shutdownNow();
    }

    private void ensureDbSchema() {
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA journal_mode=WAL;");
            stmt.execute("PRAGMA synchronous=NORMAL;");
            stmt.execute("PRAGMA busy_timeout=5000;");
            try { stmt.execute("ALTER TABLE peers ADD COLUMN rtt_ms INTEGER DEFAULT 0"); } catch (SQLException ignored) {}
            try { stmt.execute("ALTER TABLE peers ADD COLUMN share_ready BOOLEAN DEFAULT 1"); } catch (SQLException ignored) {}
            try { stmt.execute("ALTER TABLE peers ADD COLUMN data_ports TEXT"); } catch (SQLException ignored) {}
            try { stmt.execute("ALTER TABLE files ADD COLUMN file_hash TEXT"); } catch (SQLException ignored) {}
        } catch (Exception ignored) {}
    }

    private void warmUpPeersFromDb() {
        String sql = "SELECT peer_id, host, port, status, rtt_ms, share_ready, data_ports FROM peers WHERE peer_id != ?";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, myPeerId != null ? myPeerId : "peer-1");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String pid = rs.getString("peer_id");
                    String h = rs.getString("host");
                    int p = rs.getInt("port");
                    String status = rs.getString("status");
                    long rtt = rs.getLong("rtt_ms");
                    boolean sr = rs.getBoolean("share_ready");
                    List<Integer> dataPorts = parseDataPorts(rs.getString("data_ports"));

                    PeerInfo peer = new PeerInfo(pid, h, p, rtt, sr, dataPorts);
                    if (!isSelf(peer)) {
                        knownPeersMap.put(pid, peer);
                        if ("OFFLINE".equalsIgnoreCase(status)) {
                            offlinePeers.add(pid);
                        } else {
                            onlinePeers.put(pid, peer);
                        }
                    }
                }
            }
        } catch (SQLException ignored) {}
    }

    public void cacheDiscoveredPeerInMemory(PeerInfo peer) {
        if (peer == null || peer.getPeerId() == null || isSelf(peer)) return;
        knownPeersMap.put(peer.getPeerId(), peer);
        onlinePeers.put(peer.getPeerId(), peer);
        offlinePeers.remove(peer.getPeerId());
    }

    /**
     * Background automatic peer discovery sweep: broadcasts UDP beacon and probes local candidate ports
     * so multi-node meshes form with zero manual configuration.
     */
    public void autoDiscoverySweep() {
        try {
            if (networkService != null) {
                networkService.broadcastUdpDiscoveryBeacon();
            }
        } catch (Exception ignored) {}
    }

    // ==========================================
    // Tier 2: SwarmDirectoryCache API
    // ==========================================

    /**
     * Registers a peer endpoint for a specific fileHash (and optional fileId/fileName keys).
     * Endpoints are automatically ordered by lowest rttMs & highest EWMA bandwidth in the ConcurrentSkipListSet.
     */
    public void registerSwarmPeer(String fileHash, PeerEndpoint peer) {
        registerSwarmPeer(fileHash, null, null, peer);
    }

    public void registerSwarmPeer(String fileHash, String fileId, String fileName, PeerEndpoint peer) {
        if (peer == null) return;
        if (fileHash != null && !fileHash.isBlank()) {
            String normalizedHash = fileHash.trim().toLowerCase();
            upsertEndpointIntoSet(swarmDirectory.computeIfAbsent(normalizedHash, k -> new ConcurrentSkipListSet<>()), peer);
        }
        if (fileId != null && !fileId.isBlank()) {
            upsertEndpointIntoSet(swarmByFileKey.computeIfAbsent("id:" + fileId.trim().toLowerCase(), k -> new ConcurrentSkipListSet<>()), peer);
        }
        if (fileName != null && !fileName.isBlank()) {
            upsertEndpointIntoSet(swarmByFileKey.computeIfAbsent("name:" + fileName.trim().toLowerCase(), k -> new ConcurrentSkipListSet<>()), peer);
        }
    }

    private void upsertEndpointIntoSet(ConcurrentSkipListSet<PeerEndpoint> set, PeerEndpoint peer) {
        set.removeIf(p -> (peer.getPeerId() != null && peer.getPeerId().equals(p.getPeerId()))
                || (Objects.equals(peer.getHost(), p.getHost()) && peer.getPort() == p.getPort()));
        set.add(peer);
    }

    /**
     * Retrieves all known swarm peers hosting the given fileHash, ordered by lowest rttMs & highest bandwidth.
     */
    public Set<PeerEndpoint> getSwarmPeers(String fileHash) {
        if (fileHash == null || fileHash.isBlank()) {
            return Collections.emptySet();
        }
        String normalizedHash = fileHash.trim().toLowerCase();
        ConcurrentSkipListSet<PeerEndpoint> set = swarmDirectory.get(normalizedHash);
        if (set == null || set.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(new TreeSet<>(set));
    }

    /**
     * Resolves all online swarm peers for a file across fileHash, fileId, and fileName indices
     * so parallel multi-peer streaming to a single receiver always discovers all available seeders.
     */
    public List<PeerEndpoint> getSwarmPeersForFile(String fileHash, String fileId, String fileName) {
        Map<String, PeerEndpoint> merged = new LinkedHashMap<>();
        if (fileHash != null && !fileHash.isBlank()) {
            for (PeerEndpoint ep : getSwarmPeers(fileHash)) {
                if (!offlinePeers.contains(ep.getPeerId())) {
                    merged.putIfAbsent(ep.getHost() + ":" + ep.getPort(), ep);
                }
            }
        }
        if (fileId != null && !fileId.isBlank()) {
            ConcurrentSkipListSet<PeerEndpoint> byId = swarmByFileKey.get("id:" + fileId.trim().toLowerCase());
            if (byId != null) {
                for (PeerEndpoint ep : byId) {
                    if (!offlinePeers.contains(ep.getPeerId())) {
                        merged.putIfAbsent(ep.getHost() + ":" + ep.getPort(), ep);
                    }
                }
            }
        }
        if (fileName != null && !fileName.isBlank()) {
            ConcurrentSkipListSet<PeerEndpoint> byName = swarmByFileKey.get("name:" + fileName.trim().toLowerCase());
            if (byName != null) {
                for (PeerEndpoint ep : byName) {
                    if (!offlinePeers.contains(ep.getPeerId())) {
                        merged.putIfAbsent(ep.getHost() + ":" + ep.getPort(), ep);
                    }
                }
            }
        }
        List<PeerEndpoint> result = new ArrayList<>(merged.values());
        Collections.sort(result);
        return result;
    }

    /**
     * Evicts a failing or unresponsive peer from all swarms in memory.
     */
    public void evictPeerFromSwarm(String peerId) {
        if (peerId == null || peerId.isBlank()) return;
        offlinePeers.add(peerId);
        onlinePeers.remove(peerId);
        for (ConcurrentSkipListSet<PeerEndpoint> set : swarmDirectory.values()) {
            set.removeIf(p -> Objects.equals(p.getPeerId(), peerId));
        }
        swarmDirectory.entrySet().removeIf(e -> e.getValue().isEmpty());
        for (ConcurrentSkipListSet<PeerEndpoint> set : swarmByFileKey.values()) {
            set.removeIf(p -> Objects.equals(p.getPeerId(), peerId));
        }
        swarmByFileKey.entrySet().removeIf(e -> e.getValue().isEmpty());
    }

    /**
     * Evicts a failing peer endpoint by host and port from all swarms.
     */
    public void evictPeerFromSwarm(String host, int port) {
        if (host == null) return;
        for (ConcurrentSkipListSet<PeerEndpoint> set : swarmDirectory.values()) {
            set.removeIf(p -> Objects.equals(p.getHost(), host) && p.getPort() == port);
        }
        swarmDirectory.entrySet().removeIf(e -> e.getValue().isEmpty());
        for (ConcurrentSkipListSet<PeerEndpoint> set : swarmByFileKey.values()) {
            set.removeIf(p -> Objects.equals(p.getHost(), host) && p.getPort() == port);
        }
        swarmByFileKey.entrySet().removeIf(e -> e.getValue().isEmpty());
    }

    public void updateSwarmPeerMetrics(String peerId, long newRtt, double measuredBandwidthBps) {
        for (ConcurrentSkipListSet<PeerEndpoint> set : swarmDirectory.values()) {
            updatePeerInSkipList(set, peerId, newRtt, measuredBandwidthBps);
        }
        for (ConcurrentSkipListSet<PeerEndpoint> set : swarmByFileKey.values()) {
            updatePeerInSkipList(set, peerId, newRtt, measuredBandwidthBps);
        }
    }

    public void updateSwarmPeerRtt(String peerId, long newRtt) {
        updateSwarmPeerMetrics(peerId, newRtt, 0.0);
    }

    private void updatePeerInSkipList(ConcurrentSkipListSet<PeerEndpoint> set, String peerId, long newRtt, double bwBps) {
        PeerEndpoint target = null;
        for (PeerEndpoint ep : set) {
            if (Objects.equals(ep.getPeerId(), peerId)) {
                target = ep;
                break;
            }
        }
        if (target != null) {
            set.remove(target);
            PeerEndpoint updated = new PeerEndpoint(
                    target.getPeerId(),
                    target.getHost(),
                    target.getPort(),
                    target.getRttMs(),
                    target.isShareReady(),
                    target.getDataPorts()
            );
            updated.setBandwidthBps(target.getBandwidthBps());
            updated.updateEwmaMetrics(newRtt, bwBps);
            set.add(updated);
        }
    }

    // ==========================================
    // Tier 1: QueryCache API & Cache Hit Logic
    // ==========================================

    private void putInQueryCache(String normalizedQuery, List<SearchResult> results) {
        if (normalizedQuery == null) return;
        if (queryCache.size() >= MAX_QUERY_CACHE_SIZE) {
            String oldestKey = null;
            long oldestTime = Long.MAX_VALUE;
            for (Map.Entry<String, CachedQueryResult> entry : queryCache.entrySet()) {
                if (entry.getValue().isExpired(SLIDING_TTL_MS)) {
                    queryCache.remove(entry.getKey());
                } else if (entry.getValue().getLastHitAtMs() < oldestTime) {
                    oldestTime = entry.getValue().getLastHitAtMs();
                    oldestKey = entry.getKey();
                }
            }
            if (queryCache.size() >= MAX_QUERY_CACHE_SIZE && oldestKey != null) {
                queryCache.remove(oldestKey);
            }
        }
        queryCache.put(normalizedQuery, new CachedQueryResult(normalizedQuery, results));
    }

    /**
     * Fast in-memory cache lookup (< 0.05 ms, zero SQLite queries) with sliding TTL verification,
     * online hosting peer validation, live swarm enrichment, and local prefix filtering.
     */
    public List<SearchResult> checkQueryCache(String keyword) {
        if (keyword == null) return null;
        String normalized = keyword.trim().toLowerCase();

        // 1. Direct Cache Hit Check
        CachedQueryResult cached = queryCache.get(normalized);
        if (cached != null) {
            if (cached.isExpired(SLIDING_TTL_MS)) {
                queryCache.remove(normalized);
            } else {
                List<SearchResult> validResults = new ArrayList<>();
                for (SearchResult sr : cached.getResults()) {
                    if (isHostingPeerOnline(sr)) {
                        enrichSearchResultWithLiveSwarm(sr);
                        validResults.add(sr);
                    }
                }
                if (!validResults.isEmpty() || cached.getResults().isEmpty()) {
                    cached.recordHit();
                    queryCacheHits.incrementAndGet();
                    if (chunkManagerService != null) {
                        chunkManagerService.incrementSqliteQueriesSaved();
                    }
                    return validResults;
                }
            }
        }

        // 2. Local Prefix Filtering Check (e.g. if cached "ubuntu" exists, filter in-memory for "ubuntu 24")
        for (Map.Entry<String, CachedQueryResult> entry : queryCache.entrySet()) {
            String cachedKey = entry.getKey();
            if (!cachedKey.isEmpty() && normalized.startsWith(cachedKey) && !normalized.equals(cachedKey)) {
                CachedQueryResult parentCached = entry.getValue();
                if (!parentCached.isExpired(SLIDING_TTL_MS)) {
                    List<SearchResult> filtered = new ArrayList<>();
                    for (SearchResult sr : parentCached.getResults()) {
                        String name = sr.getFileName() != null ? sr.getFileName().toLowerCase() : "";
                        String fid = sr.getFileId() != null ? sr.getFileId().toLowerCase() : "";
                        if (matchesQuery(name, fid, normalized) && isHostingPeerOnline(sr)) {
                            enrichSearchResultWithLiveSwarm(sr);
                            filtered.add(sr);
                        }
                    }
                    if (!filtered.isEmpty()) {
                        parentCached.recordHit();
                        queryCacheHits.incrementAndGet();
                        if (chunkManagerService != null) {
                            chunkManagerService.incrementSqliteQueriesSaved();
                        }
                        putInQueryCache(normalized, filtered);
                        return filtered;
                    }
                }
            }
        }

        return null;
    }

    private void enrichSearchResultWithLiveSwarm(SearchResult sr) {
        if (sr == null) return;
        List<PeerEndpoint> liveSwarm = getSwarmPeersForFile(sr.getFileHash(), sr.getFileId(), sr.getFileName());
        if (!liveSwarm.isEmpty()) {
            sr.setSwarmPeers(liveSwarm);
            PeerEndpoint best = liveSwarm.get(0);
            if (offlinePeers.contains(sr.getSourcePeerId())) {
                sr.setSourcePeerId(best.getPeerId());
                sr.setSourceHost(best.getHost());
                sr.setSourcePort(best.getPort());
                sr.setRttMs(best.getRttMs());
            }
        }
    }

    private boolean matchesQuery(String name, String fid, String normalized) {
        if (name.contains(normalized)) {
            return true;
        }
        String[] tokens = normalized.split("\\s+");
        if (tokens.length > 1) {
            for (String t : tokens) {
                if (!name.contains(t)) {
                    return false;
                }
            }
            return true;
        }
        return fid != null && fid.contains(normalized);
    }

    /**
     * Checks in O(1) RAM time if at least one hosting peer for the SearchResult is online (0 SQLite queries!).
     */
    public boolean isHostingPeerOnline(SearchResult sr) {
        if (sr == null) return false;
        if (myPeerId != null && myPeerId.equals(sr.getSourcePeerId())) {
            return true;
        }
        if (isSelfHostPort(sr.getSourceHost(), sr.getSourcePort())) {
            return true;
        }

        // Check SwarmDirectoryCache for active endpoints hosting this file in O(1) RAM
        String fileHash = sr.getFileHash();
        if (fileHash != null && !fileHash.isBlank()) {
            Set<PeerEndpoint> swarm = getSwarmPeers(fileHash);
            if (!swarm.isEmpty()) {
                for (PeerEndpoint ep : swarm) {
                    if (!offlinePeers.contains(ep.getPeerId())) {
                        return true;
                    }
                }
            }
        }

        String sourcePeerId = sr.getSourcePeerId();
        if (sourcePeerId != null && !sourcePeerId.isBlank()) {
            return !offlinePeers.contains(sourcePeerId);
        }

        return true;
    }

    // ==========================================
    // 15-Second Scheduled Heartbeat Sweep
    // ==========================================

    @Scheduled(fixedRate = 15000, initialDelay = 5000)
    public void heartbeatSweep() {
        List<PeerInfo> peers = getKnownPeers();
        if (peers == null || peers.isEmpty()) return;

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (PeerInfo peer : peers) {
            if (isSelf(peer)) continue;
            futures.add(CompletableFuture.runAsync(() -> probePeer(peer), searchExecutor));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(4000, TimeUnit.MILLISECONDS);
        } catch (Exception ignored) {}
    }

    private void probePeer(PeerInfo peer) {
        long startNs = System.nanoTime();
        boolean success = false;
        long rttMs = 0;

        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(peer.getHost(), peer.getPort()), 1000);
            socket.setSoTimeout(1500);

            try (DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                 DataInputStream dis = new DataInputStream(socket.getInputStream())) {

                NetworkMessage hello = new NetworkMessage("HELLO", myPeerId, "HEARTBEAT");
                hello.setRequestId(UUID.randomUUID().toString());
                FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(hello));
                String rawAck = FramingUtil.readFrame(dis);
                rttMs = Math.max(1L, (System.nanoTime() - startNs) / 1_000_000L);

                NetworkMessage ack = objectMapper.readValue(rawAck, NetworkMessage.class);
                if ("HELLO_ACK".equals(ack.getType())) {
                    success = true;
                }
            }
        } catch (Exception ignored) {
            success = false;
        }

        if (success) {
            long prevRtt = peer.getRttMs();
            long rollingRtt = (prevRtt <= 0) ? rttMs : Math.max(1L, Math.round(0.7 * prevRtt + 0.3 * rttMs));
            peer.setRttMs(rollingRtt);
            peer.setShareReady(true);
            knownPeersMap.put(peer.getPeerId(), peer);
            onlinePeers.put(peer.getPeerId(), peer);
            offlinePeers.remove(peer.getPeerId());
            updateSwarmPeerRtt(peer.getPeerId(), rollingRtt);
            updatePeerStatusInDbAsync(peer.getPeerId(), "CONNECTED", rollingRtt, true);
        } else {
            onlinePeers.remove(peer.getPeerId());
            offlinePeers.add(peer.getPeerId());
            evictPeerFromSwarm(peer.getPeerId());
            updatePeerStatusInDbAsync(peer.getPeerId(), "OFFLINE", -1, false);
        }
    }

    private void updatePeerStatusInDbAsync(String peerId, String status, long rttMs, boolean shareReady) {
        dbWriteBehindExecutor.submit(() -> {
            String sql = "UPDATE peers SET status = ?, rtt_ms = ?, share_ready = ?, last_seen = CURRENT_TIMESTAMP WHERE peer_id = ?";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, status);
                ps.setLong(2, rttMs);
                ps.setInt(3, shareReady ? 1 : 0);
                ps.setString(4, peerId);
                ps.executeUpdate();
            } catch (SQLException ignored) {}
        });
    }

    // ==========================================
    // Distributed Search Execution & Multi-Peer Swarm Aggregation
    // ==========================================

    /**
     * Executes distributed search:
     * 1. Checks QueryCache first (< 0.05 ms in RAM, 0 SQLite queries).
     * 2. If miss, searches local files using ChunkManagerService's in-memory inverted trigram/token index (0 SQLite queries).
     * 3. Queries all active remote peers in parallel via CompletableFuture.allOf.
     * 4. Aggregates all peers hosting each file into SwarmDirectoryCache AND attaches the full sorted
     *    `swarmPeers` list to each SearchResult so a single receiver can stream in parallel from all seeders.
     */
    public List<SearchResult> executeDistributedSearch(String keyword, int initialTtl) {
        // 1. Check QueryCache first
        List<SearchResult> cachedHit = checkQueryCache(keyword);
        if (cachedHit != null) {
            return cachedHit;
        }

        // 2. Cache miss: search local in-memory catalog (zero SQLite overhead)
        queryCacheMisses.incrementAndGet();
        List<SearchResult> localResults = searchLocalFiles(keyword);

        // Populate SwarmDirectoryCache with local files
        for (SearchResult sr : localResults) {
            PeerEndpoint selfEp = new PeerEndpoint(myPeerId, getAdvertisedHost(), myTcpPort, 0L, true);
            registerSwarmPeer(sr.getFileHash(), sr.getFileId(), sr.getFileName(), selfEp);
            sr.setSwarmPeers(List.of(selfEp));
        }

        if (initialTtl <= 0) {
            putInQueryCache(keyword != null ? keyword.trim().toLowerCase() : "", localResults);
            return localResults;
        }

        String requestId = UUID.randomUUID().toString();
        if (networkService != null) {
            networkService.hasProcessed(requestId);
        }

        NetworkMessage searchMsg = new NetworkMessage(
                "SEARCH",
                requestId,
                myPeerId,
                myPeerId,
                initialTtl,
                keyword
        );

        List<PeerInfo> knownPeers = getKnownPeers();
        List<CompletableFuture<List<SearchResult>>> futures = new ArrayList<>();

        for (PeerInfo peer : knownPeers) {
            if (isSelf(peer)) continue;
            if (offlinePeers.contains(peer.getPeerId())) continue;
            futures.add(CompletableFuture.supplyAsync(() -> queryPeer(peer, searchMsg, initialTtl), searchExecutor));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(2500, TimeUnit.MILLISECONDS);
        } catch (Exception ignored) {}

        List<SearchResult> aggregated = new ArrayList<>(localResults);
        for (CompletableFuture<List<SearchResult>> f : futures) {
            try {
                if (f.isDone() && !f.isCompletedExceptionally()) {
                    List<SearchResult> peerRes = f.getNow(Collections.emptyList());
                    if (peerRes != null) {
                        aggregated.addAll(peerRes);
                    }
                }
            } catch (Exception ignored) {}
        }

        // Deduplicate by fileHash (or fileName + sizeBytes) WHILE merging all hosting peers into swarmPeers!
        Map<String, SearchResult> deduped = new LinkedHashMap<>();
        Map<String, Map<String, PeerEndpoint>> mergedSwarms = new LinkedHashMap<>();

        for (SearchResult sr : aggregated) {
            String key = (sr.getFileHash() != null && !sr.getFileHash().isBlank())
                    ? sr.getFileHash().trim().toLowerCase()
                    : ((sr.getFileName() != null ? sr.getFileName().trim().toLowerCase() : sr.getFileId()) + ":" + sr.getSizeBytes());

            PeerEndpoint ep = new PeerEndpoint(
                    sr.getSourcePeerId(),
                    sr.getSourceHost(),
                    sr.getSourcePort(),
                    sr.getRttMs(),
                    true
            );
            registerSwarmPeer(sr.getFileHash(), sr.getFileId(), sr.getFileName(), ep);

            Map<String, PeerEndpoint> epMap = mergedSwarms.computeIfAbsent(key, k -> new LinkedHashMap<>());
            epMap.putIfAbsent(ep.getHost() + ":" + ep.getPort(), ep);
            if (sr.getSwarmPeers() != null) {
                for (PeerEndpoint extraEp : sr.getSwarmPeers()) {
                    if (extraEp != null) {
                        registerSwarmPeer(sr.getFileHash(), sr.getFileId(), sr.getFileName(), extraEp);
                        epMap.putIfAbsent(extraEp.getHost() + ":" + extraEp.getPort(), extraEp);
                    }
                }
            }

            SearchResult existing = deduped.get(key);
            if (existing == null) {
                deduped.put(key, sr);
            } else if (sr.getRttMs() > 0 && (existing.getRttMs() <= 0 || sr.getRttMs() < existing.getRttMs())) {
                // Prefer lowest-RTT peer as primary source endpoint while retaining all peers in swarmPeers
                existing.setSourcePeerId(sr.getSourcePeerId());
                existing.setSourceHost(sr.getSourceHost());
                existing.setSourcePort(sr.getSourcePort());
                existing.setRttMs(sr.getRttMs());
            }
        }

        List<SearchResult> finalResults = new ArrayList<>(deduped.size());
        for (Map.Entry<String, SearchResult> entry : deduped.entrySet()) {
            SearchResult sr = entry.getValue();
            Map<String, PeerEndpoint> epMap = mergedSwarms.get(entry.getKey());
            if (epMap != null && !epMap.isEmpty()) {
                List<PeerEndpoint> sortedSwarm = new ArrayList<>(epMap.values());
                Collections.sort(sortedSwarm);
                sr.setSwarmPeers(sortedSwarm);
            }
            finalResults.add(sr);
        }

        // Cache aggregated results in QueryCache
        putInQueryCache(keyword != null ? keyword.trim().toLowerCase() : "", finalResults);

        return finalResults;
    }

    private List<SearchResult> queryPeer(PeerInfo peer, NetworkMessage searchMsg, int initialTtl) {
        long startNs = System.nanoTime();
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(peer.getHost(), peer.getPort()), 1500);
            socket.setSoTimeout(2000);

            try (DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                 DataInputStream dis = new DataInputStream(socket.getInputStream())) {

                FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(searchMsg));
                String rawReply = FramingUtil.readFrame(dis);
                long rtt = Math.max(1L, (System.nanoTime() - startNs) / 1_000_000L);

                NetworkMessage replyMsg = objectMapper.readValue(rawReply, NetworkMessage.class);
                if ("SEARCH_RESULT".equals(replyMsg.getType()) && replyMsg.getPayload() != null) {
                    List<SearchResult> peerResults = objectMapper.readValue(
                            replyMsg.getPayload(),
                            new TypeReference<List<SearchResult>>() {}
                    );
                    if (peerResults != null) {
                        peer.setRttMs(rtt);
                        knownPeersMap.put(peer.getPeerId(), peer);
                        onlinePeers.put(peer.getPeerId(), peer);
                        offlinePeers.remove(peer.getPeerId());

                        for (SearchResult sr : peerResults) {
                            if (sr.getSourcePeerId() == null || sr.getSourcePeerId().isBlank() || sr.getSourcePeerId().equals(myPeerId)) {
                                sr.setSourcePeerId(peer.getPeerId());
                            }
                            if (sr.getSourceHost() == null || sr.getSourceHost().isBlank()
                                    || "127.0.0.1".equals(sr.getSourceHost())
                                    || "localhost".equalsIgnoreCase(sr.getSourceHost())
                                    || "0.0.0.0".equals(sr.getSourceHost())
                                    || initialTtl <= 1
                                    || !sr.getSourceHost().equals(peer.getHost())) {
                                sr.setSourceHost(peer.getHost());
                            }
                            if (sr.getSourcePort() <= 0) {
                                sr.setSourcePort(peer.getPort());
                            }
                            if (sr.getRttMs() <= 0) {
                                sr.setRttMs(rtt);
                            }

                            PeerEndpoint ep = new PeerEndpoint(
                                    sr.getSourcePeerId(),
                                    sr.getSourceHost(),
                                    sr.getSourcePort(),
                                    sr.getRttMs(),
                                    true
                            );
                            registerSwarmPeer(sr.getFileHash(), sr.getFileId(), sr.getFileName(), ep);
                        }
                        return peerResults;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Search forward failed to peer: " + peer.getPeerId() + " (" + e.getMessage() + ")");
        }
        return Collections.emptyList();
    }

    public List<SearchResult> handleIncomingSearch(NetworkMessage msg) {
        List<SearchResult> results = new ArrayList<>(searchLocalFiles(msg.getPayload()));

        int nextTtl = msg.getTtl() - 1;
        if (nextTtl > 0) {
            NetworkMessage forwardMsg = new NetworkMessage(
                    "SEARCH",
                    msg.getRequestId(),
                    msg.getOriginPeerId(),
                    myPeerId,
                    nextTtl,
                    msg.getPayload()
            );

            List<CompletableFuture<List<SearchResult>>> forwardFutures = new ArrayList<>();
            for (PeerInfo peer : getKnownPeers()) {
                if (isSelf(peer)) continue;
                if (peer.getPeerId().equals(msg.getSenderPeerId()) || peer.getPeerId().equals(msg.getOriginPeerId())) {
                    continue;
                }
                if (offlinePeers.contains(peer.getPeerId())) continue;

                forwardFutures.add(CompletableFuture.supplyAsync(() -> queryPeer(peer, forwardMsg, nextTtl), searchExecutor));
            }

            try {
                CompletableFuture.allOf(forwardFutures.toArray(new CompletableFuture[0]))
                        .get(2000, TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {}

            for (CompletableFuture<List<SearchResult>> f : forwardFutures) {
                try {
                    if (f.isDone() && !f.isCompletedExceptionally()) {
                        List<SearchResult> forwarded = f.getNow(Collections.emptyList());
                        if (forwarded != null) {
                            results.addAll(forwarded);
                        }
                    }
                } catch (Exception ignored) {}
            }
        }

        Map<String, SearchResult> deduped = new LinkedHashMap<>();
        for (SearchResult sr : results) {
            String key = (sr.getFileHash() != null && !sr.getFileHash().isBlank()) ? sr.getFileHash() : sr.getFileId();
            deduped.putIfAbsent(key, sr);
        }
        return new ArrayList<>(deduped.values());
    }

    // ==========================================
    // Zero-SQLite Local File Search via Inverted Index
    // ==========================================

    public List<SearchResult> searchLocalFiles(String keyword) {
        String host = getAdvertisedHost();

        // Fast Path: Use ChunkManagerService's in-memory inverted trigram/token index (0 SQLite queries!)
        if (chunkManagerService != null) {
            List<SharedFile> matchedFiles = chunkManagerService.searchInMemoryCatalog(keyword);
            if (!matchedFiles.isEmpty()) {
                List<SearchResult> results = new ArrayList<>(matchedFiles.size());
                for (SharedFile sf : matchedFiles) {
                    PeerEndpoint selfEp = new PeerEndpoint(myPeerId, host, myTcpPort, 0L, true);
                    SearchResult sr = new SearchResult(
                            sf.getFileId(),
                            sf.getName(),
                            sf.getSizeBytes(),
                            myPeerId,
                            host,
                            myTcpPort,
                            sf.getFileHash(),
                            0L
                    );
                    sr.setSwarmPeers(List.of(selfEp));
                    results.add(sr);
                    registerSwarmPeer(sf.getFileHash(), sf.getFileId(), sf.getName(), selfEp);
                }
                return results;
            }
        }

        // Fallback if ChunkManagerService was not injected in a standalone unit test
        List<SearchResult> results = new ArrayList<>();
        String sql = "SELECT file_id, name, size_bytes, file_hash FROM files WHERE name LIKE ? OR file_id LIKE ?";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            String wildcard = "%" + (keyword != null ? keyword.trim() : "") + "%";
            ps.setString(1, wildcard);
            ps.setString(2, wildcard);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String fileId = rs.getString("file_id");
                    String name = rs.getString("name");
                    long sizeBytes = rs.getLong("size_bytes");
                    String fileHash = rs.getString("file_hash");
                    PeerEndpoint selfEp = new PeerEndpoint(myPeerId, host, myTcpPort, 0L, true);
                    SearchResult sr = new SearchResult(fileId, name, sizeBytes, myPeerId, host, myTcpPort, fileHash, 0L);
                    sr.setSwarmPeers(List.of(selfEp));
                    results.add(sr);
                    registerSwarmPeer(fileHash, fileId, name, selfEp);
                }
            }
        } catch (SQLException ignored) {}
        return results;
    }

    public String getAdvertisedHost() {
        if (cachedAdvertisedHost != null) {
            return cachedAdvertisedHost;
        }
        if (configuredHost != null && !configuredHost.isBlank()) {
            String trimmed = configuredHost.trim();
            try {
                InetAddress configuredAddr = InetAddress.getByName(trimmed);
                NetworkInterface nif = NetworkInterface.getByInetAddress(configuredAddr);
                if (nif != null && nif.isUp()) {
                    cachedAdvertisedHost = trimmed;
                    return trimmed;
                }
            } catch (Exception ignored) {}
        }
        try {
            List<String> preferredIps = new ArrayList<>();
            List<String> fallbackIps = new ArrayList<>();

            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                if (iface.isLoopback() || !iface.isUp() || iface.isVirtual()) continue;

                String name = iface.getName().toLowerCase();
                String displayName = iface.getDisplayName().toLowerCase();

                if (name.startsWith("docker") || name.startsWith("br-") || name.startsWith("veth")
                        || name.startsWith("vmnet") || name.startsWith("vbox") || name.startsWith("virbr")
                        || name.startsWith("tun") || name.startsWith("tap") || name.startsWith("cni")
                        || name.startsWith("tailscale")
                        || displayName.contains("docker") || displayName.contains("virtual")
                        || displayName.contains("wsl") || displayName.contains("vmware")
                        || displayName.contains("vbox")) {
                    continue;
                }

                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    if (addr instanceof Inet4Address && !addr.isLoopbackAddress() && !addr.isLinkLocalAddress()) {
                        String ip = addr.getHostAddress();
                        if (name.startsWith("en") || name.startsWith("eth") || name.startsWith("wlan") || name.startsWith("wl")) {
                            preferredIps.add(ip);
                        } else {
                            fallbackIps.add(ip);
                        }
                    }
                }
            }

            if (!preferredIps.isEmpty()) {
                cachedAdvertisedHost = preferredIps.get(0);
                return cachedAdvertisedHost;
            }
            if (!fallbackIps.isEmpty()) {
                cachedAdvertisedHost = fallbackIps.get(0);
                return cachedAdvertisedHost;
            }
            cachedAdvertisedHost = InetAddress.getLocalHost().getHostAddress();
            return cachedAdvertisedHost;
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    public boolean isSelf(PeerInfo peer) {
        if (peer == null) return true;
        if (myPeerId != null && myPeerId.equals(peer.getPeerId())) return true;
        return isSelfHostPort(peer.getHost(), peer.getPort());
    }

    public boolean isSelf(PeerEndpoint peer) {
        if (peer == null) return true;
        if (myPeerId != null && myPeerId.equals(peer.getPeerId())) return true;
        return isSelfHostPort(peer.getHost(), peer.getPort());
    }

    public boolean isSelfHostPort(String host, int port) {
        if (host == null) return false;
        boolean isLocalHost = "127.0.0.1".equals(host)
                || "localhost".equalsIgnoreCase(host)
                || "0.0.0.0".equals(host)
                || getAdvertisedHost().equalsIgnoreCase(host);
        return isLocalHost && (port == myTcpPort);
    }

    // ==========================================
    // Peer Mesh Management (In-Memory O(1) + Write-Behind SQLite)
    // ==========================================

    public List<PeerInfo> getKnownPeers() {
        if (!knownPeersMap.isEmpty()) {
            if (chunkManagerService != null) {
                chunkManagerService.incrementSqliteQueriesSaved();
            }
            return new ArrayList<>(knownPeersMap.values());
        }
        warmUpPeersFromDb();
        return new ArrayList<>(knownPeersMap.values());
    }

    private List<Integer> parseDataPorts(String dpStr) {
        if (dpStr == null || dpStr.isBlank()) return new ArrayList<>();
        List<Integer> list = new ArrayList<>();
        String clean = dpStr.replace("[", "").replace("]", "");
        for (String part : clean.split(",")) {
            try {
                list.add(Integer.parseInt(part.trim()));
            } catch (NumberFormatException ignored) {}
        }
        return list;
    }

    public void registerPeer(PeerInfo peer) {
        if (peer == null || isSelf(peer)) return;
        knownPeersMap.put(peer.getPeerId(), peer);
        onlinePeers.put(peer.getPeerId(), peer);
        offlinePeers.remove(peer.getPeerId());

        String sql = "INSERT OR REPLACE INTO peers (peer_id, host, ip, port, status, rtt_ms, share_ready, last_seen) VALUES (?, ?, ?, ?, 'CONNECTED', ?, ?, CURRENT_TIMESTAMP)";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, peer.getPeerId());
            ps.setString(2, peer.getHost());
            ps.setString(3, peer.getHost());
            ps.setInt(4, peer.getPort());
            ps.setLong(5, peer.getRttMs());
            ps.setInt(6, peer.isShareReady() ? 1 : 0);
            ps.executeUpdate();
        } catch (SQLException ignored) {}
    }

    public boolean removePeer(String peerId) {
        boolean wasInMem = knownPeersMap.remove(peerId) != null;
        onlinePeers.remove(peerId);
        offlinePeers.add(peerId);
        evictPeerFromSwarm(peerId);

        String sql = "DELETE FROM peers WHERE peer_id = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, peerId);
            return ps.executeUpdate() > 0 || wasInMem;
        } catch (SQLException e) {
            return wasInMem;
        }
    }

    // ==========================================
    // Cache & Swarm Observability Stats
    // ==========================================

    public CacheStats getCacheStats() {
        long hits = queryCacheHits.get();
        long misses = queryCacheMisses.get();
        long total = hits + misses;
        double hitRate = total > 0 ? (double) hits / total : 0.0;

        int cachedQueryCount = queryCache.size();
        int activeSwarmCount = swarmDirectory.size();

        List<PeerInfo> known = getKnownPeers();
        int knownPeersCount = known.size();

        double avgRtt = 0.0;
        long rttSum = 0;
        int rttCount = 0;
        for (PeerInfo p : known) {
            if (p.getRttMs() > 0) {
                rttSum += p.getRttMs();
                rttCount++;
            }
        }
        if (rttCount > 0) {
            avgRtt = (double) rttSum / rttCount;
        }

        Map<String, Integer> swarms = new HashMap<>();
        swarmDirectory.forEach((hash, set) -> swarms.put(hash, set.size()));

        int indexedFiles = chunkManagerService != null ? chunkManagerService.getInMemoryCatalogSize() : 0;
        int indexTokens = chunkManagerService != null ? chunkManagerService.getInvertedIndexTokenCount() : 0;
        long sqlSaved = chunkManagerService != null ? chunkManagerService.getSqliteQueriesSaved() : 0L;
        long fecRec = erasureCodingRecoveryEngine != null ? erasureCodingRecoveryEngine.getTotalFecRecoveries() : 0L;

        return new CacheStats(
                hits,
                misses,
                hitRate,
                cachedQueryCount,
                activeSwarmCount,
                knownPeersCount,
                avgRtt,
                swarms,
                indexedFiles,
                indexTokens,
                sqlSaved,
                fecRec
        );
    }
}
