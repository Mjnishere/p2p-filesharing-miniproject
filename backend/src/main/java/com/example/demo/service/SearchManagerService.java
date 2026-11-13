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

    private final ObjectMapper objectMapper = new ObjectMapper();

    // Tier 1: QueryCache (LRU, max 500 entries, 60s sliding TTL)
    private final ConcurrentHashMap<String, CachedQueryResult> queryCache = new ConcurrentHashMap<>();
    private final AtomicLong queryCacheHits = new AtomicLong(0);
    private final AtomicLong queryCacheMisses = new AtomicLong(0);

    // Tier 2: SwarmDirectoryCache (fileHash -> PeerEndpoints sorted by lowest rttMs)
    private final ConcurrentHashMap<String, ConcurrentSkipListSet<PeerEndpoint>> swarmDirectory = new ConcurrentHashMap<>();

    // In-memory peer tracking
    private final ConcurrentHashMap<String, PeerInfo> onlinePeers = new ConcurrentHashMap<>();
    private final Set<String> offlinePeers = ConcurrentHashMap.newKeySet();

    // Thread pools for non-blocking parallel distributed searches and scheduled heartbeat sweeps
    private final ExecutorService searchExecutor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "HeartbeatSweepWorker");
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
        // 15-second heartbeat sweep to probe known peers, update rolling RTT, and evict dead peers
        scheduledExecutor.scheduleAtFixedRate(this::heartbeatSweep, 5, 15, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        scheduledExecutor.shutdownNow();
        searchExecutor.shutdownNow();
    }

    private void ensureDbSchema() {
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            try { stmt.execute("ALTER TABLE peers ADD COLUMN rtt_ms INTEGER DEFAULT 0"); } catch (SQLException ignored) {}
            try { stmt.execute("ALTER TABLE peers ADD COLUMN share_ready BOOLEAN DEFAULT 1"); } catch (SQLException ignored) {}
            try { stmt.execute("ALTER TABLE peers ADD COLUMN data_ports TEXT"); } catch (SQLException ignored) {}
            try { stmt.execute("ALTER TABLE files ADD COLUMN file_hash TEXT"); } catch (SQLException ignored) {}
        } catch (Exception ignored) {}
    }

    // ==========================================
    // Tier 2: SwarmDirectoryCache API
    // ==========================================

    /**
     * Registers a peer endpoint for a specific file hash.
     * Endpoints are automatically ordered by lowest rttMs in the ConcurrentSkipListSet.
     */
    public void registerSwarmPeer(String fileHash, PeerEndpoint peer) {
        if (fileHash == null || fileHash.isBlank() || peer == null) return;
        String normalizedHash = fileHash.trim().toLowerCase();
        ConcurrentSkipListSet<PeerEndpoint> set = swarmDirectory.computeIfAbsent(
                normalizedHash,
                k -> new ConcurrentSkipListSet<>()
        );
        // Remove prior entry for same peer identifier or host:port so that updated RTT is respected
        set.removeIf(p -> (peer.getPeerId() != null && peer.getPeerId().equals(p.getPeerId()))
                || (Objects.equals(peer.getHost(), p.getHost()) && peer.getPort() == p.getPort()));
        set.add(peer);
    }

    /**
     * Retrieves all known swarm peers hosting the given fileHash, ordered by lowest rttMs.
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
     * Evicts a failing or unresponsive peer from all swarms.
     */
    public void evictPeerFromSwarm(String peerId) {
        if (peerId == null || peerId.isBlank()) return;
        for (ConcurrentSkipListSet<PeerEndpoint> set : swarmDirectory.values()) {
            set.removeIf(p -> Objects.equals(p.getPeerId(), peerId));
        }
        swarmDirectory.entrySet().removeIf(e -> e.getValue().isEmpty());
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
    }

    public void updateSwarmPeerRtt(String peerId, long newRtt) {
        for (ConcurrentSkipListSet<PeerEndpoint> set : swarmDirectory.values()) {
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
                        newRtt,
                        target.isShareReady(),
                        target.getDataPorts()
                );
                set.add(updated);
            }
        }
    }

    // ==========================================
    // Tier 1: QueryCache API & Cache Hit Logic
    // ==========================================

    private void putInQueryCache(String normalizedQuery, List<SearchResult> results) {
        if (normalizedQuery == null) return;
        // Evict expired entries or oldest LRU if capacity reached
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
     * Fast in-memory cache lookup (< 0.5 ms) with sliding TTL verification, online hosting peer validation,
     * and local prefix filtering.
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
                        validResults.add(sr);
                    }
                }
                if (!validResults.isEmpty() || cached.getResults().isEmpty()) {
                    cached.recordHit();
                    queryCacheHits.incrementAndGet();
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
                            filtered.add(sr);
                        }
                    }
                    if (!filtered.isEmpty()) {
                        parentCached.recordHit();
                        queryCacheHits.incrementAndGet();
                        putInQueryCache(normalized, filtered);
                        return filtered;
                    }
                }
            }
        }

        return null;
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

    public boolean isHostingPeerOnline(SearchResult sr) {
        if (sr == null) return false;
        // Local node is always online
        if (myPeerId != null && myPeerId.equals(sr.getSourcePeerId())) {
            return true;
        }
        if (isSelfHostPort(sr.getSourceHost(), sr.getSourcePort())) {
            return true;
        }

        // Check SwarmDirectoryCache for active endpoints hosting this file
        String fileHash = sr.getFileHash();
        if (fileHash != null && !fileHash.isBlank()) {
            Set<PeerEndpoint> swarm = getSwarmPeers(fileHash);
            if (!swarm.isEmpty()) {
                for (PeerEndpoint ep : swarm) {
                    if (!offlinePeers.contains(ep.getPeerId()) && !isPeerExplicitlyOffline(ep.getPeerId())) {
                        return true;
                    }
                }
            }
        }

        // Check source peer status
        String sourcePeerId = sr.getSourcePeerId();
        if (sourcePeerId != null && !sourcePeerId.isBlank()) {
            if (offlinePeers.contains(sourcePeerId)) {
                return false;
            }
            if (onlinePeers.containsKey(sourcePeerId)) {
                return true;
            }
            return !isPeerExplicitlyOffline(sourcePeerId);
        }

        return true;
    }

    private boolean isPeerExplicitlyOffline(String peerId) {
        if (peerId == null) return false;
        if (offlinePeers.contains(peerId)) return true;
        String sql = "SELECT status FROM peers WHERE peer_id = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, peerId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return "OFFLINE".equalsIgnoreCase(rs.getString("status"));
                }
            }
        } catch (SQLException ignored) {}
        return false;
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
                rttMs = (System.nanoTime() - startNs) / 1_000_000L;

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
            long rollingRtt = (prevRtt <= 0) ? rttMs : (long) (0.7 * prevRtt + 0.3 * rttMs);
            peer.setRttMs(rollingRtt);
            peer.setShareReady(true);
            onlinePeers.put(peer.getPeerId(), peer);
            offlinePeers.remove(peer.getPeerId());
            updatePeerStatusInDb(peer.getPeerId(), "CONNECTED", rollingRtt, true);
            updateSwarmPeerRtt(peer.getPeerId(), rollingRtt);
        } else {
            onlinePeers.remove(peer.getPeerId());
            offlinePeers.add(peer.getPeerId());
            updatePeerStatusInDb(peer.getPeerId(), "OFFLINE", -1, false);
            evictPeerFromSwarm(peer.getPeerId());
        }
    }

    private void updatePeerStatusInDb(String peerId, String status, long rttMs, boolean shareReady) {
        String sql = "UPDATE peers SET status = ?, rtt_ms = ?, share_ready = ?, last_seen = CURRENT_TIMESTAMP WHERE peer_id = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setLong(2, rttMs);
            ps.setInt(3, shareReady ? 1 : 0);
            ps.setString(4, peerId);
            ps.executeUpdate();
        } catch (SQLException e) {
            String fallback = "UPDATE peers SET status = ?, last_seen = CURRENT_TIMESTAMP WHERE peer_id = ?";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement ps = conn.prepareStatement(fallback)) {
                ps.setString(1, status);
                ps.setString(2, peerId);
                ps.executeUpdate();
            } catch (SQLException ignored) {}
        }
    }

    // ==========================================
    // Distributed Search Execution
    // ==========================================

    /**
     * Executes distributed search:
     * 1. Checks QueryCache first. If hit, returns instantly (< 0.5 ms).
     * 2. If miss, searches local SQLite files.
     * 3. Parallelizes remote peer queries using CompletableFuture.allOf.
     * 4. Populates SwarmDirectoryCache and puts aggregated results in QueryCache.
     */
    public List<SearchResult> executeDistributedSearch(String keyword, int initialTtl) {
        // 1. Check QueryCache first
        List<SearchResult> cachedHit = checkQueryCache(keyword);
        if (cachedHit != null) {
            return cachedHit;
        }

        // 2. Cache miss: record miss and perform distributed search
        queryCacheMisses.incrementAndGet();
        List<SearchResult> localResults = searchLocalFiles(keyword);

        // Populate SwarmDirectoryCache with local files
        for (SearchResult sr : localResults) {
            if (sr.getFileHash() != null && !sr.getFileHash().isBlank()) {
                registerSwarmPeer(sr.getFileHash(), new PeerEndpoint(myPeerId, getAdvertisedHost(), myTcpPort, 0L, true));
            }
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

        // Deduplicate by fileHash or fileId
        Map<String, SearchResult> deduped = new LinkedHashMap<>();
        for (SearchResult sr : aggregated) {
            String key = (sr.getFileHash() != null && !sr.getFileHash().isBlank())
                    ? sr.getFileHash()
                    : sr.getFileId();
            deduped.putIfAbsent(key, sr);
        }
        List<SearchResult> finalResults = new ArrayList<>(deduped.values());

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
                long rtt = (System.nanoTime() - startNs) / 1_000_000L;

                NetworkMessage replyMsg = objectMapper.readValue(rawReply, NetworkMessage.class);
                if ("SEARCH_RESULT".equals(replyMsg.getType()) && replyMsg.getPayload() != null) {
                    List<SearchResult> peerResults = objectMapper.readValue(
                            replyMsg.getPayload(),
                            new TypeReference<List<SearchResult>>() {}
                    );
                    if (peerResults != null) {
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

                            // Passively populate SwarmDirectoryCache
                            if (sr.getFileHash() != null && !sr.getFileHash().isBlank()) {
                                registerSwarmPeer(sr.getFileHash(), new PeerEndpoint(
                                        sr.getSourcePeerId(),
                                        sr.getSourceHost(),
                                        sr.getSourcePort(),
                                        sr.getRttMs(),
                                        true
                                ));
                            }
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
    // Local SQLite File Search & Host Resolution
    // ==========================================

    public List<SearchResult> searchLocalFiles(String keyword) {
        List<SearchResult> results = new ArrayList<>();
        String sql = "SELECT file_id, name, size_bytes, file_hash FROM files WHERE name LIKE ? OR file_id LIKE ?";

        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            String wildcard = "%" + (keyword != null ? keyword.trim() : "") + "%";
            ps.setString(1, wildcard);
            ps.setString(2, wildcard);
            try (ResultSet rs = ps.executeQuery()) {
                String host = getAdvertisedHost();
                while (rs.next()) {
                    String fileId = rs.getString("file_id");
                    String name = rs.getString("name");
                    long sizeBytes = rs.getLong("size_bytes");
                    String fileHash = rs.getString("file_hash");
                    results.add(new SearchResult(
                            fileId,
                            name,
                            sizeBytes,
                            myPeerId,
                            host,
                            myTcpPort,
                            fileHash,
                            0L
                    ));
                    if (fileHash != null && !fileHash.isBlank()) {
                        registerSwarmPeer(fileHash, new PeerEndpoint(myPeerId, host, myTcpPort, 0L, true));
                    }
                }
            }
        } catch (SQLException e) {
            String fallbackSql = "SELECT file_id, name, size_bytes FROM files WHERE name LIKE ? OR file_id LIKE ?";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement ps = conn.prepareStatement(fallbackSql)) {
                String wildcard = "%" + (keyword != null ? keyword.trim() : "") + "%";
                ps.setString(1, wildcard);
                ps.setString(2, wildcard);
                try (ResultSet rs = ps.executeQuery()) {
                    String host = getAdvertisedHost();
                    while (rs.next()) {
                        results.add(new SearchResult(
                                rs.getString("file_id"),
                                rs.getString("name"),
                                rs.getLong("size_bytes"),
                                myPeerId,
                                host,
                                myTcpPort,
                                null,
                                0L
                        ));
                    }
                }
            } catch (SQLException ex) {
                System.err.println("Error searching local SQLite files: " + ex.getMessage());
            }
        }
        return results;
    }

    public String getAdvertisedHost() {
        if (configuredHost != null && !configuredHost.isBlank()) {
            String trimmed = configuredHost.trim();
            try {
                InetAddress configuredAddr = InetAddress.getByName(trimmed);
                NetworkInterface nif = NetworkInterface.getByInetAddress(configuredAddr);
                if (nif != null && nif.isUp()) {
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
                return preferredIps.get(0);
            }
            if (!fallbackIps.isEmpty()) {
                return fallbackIps.get(0);
            }
            return InetAddress.getLocalHost().getHostAddress();
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
    // Peer Mesh Management
    // ==========================================

    public List<PeerInfo> getKnownPeers() {
        List<PeerInfo> peers = new ArrayList<>();
        String sql = "SELECT peer_id, host, port, rtt_ms, share_ready, data_ports FROM peers WHERE peer_id != ?";

        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, myPeerId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String pid = rs.getString("peer_id");
                    String h = rs.getString("host");
                    int p = rs.getInt("port");
                    long rtt = rs.getLong("rtt_ms");
                    boolean sr = rs.getBoolean("share_ready");
                    String dpStr = rs.getString("data_ports");
                    List<Integer> dataPorts = parseDataPorts(dpStr);

                    PeerInfo peer = new PeerInfo(pid, h, p, rtt, sr, dataPorts);
                    if (!isSelf(peer)) {
                        peers.add(peer);
                    }
                }
            }
        } catch (SQLException e) {
            String fallbackSql = "SELECT peer_id, host, port FROM peers WHERE peer_id != ?";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement ps = conn.prepareStatement(fallbackSql)) {
                ps.setString(1, myPeerId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        PeerInfo peer = new PeerInfo(
                                rs.getString("peer_id"),
                                rs.getString("host"),
                                rs.getInt("port")
                        );
                        if (!isSelf(peer)) {
                            peers.add(peer);
                        }
                    }
                }
            } catch (SQLException ex) {
                System.err.println("Error retrieving peers: " + ex.getMessage());
            }
        }
        return peers;
    }

    private List<Integer> parseDataPorts(String dpStr) {
        if (dpStr == null || dpStr.isBlank()) return new ArrayList<>();
        List<Integer> list = new ArrayList<>();
        for (String part : dpStr.split(",")) {
            try {
                list.add(Integer.parseInt(part.trim()));
            } catch (NumberFormatException ignored) {}
        }
        return list;
    }

    public void registerPeer(PeerInfo peer) {
        if (peer == null || isSelf(peer)) return;
        ensureDbSchema();
        String sql = "INSERT OR REPLACE INTO peers (peer_id, host, ip, port, rtt_ms, share_ready, last_seen) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, peer.getPeerId());
            ps.setString(2, peer.getHost());
            ps.setString(3, peer.getHost());
            ps.setInt(4, peer.getPort());
            ps.setLong(5, peer.getRttMs());
            ps.setInt(6, peer.isShareReady() ? 1 : 0);
            ps.executeUpdate();
            onlinePeers.put(peer.getPeerId(), peer);
            offlinePeers.remove(peer.getPeerId());
        } catch (SQLException e) {
            String fallback = "INSERT OR REPLACE INTO peers (peer_id, host, port, last_seen) VALUES (?, ?, ?, CURRENT_TIMESTAMP)";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement ps = conn.prepareStatement(fallback)) {
                ps.setString(1, peer.getPeerId());
                ps.setString(2, peer.getHost());
                ps.setInt(3, peer.getPort());
                ps.executeUpdate();
                onlinePeers.put(peer.getPeerId(), peer);
                offlinePeers.remove(peer.getPeerId());
            } catch (SQLException ex) {
                System.err.println("Error registering peer: " + ex.getMessage());
            }
        }
    }

    public boolean removePeer(String peerId) {
        String sql = "DELETE FROM peers WHERE peer_id = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, peerId);
            boolean removed = ps.executeUpdate() > 0;
            onlinePeers.remove(peerId);
            offlinePeers.add(peerId);
            evictPeerFromSwarm(peerId);
            return removed;
        } catch (SQLException e) {
            System.err.println("Error removing peer: " + e.getMessage());
            return false;
        }
    }

    // ==========================================
    // Cache & Swarm Observability Stats
    // ==========================================

    /**
     * Aggregates and returns two-tier cache telemetry metrics.
     */
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

        return new CacheStats(
                hits,
                misses,
                hitRate,
                cachedQueryCount,
                activeSwarmCount,
                knownPeersCount,
                avgRtt,
                swarms
        );
    }
}
