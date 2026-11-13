package com.example.demo;

import com.example.demo.service.ChunkManagerService;
import com.example.demo.service.SearchManagerService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import modell.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.*;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.LongConsumer;

@Service
public class NetworkService {

    @Value("${peer.tcp.port:9001}")
    private int tcpPort;

    @Value("${peer.id:peer-1}")
    private String myPeerId;

    @Value("${peer.scan.ports:9000,9001,9002,9003,9004,9005}")
    private String scanPortsConfig;

    @Value("${db.url:jdbc:sqlite:peer.db}")
    private String dbUrl;

    private final ApplicationContext applicationContext;

    private ServerSocket serverSocket;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private volatile boolean isRunning = true;

    private final Set<String> processedRequestIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<String> localInterfaceIps = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Socket Connection Pool
    private final ConcurrentHashMap<String, BlockingQueue<PooledConnection>> connectionPool = new ConcurrentHashMap<>();
    private static final int MAX_POOL_PER_PEER = 8;

    public NetworkService(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public int getTcpPort() {
        return tcpPort;
    }

    public String getMyPeerId() {
        return myPeerId;
    }

    public static class PooledConnection implements AutoCloseable {
        private final String peerKey;
        private final Socket socket;
        private final DataInputStream dis;
        private final DataOutputStream dos;
        private volatile boolean valid = true;

        public PooledConnection(String peerKey, Socket socket) throws IOException {
            this.peerKey = peerKey;
            this.socket = socket;
            this.socket.setTcpNoDelay(true);
            this.socket.setSoTimeout(5000);
            this.dis = new DataInputStream(socket.getInputStream());
            this.dos = new DataOutputStream(socket.getOutputStream());
        }

        public String getPeerKey() { return peerKey; }
        public Socket getSocket() { return socket; }
        public DataInputStream getDis() { return dis; }
        public DataOutputStream getDos() { return dos; }
        public boolean isValid() { return valid && !socket.isClosed() && socket.isConnected(); }
        public void markInvalid() { this.valid = false; }

        @Override
        public void close() {
            valid = false;
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    public PooledConnection acquireConnection(String host, int port) throws IOException {
        String key = host + ":" + port;
        BlockingQueue<PooledConnection> queue = connectionPool.computeIfAbsent(key, k -> new LinkedBlockingQueue<>());
        PooledConnection conn;
        while ((conn = queue.poll()) != null) {
            if (conn.isValid() && !conn.getSocket().isClosed()) {
                return conn;
            }
            conn.close();
        }

        Socket socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(5000);
        socket.connect(new InetSocketAddress(host, port), 3000);
        return new PooledConnection(key, socket);
    }

    public void releaseConnection(PooledConnection conn) {
        if (conn == null) return;
        if (conn.isValid() && !conn.getSocket().isClosed()) {
            BlockingQueue<PooledConnection> queue = connectionPool.get(conn.getPeerKey());
            if (queue != null && queue.size() < MAX_POOL_PER_PEER) {
                queue.offer(conn);
                return;
            }
        }
        conn.close();
    }

    public void evictConnection(PooledConnection conn) {
        if (conn == null) return;
        conn.markInvalid();
        conn.close();
    }

    public void evictPeer(String host, int port) {
        String key = host + ":" + port;
        BlockingQueue<PooledConnection> queue = connectionPool.remove(key);
        if (queue != null) {
            PooledConnection c;
            while ((c = queue.poll()) != null) {
                c.close();
            }
        }
    }

    public void closeAllConnections() {
        for (BlockingQueue<PooledConnection> queue : connectionPool.values()) {
            PooledConnection c;
            while ((c = queue.poll()) != null) {
                c.close();
            }
        }
        connectionPool.clear();
    }

    @PostConstruct
    public void startServer() {
        refreshLocalInterfaceIps();

        threadPool.submit(() -> {
            try {
                serverSocket = new ServerSocket(tcpPort);
                System.out.println(">>> TCP Network Server listening on port: " + tcpPort);

                while (isRunning && !serverSocket.isClosed()) {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setTcpNoDelay(true);
                    threadPool.submit(() -> handleIncomingConnection(clientSocket));
                }
            } catch (IOException e) {
                if (isRunning) {
                    System.err.println("TCP Server socket error on port " + tcpPort + ": " + e.getMessage());
                }
            }
        });
    }

    private SearchManagerService getSearchManagerService() {
        return applicationContext.getBean(SearchManagerService.class);
    }

    private ChunkManagerService getChunkManagerService() {
        return applicationContext.getBean(ChunkManagerService.class);
    }

    private void refreshLocalInterfaceIps() {
        localInterfaceIps.clear();
        localInterfaceIps.add("127.0.0.1");
        localInterfaceIps.add("localhost");
        localInterfaceIps.add("0.0.0.0");
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces != null && interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                if (!iface.isUp()) continue;
                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses != null && addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    localInterfaceIps.add(addr.getHostAddress().toLowerCase());
                }
            }
        } catch (Exception ignored) {}
    }

    public boolean isLocalSelf(String host, int port) {
        if (port != tcpPort) {
            return false;
        }
        if (host == null || host.isBlank()) {
            return true;
        }
        String trimmed = host.trim().toLowerCase();
        if ("127.0.0.1".equals(trimmed) || "localhost".equals(trimmed) || "0.0.0.0".equals(trimmed) || "::1".equals(trimmed)) {
            return true;
        }
        return localInterfaceIps.contains(trimmed);
    }

    private void handleIncomingConnection(Socket socket) {
        try {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(60000); // 60s idle timeout on server side
        } catch (IOException ignored) {}

        try (DataInputStream dis = new DataInputStream(socket.getInputStream());
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            while (isRunning && !socket.isClosed()) {
                String rawJson;
                try {
                    rawJson = FramingUtil.readFrame(dis);
                } catch (EOFException e) {
                    // Client disconnected normally
                    break;
                }

                JsonNode rootNode = objectMapper.readTree(rawJson);
                String type = rootNode.path("type").asText();

                // Duplicate Request Suppression
                if (rootNode.has("requestId") && !rootNode.path("requestId").isNull()) {
                    String reqId = rootNode.path("requestId").asText();
                    if (!reqId.isBlank()) {
                        if (processedRequestIds.contains(reqId)) {
                            System.out.println(">>> Suppressed duplicate request: " + reqId);
                            if ("SEARCH".equals(type)) {
                                NetworkMessage reply = new NetworkMessage(
                                        "SEARCH_RESULT",
                                        reqId,
                                        myPeerId,
                                        myPeerId,
                                        0,
                                        "[]"
                                );
                                FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(reply));
                            }
                            continue;
                        }
                        processedRequestIds.add(reqId);
                    }
                }

                if ("HELLO".equals(type)) {
                    Map<String, Object> ack = new LinkedHashMap<>();
                    ack.put("type", "HELLO_ACK");
                    ack.put("status", "AVAILABLE");
                    ack.put("peerId", myPeerId);
                    ack.put("senderPeerId", myPeerId);
                    ack.put("shareReady", true);
                    ack.put("payload", "Connection established");
                    FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(ack));

                    if (rootNode.has("peerId") && rootNode.has("listenPort")) {
                        String senderPeerId = rootNode.path("peerId").asText();
                        int senderPort = rootNode.path("listenPort").asInt();
                        String remoteIp = socket.getInetAddress().getHostAddress();
                        if (!myPeerId.equals(senderPeerId) && !isLocalSelf(remoteIp, senderPort)) {
                            PeerEndpoint ep = new PeerEndpoint(senderPeerId, remoteIp, senderPort, 0, true);
                            registerDiscoveredPeer(ep);
                        }
                    }
                } else if ("STREAM_REQ".equals(type)) {
                    StreamRequest req;
                    if (rootNode.has("payload") && !rootNode.path("payload").isNull() && !rootNode.path("payload").asText().isBlank()) {
                        try {
                            req = objectMapper.readValue(rootNode.path("payload").asText(), StreamRequest.class);
                        } catch (Exception e) {
                            req = objectMapper.treeToValue(rootNode, StreamRequest.class);
                        }
                    } else {
                        req = objectMapper.treeToValue(rootNode, StreamRequest.class);
                    }
                    handleStreamRequest(req, socket, dos);
                } else if ("JOIN".equals(type)) {
                    SearchManagerService sms = getSearchManagerService();
                    String payload = rootNode.has("payload") ? rootNode.path("payload").asText() : null;
                    if (payload != null && !payload.isBlank()) {
                        PeerInfo incomingPeer = objectMapper.readValue(payload, PeerInfo.class);
                        sms.registerPeer(incomingPeer);
                    }
                    List<PeerInfo> known = sms.getKnownPeers();
                    NetworkMessage reply = new NetworkMessage("PEER_LIST", myPeerId, objectMapper.writeValueAsString(known));
                    FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(reply));
                } else if ("SEARCH".equals(type)) {
                    NetworkMessage msg = objectMapper.treeToValue(rootNode, NetworkMessage.class);
                    SearchManagerService sms = getSearchManagerService();
                    List<SearchResult> results = sms.handleIncomingSearch(msg);
                    NetworkMessage reply = new NetworkMessage(
                            "SEARCH_RESULT",
                            msg.getRequestId(),
                            myPeerId,
                            myPeerId,
                            0,
                            objectMapper.writeValueAsString(results)
                    );
                    FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(reply));
                } else if ("CHUNK_REQUEST".equals(type)) {
                    ChunkManagerService cms = getChunkManagerService();
                    ChunkRequest req;
                    String payload = rootNode.has("payload") ? rootNode.path("payload").asText() : null;
                    if (payload != null && !payload.isBlank()) {
                        req = objectMapper.readValue(payload, ChunkRequest.class);
                    } else {
                        req = objectMapper.treeToValue(rootNode, ChunkRequest.class);
                    }
                    byte[] chunkBytes = cms.getChunkBytes(req.getFileId(), req.getChunkNo());
                    ChunkData chunkData;
                    if (chunkBytes != null) {
                        String base64 = Base64.getEncoder().encodeToString(chunkBytes);
                        chunkData = new ChunkData(req.getFileId(), req.getChunkNo(), base64, chunkBytes.length);
                    } else {
                        chunkData = ChunkData.error(req.getFileId(), req.getChunkNo(), "Chunk not found");
                    }
                    String reqId = rootNode.has("requestId") ? rootNode.path("requestId").asText() : UUID.randomUUID().toString();
                    NetworkMessage reply = new NetworkMessage(
                            "CHUNK_RESPONSE",
                            reqId,
                            myPeerId,
                            myPeerId,
                            0,
                            objectMapper.writeValueAsString(chunkData)
                    );
                    FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(reply));
                }
            }
        } catch (IOException ignored) {
        }
    }

    private void handleStreamRequest(StreamRequest req, Socket socket, DataOutputStream dos) throws IOException {
        if (req == null) {
            StreamResponse res = StreamResponse.error("unknown", "Null request");
            FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(res));
            return;
        }

        File targetFile = resolveLocalFile(req.getFileId(), req.getFileHash());
        if (targetFile != null && targetFile.exists() && targetFile.isFile()) {
            long fileSize = targetFile.length();
            long offset = req.getOffset();
            long length = req.getLength();

            if (offset >= 0 && length >= 0 && (offset + length <= fileSize)) {
                StreamResponse res = StreamResponse.ok(req.getFileId(), offset, length);
                FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(res));
                try (FileChannel fileChannel = FileChannel.open(targetFile.toPath(), StandardOpenOption.READ)) {
                    FramingUtil.pipeChannelToStream(fileChannel, dos, offset, length, null);
                }
                dos.flush();
            } else {
                StreamResponse res = StreamResponse.error(req.getFileId(),
                        "Invalid range: offset " + offset + ", length " + length + ", fileSize " + fileSize);
                FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(res));
            }
        } else {
            StreamResponse res = StreamResponse.error(req.getFileId(), "File not found: " + req.getFileId());
            FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(res));
        }
    }

    private File resolveLocalFile(String fileId, String fileHash) {
        ChunkManagerService cms = getChunkManagerService();
        if (fileId != null && !fileId.isBlank()) {
            try {
                File f = cms.resolveFileForDownload(fileId, null, null, 0);
                if (f != null && f.exists() && f.isFile()) {
                    return f;
                }
            } catch (Exception ignored) {}

            try {
                SharedFile sf = cms.getSharedFile(fileId);
                if (sf != null && sf.getOriginalPath() != null) {
                    File f = new File(sf.getOriginalPath());
                    if (f.exists() && f.isFile()) {
                        return f;
                    }
                }
            } catch (Exception ignored) {}
        }

        // Query SQLite files table
        String sql = "SELECT original_path, name FROM files WHERE file_id = ? OR (file_hash IS NOT NULL AND file_hash = ? AND file_hash != '') LIMIT 1";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, fileId != null ? fileId : "");
            ps.setString(2, fileHash != null ? fileHash : "");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String origPath = rs.getString("original_path");
                    if (origPath != null && !origPath.isBlank()) {
                        File f = new File(origPath);
                        if (f.exists() && f.isFile()) return f;
                    }
                    String name = rs.getString("name");
                    if (name != null && !name.isBlank()) {
                        File f1 = Paths.get("peer-data", "uploads", name).toFile();
                        if (f1.exists() && f1.isFile()) return f1;
                        File f2 = Paths.get("peer-data", "reconstructed", name).toFile();
                        if (f2.exists() && f2.isFile()) return f2;
                        File f3 = new File(name);
                        if (f3.exists() && f3.isFile()) return f3;
                    }
                }
            }
        } catch (Exception ignored) {}

        return null;
    }

    public StreamResponse requestStream(String targetIp, int targetPort, StreamRequest req,
                                        FileChannel targetChannel, long destinationOffset,
                                        LongConsumer onProgress) throws IOException {
        IOException lastEx = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            PooledConnection conn = null;
            try {
                conn = acquireConnection(targetIp, targetPort);
                String reqJson = objectMapper.writeValueAsString(req);
                FramingUtil.writeFrame(conn.getDos(), reqJson);

                String resJson = FramingUtil.readFrame(conn.getDis());
                StreamResponse res = objectMapper.readValue(resJson, StreamResponse.class);

                if ("OK".equalsIgnoreCase(res.getStatus())) {
                    FramingUtil.pipeRawBytes(conn.getDis(), targetChannel, destinationOffset, res.getLength(), onProgress);
                    releaseConnection(conn);
                    return res;
                } else {
                    releaseConnection(conn);
                    return res;
                }
            } catch (IOException e) {
                lastEx = e;
                if (conn != null) {
                    evictConnection(conn);
                }
            }
        }
        throw lastEx != null ? lastEx : new IOException("Failed to stream from " + targetIp + ":" + targetPort);
    }

    public List<PeerEndpoint> scanSubnetAndPorts() {
        refreshLocalInterfaceIps();
        Set<String> candidateIps = getCandidateIps();
        List<Integer> targetPorts = parseTargetPorts();

        List<Callable<PeerEndpoint>> probeTasks = new ArrayList<>();
        for (String ip : candidateIps) {
            for (int port : targetPorts) {
                if (isLocalSelf(ip, port)) {
                    continue;
                }
                probeTasks.add(() -> probeEndpoint(ip, port));
            }
        }

        List<PeerEndpoint> discovered = new CopyOnWriteArrayList<>();
        int poolSize = Math.min(64, Math.max(16, Runtime.getRuntime().availableProcessors() * 8));
        ExecutorService scanPool = Executors.newFixedThreadPool(poolSize);
        try {
            List<Future<PeerEndpoint>> futures = scanPool.invokeAll(probeTasks, 15, TimeUnit.SECONDS);
            for (Future<PeerEndpoint> f : futures) {
                try {
                    PeerEndpoint ep = f.get();
                    if (ep != null) {
                        discovered.add(ep);
                    }
                } catch (Exception ignored) {}
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            scanPool.shutdownNow();
        }

        // Deduplicate by peerId
        Map<String, PeerEndpoint> deduped = new LinkedHashMap<>();
        for (PeerEndpoint ep : discovered) {
            deduped.putIfAbsent(ep.getPeerId(), ep);
        }
        return new ArrayList<>(deduped.values());
    }

    private PeerEndpoint probeEndpoint(String ip, int port) {
        if (isLocalSelf(ip, port)) {
            return null;
        }

        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(1000); // 1s read timeout for handshake response

            long startNs = System.nanoTime();
            socket.connect(new InetSocketAddress(ip, port), 300); // 300ms connect timeout

            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());

            // Send HELLO frame
            Map<String, Object> helloMap = new LinkedHashMap<>();
            helloMap.put("type", "HELLO");
            helloMap.put("peerId", myPeerId);
            helloMap.put("listenPort", tcpPort);
            helloMap.put("dataPorts", List.of(9001, 9002, 9003, 9004));
            helloMap.put("shareReady", true);

            FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(helloMap));

            String respJson = FramingUtil.readFrame(dis);
            long rttMs = Math.max(1, (System.nanoTime() - startNs) / 1_000_000);

            JsonNode node = objectMapper.readTree(respJson);
            String type = node.path("type").asText();
            if (!"HELLO_ACK".equalsIgnoreCase(type)) {
                return null;
            }

            String remotePeerId = node.has("peerId") ? node.path("peerId").asText() : node.path("senderPeerId").asText();
            if (remotePeerId == null || remotePeerId.isBlank() || myPeerId.equals(remotePeerId)) {
                return null; // Ignore self!
            }

            boolean shareReady = !node.has("shareReady") || node.path("shareReady").asBoolean();

            List<Integer> dataPorts = new ArrayList<>();
            if (node.has("dataPorts") && node.path("dataPorts").isArray()) {
                for (JsonNode pNode : node.path("dataPorts")) {
                    dataPorts.add(pNode.asInt());
                }
            }
            if (dataPorts.isEmpty()) {
                dataPorts.add(port);
            }

            PeerEndpoint endpoint = new PeerEndpoint(remotePeerId, ip, port, rttMs, shareReady, dataPorts);
            registerDiscoveredPeer(endpoint);
            return endpoint;
        } catch (Exception ignored) {
            return null;
        }
    }

    private Set<String> getCandidateIps() {
        Set<String> candidateIps = new LinkedHashSet<>();
        // Priority 1: localhost loopback
        candidateIps.add("127.0.0.1");

        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces != null && interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                if (iface.isLoopback() || !iface.isUp() || iface.isVirtual()) continue;

                String name = iface.getName().toLowerCase();
                String displayName = iface.getDisplayName().toLowerCase();

                // Skip container, VM, bridge, and tunnel virtual interfaces
                if (name.startsWith("docker") || name.startsWith("br-") || name.startsWith("veth")
                        || name.startsWith("vmnet") || name.startsWith("vbox") || name.startsWith("virbr")
                        || name.startsWith("tun") || name.startsWith("tap") || name.startsWith("cni")
                        || name.startsWith("tailscale")
                        || displayName.contains("docker") || displayName.contains("virtual")
                        || displayName.contains("wsl") || displayName.contains("vmware")
                        || displayName.contains("vbox")) {
                    continue;
                }

                for (InterfaceAddress ifaceAddr : iface.getInterfaceAddresses()) {
                    InetAddress addr = ifaceAddr.getAddress();
                    if (addr instanceof Inet4Address && !addr.isLoopbackAddress() && !addr.isLinkLocalAddress()) {
                        String hostIp = addr.getHostAddress();
                        candidateIps.add(hostIp);

                        // Scan /24 neighborhood of active host
                        int lastDot = hostIp.lastIndexOf('.');
                        if (lastDot > 0) {
                            String subnetPrefix = hostIp.substring(0, lastDot + 1);
                            for (int i = 1; i <= 254; i++) {
                                candidateIps.add(subnetPrefix + i);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error enumerating network interfaces: " + e.getMessage());
        }
        return candidateIps;
    }

    private List<Integer> parseTargetPorts() {
        Set<Integer> ports = new LinkedHashSet<>();
        if (scanPortsConfig != null && !scanPortsConfig.isBlank()) {
            for (String part : scanPortsConfig.split(",")) {
                try {
                    int p = Integer.parseInt(part.trim());
                    if (p > 0 && p <= 65535) {
                        ports.add(p);
                    }
                } catch (NumberFormatException ignored) {}
            }
        }
        if (ports.isEmpty()) {
            for (int p = 9000; p <= 9005; p++) {
                ports.add(p);
            }
        }
        return new ArrayList<>(ports);
    }

    public void registerDiscoveredPeer(PeerEndpoint endpoint) {
        if (endpoint == null || endpoint.getPeerId() == null || endpoint.getPeerId().isBlank()) {
            return;
        }
        String sql = """
            INSERT INTO peers (peer_id, host, ip, port, status, rtt_ms, share_ready, data_ports, last_seen)
            VALUES (?, ?, ?, ?, 'CONNECTED', ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(peer_id) DO UPDATE SET
                host=excluded.host,
                ip=excluded.ip,
                port=excluded.port,
                status='CONNECTED',
                rtt_ms=excluded.rtt_ms,
                share_ready=excluded.share_ready,
                data_ports=excluded.data_ports,
                last_seen=CURRENT_TIMESTAMP;
        """;
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, endpoint.getPeerId());
            ps.setString(2, endpoint.getHost());
            ps.setString(3, endpoint.getHost());
            ps.setInt(4, endpoint.getPort());
            ps.setLong(5, endpoint.getRttMs());
            ps.setInt(6, endpoint.isShareReady() ? 1 : 0);
            String dataPortsJson = objectMapper.writeValueAsString(endpoint.getDataPorts());
            ps.setString(7, dataPortsJson);
            ps.executeUpdate();
        } catch (Exception e) {
            System.err.println("Error registering discovered peer in DB: " + e.getMessage());
        }
    }

    public List<PeerEndpoint> getActivePeerEndpoints() {
        List<PeerEndpoint> list = new ArrayList<>();
        String sql = "SELECT peer_id, host, ip, port, rtt_ms, share_ready, data_ports FROM peers WHERE peer_id != ? AND status = 'CONNECTED'";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, myPeerId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String pId = rs.getString("peer_id");
                    String host = rs.getString("host");
                    int port = rs.getInt("port");
                    long rtt = rs.getLong("rtt_ms");
                    boolean shareReady = rs.getInt("share_ready") == 1;
                    String dpJson = rs.getString("data_ports");
                    List<Integer> dataPorts = new ArrayList<>();
                    if (dpJson != null && !dpJson.isBlank()) {
                        try {
                            dataPorts = objectMapper.readValue(dpJson, new TypeReference<List<Integer>>() {});
                        } catch (Exception ignored) {}
                    }
                    list.add(new PeerEndpoint(pId, host, port, rtt, shareReady, dataPorts));
                }
            }
        } catch (Exception e) {
            System.err.println("Error loading active peers: " + e.getMessage());
        }
        return list;
    }

    public String sendDirectMessage(String targetIp, int targetPort, NetworkMessage message) throws IOException {
        IOException lastEx = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            PooledConnection conn = null;
            try {
                conn = acquireConnection(targetIp, targetPort);
                String rawJson = objectMapper.writeValueAsString(message);
                FramingUtil.writeFrame(conn.getDos(), rawJson);
                String response = FramingUtil.readFrame(conn.getDis());
                releaseConnection(conn);
                return response;
            } catch (IOException e) {
                lastEx = e;
                if (conn != null) {
                    evictConnection(conn);
                }
            }
        }
        throw lastEx != null ? lastEx : new IOException("Failed to send message to " + targetIp + ":" + targetPort);
    }

    public ChunkData requestChunkFromPeer(String targetIp, int targetPort, String fileId, int chunkNo) throws IOException {
        IOException lastEx = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            PooledConnection conn = null;
            try {
                conn = acquireConnection(targetIp, targetPort);
                ChunkRequest req = new ChunkRequest(fileId, chunkNo);
                String payload = objectMapper.writeValueAsString(req);
                NetworkMessage msg = new NetworkMessage("CHUNK_REQUEST", myPeerId, payload);
                msg.setRequestId(UUID.randomUUID().toString());

                FramingUtil.writeFrame(conn.getDos(), objectMapper.writeValueAsString(msg));
                String rawReply = FramingUtil.readFrame(conn.getDis());
                NetworkMessage replyMsg = objectMapper.readValue(rawReply, NetworkMessage.class);

                releaseConnection(conn);

                if ("CHUNK_RESPONSE".equals(replyMsg.getType()) && replyMsg.getPayload() != null) {
                    return objectMapper.readValue(replyMsg.getPayload(), ChunkData.class);
                } else {
                    return ChunkData.error(fileId, chunkNo, "Unexpected response type: " + replyMsg.getType());
                }
            } catch (IOException e) {
                lastEx = e;
                if (conn != null) {
                    evictConnection(conn);
                }
            }
        }
        throw lastEx != null ? lastEx : new IOException("Failed to request chunk from " + targetIp + ":" + targetPort);
    }

    public boolean hasProcessed(String requestId) {
        return !processedRequestIds.add(requestId);
    }

    @PreDestroy
    public void shutdown() {
        isRunning = false;
        closeAllConnections();
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}
        threadPool.shutdownNow();
    }
}
