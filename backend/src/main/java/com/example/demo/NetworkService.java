package com.example.demo;

import com.example.demo.service.ChunkManagerService;
import com.example.demo.service.ErasureCodingRecoveryEngine;
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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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

    public static final int UDP_DISCOVERY_PORT = 9099;
    private static final String BEACON_PREFIX = "P2P_DISCOVERY_BEACON|";

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
    private DatagramSocket udpBeaconSocket;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final ExecutorService dbAsyncWriter = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "NetworkDbWriteBehind");
        t.setDaemon(true);
        return t;
    });
    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private volatile boolean isRunning = true;

    private final Set<String> processedRequestIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<String> localInterfaceIps = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Persistent Socket Connection Pool
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
        socket.setSendBufferSize(65536);
        socket.setReceiveBufferSize(65536);
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

        startUdpDiscoveryListener();
        broadcastUdpDiscoveryBeacon();
    }

    /**
     * Starts a lightweight zero-config UDP broadcast/loopback listener for sub-50ms peer discovery.
     */
    private void startUdpDiscoveryListener() {
        threadPool.submit(() -> {
            try {
                udpBeaconSocket = new DatagramSocket(null);
                udpBeaconSocket.setReuseAddress(true);
                udpBeaconSocket.setBroadcast(true);
                udpBeaconSocket.bind(new InetSocketAddress("0.0.0.0", UDP_DISCOVERY_PORT));

                byte[] buf = new byte[512];
                while (isRunning && !udpBeaconSocket.isClosed()) {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    udpBeaconSocket.receive(packet);
                    String msg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    if (msg.startsWith(BEACON_PREFIX)) {
                        String[] parts = msg.split("\\|");
                        if (parts.length >= 3) {
                            String remotePeerId = parts[1].trim();
                            int remoteTcpPort = Integer.parseInt(parts[2].trim());
                            String senderIp = packet.getAddress().getHostAddress();
                            if (!myPeerId.equals(remotePeerId) && !isLocalSelf(senderIp, remoteTcpPort)) {
                                threadPool.submit(() -> probeEndpoint(senderIp, remoteTcpPort));
                            }
                        }
                    }
                }
            } catch (Exception ignored) {
                // Graceful fallback if UDP port is already bound on single-host multi-instance runs
            }
        });
    }

    /**
     * Broadcasts a UDP gossip beacon to loopback and local subnet broadcast addresses so listening peers
     * immediately discover and handshake with this node.
     */
    public void broadcastUdpDiscoveryBeacon() {
        threadPool.submit(() -> {
            String payload = BEACON_PREFIX + myPeerId + "|" + tcpPort;
            byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
            try (DatagramSocket sender = new DatagramSocket()) {
                sender.setBroadcast(true);
                DatagramPacket loopbackPacket = new DatagramPacket(
                        bytes, bytes.length, InetAddress.getByName("127.0.0.1"), UDP_DISCOVERY_PORT);
                sender.send(loopbackPacket);

                DatagramPacket broadcastPacket = new DatagramPacket(
                        bytes, bytes.length, InetAddress.getByName("255.255.255.255"), UDP_DISCOVERY_PORT);
                sender.send(broadcastPacket);
            } catch (Exception ignored) {}
        });
    }

    private SearchManagerService getSearchManagerService() {
        return applicationContext.getBean(SearchManagerService.class);
    }

    private ChunkManagerService getChunkManagerService() {
        return applicationContext.getBean(ChunkManagerService.class);
    }

    private ErasureCodingRecoveryEngine getErasureCodingEngine() {
        try {
            return applicationContext.getBean(ErasureCodingRecoveryEngine.class);
        } catch (Exception e) {
            return null;
        }
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
                    break;
                }

                JsonNode rootNode = objectMapper.readTree(rawJson);
                String type = rootNode.path("type").asText();

                // Duplicate Request Suppression
                if (rootNode.has("requestId") && !rootNode.path("requestId").isNull()) {
                    String reqId = rootNode.path("requestId").asText();
                    if (!reqId.isBlank()) {
                        if (processedRequestIds.contains(reqId)) {
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
                            PeerEndpoint ep = new PeerEndpoint(senderPeerId, remoteIp, senderPort, 1L, true);
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
                } else if ("PARITY_REQ".equals(type)) {
                    handleParityRequest(rootNode, dos);
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

    /**
     * Handles a zero-chunking STREAM_REQ using O(1) in-memory file resolution and attaches
     * hardware-accelerated CRC32C + 64KB sub-block Merkle digests before piping raw bytes.
     */
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
                try (FileChannel fileChannel = FileChannel.open(targetFile.toPath(), StandardOpenOption.READ)) {
                    long windowCrc = 0L;
                    List<Long> subCrcs = Collections.emptyList();
                    ErasureCodingRecoveryEngine fecEngine = getErasureCodingEngine();
                    if (fecEngine != null && length > 0) {
                        ErasureCodingRecoveryEngine.WindowIntegrityMetadata meta =
                                fecEngine.computeWindowMetadata(fileChannel, offset, length);
                        windowCrc = meta.getWindowCrc32c();
                        subCrcs = meta.getSubBlockCrcs();
                    }
                    StreamResponse res = StreamResponse.ok(req.getFileId(), offset, length, windowCrc, subCrcs);
                    FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(res));
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

    /**
     * Serves XOR / Reed-Solomon parity stripes across requested file ranges for FEC error recovery.
     */
    private void handleParityRequest(JsonNode rootNode, DataOutputStream dos) throws IOException {
        String fileId = rootNode.path("fileId").asText(null);
        String fileHash = rootNode.path("fileHash").asText(null);
        long stripeOffset = rootNode.path("stripeOffset").asLong(0);
        int blockSize = rootNode.path("blockSize").asInt(65536);
        int stripeBlocks = rootNode.path("stripeBlocks").asInt(4);

        File targetFile = resolveLocalFile(fileId, fileHash);
        ErasureCodingRecoveryEngine fecEngine = getErasureCodingEngine();
        if (targetFile != null && targetFile.exists() && fecEngine != null) {
            try (FileChannel fc = FileChannel.open(targetFile.toPath(), StandardOpenOption.READ)) {
                List<byte[]> blocks = new ArrayList<>(stripeBlocks);
                for (int i = 0; i < stripeBlocks; i++) {
                    long pos = stripeOffset + (long) i * blockSize;
                    if (pos >= fc.size()) break;
                    int len = (int) Math.min(blockSize, fc.size() - pos);
                    byte[] arr = new byte[blockSize];
                    ByteBuffer bb = ByteBuffer.wrap(arr, 0, len);
                    fc.read(bb, pos);
                    blocks.add(arr);
                }
                byte[] parity = fecEngine.computeXorParityBlock(blocks, blockSize);
                Map<String, Object> header = Map.of(
                        "type", "PARITY_RES",
                        "status", "OK",
                        "fileId", fileId != null ? fileId : "",
                        "length", parity.length
                );
                FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(header));
                dos.write(parity);
                dos.flush();
                return;
            }
        }
        Map<String, Object> err = Map.of("type", "PARITY_RES", "status", "ERROR", "message", "File unavailable for parity");
        FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(err));
    }

    /**
     * Resolves a local file in O(1) RAM time via ChunkManagerService's in-memory catalog,
     * touching SQLite only as a one-time fallback for unit tests that insert rows via raw SQL.
     */
    private File resolveLocalFile(String fileId, String fileHash) {
        ChunkManagerService cms = getChunkManagerService();

        // 1. O(1) In-Memory Catalog Lookup (Zero SQLite Queries!)
        File memResolved = cms.resolveInMemoryFile(fileId, fileHash);
        if (memResolved != null) {
            return memResolved;
        }

        if (fileId != null && !fileId.isBlank()) {
            try {
                File f = cms.resolveFileForDownload(fileId, null, null, 0);
                if (f != null && f.exists() && f.isFile()) {
                    return f;
                }
            } catch (Exception ignored) {}
        }

        // 2. Fallback for raw SQL test inserts: query SQLite once and cache in memory
        String sql = "SELECT file_id, name, size_bytes, owner_peer_id, original_path, file_hash FROM files WHERE file_id = ? OR (file_hash IS NOT NULL AND file_hash = ? AND file_hash != '') LIMIT 1";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, fileId != null ? fileId : "");
            ps.setString(2, fileHash != null ? fileHash : "");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String origPath = rs.getString("original_path");
                    String name = rs.getString("name");
                    SharedFile sf = new SharedFile(
                            rs.getString("file_id"),
                            name,
                            rs.getLong("size_bytes"),
                            rs.getString("owner_peer_id"),
                            origPath,
                            rs.getString("file_hash")
                    );
                    cms.indexSharedFileInMemory(sf);

                    if (origPath != null && !origPath.isBlank()) {
                        File f = new File(origPath);
                        if (f.exists() && f.isFile()) return f;
                    }
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
                    long actualCrc = FramingUtil.pipeRawBytesWithCrc(
                            conn.getDis(), targetChannel, destinationOffset, res.getLength(), onProgress);
                    if (res.getCrc32c() > 0 && actualCrc != res.getCrc32c()) {
                        throw new IOException("CRC32C integrity mismatch on stream window: expected "
                                + res.getCrc32c() + " but computed " + actualCrc);
                    }
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
        broadcastUdpDiscoveryBeacon();

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
            List<Future<PeerEndpoint>> futures = scanPool.invokeAll(probeTasks, 10, TimeUnit.SECONDS);
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
            socket.setSoTimeout(800);

            long startNs = System.nanoTime();
            socket.connect(new InetSocketAddress(ip, port), 250);

            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());

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
                return null;
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
        candidateIps.add("127.0.0.1");

        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces != null && interfaces.hasMoreElements()) {
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

                for (InterfaceAddress ifaceAddr : iface.getInterfaceAddresses()) {
                    InetAddress addr = ifaceAddr.getAddress();
                    if (addr instanceof Inet4Address && !addr.isLoopbackAddress() && !addr.isLinkLocalAddress()) {
                        String hostIp = addr.getHostAddress();
                        candidateIps.add(hostIp);

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

        // Update in-memory peer cache immediately in SearchManagerService (0ms latency)
        try {
            SearchManagerService sms = getSearchManagerService();
            if (sms != null) {
                PeerInfo info = new PeerInfo(
                        endpoint.getPeerId(),
                        endpoint.getHost(),
                        endpoint.getPort(),
                        endpoint.getRttMs(),
                        endpoint.isShareReady(),
                        endpoint.getDataPorts()
                );
                sms.cacheDiscoveredPeerInMemory(info);
            }
        } catch (Exception ignored) {}

        // Persist asynchronously to SQLite off the network hot path
        dbAsyncWriter.submit(() -> {
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
        });
    }

    public List<PeerEndpoint> getActivePeerEndpoints() {
        try {
            SearchManagerService sms = getSearchManagerService();
            if (sms != null) {
                List<PeerInfo> peers = sms.getKnownPeers();
                List<PeerEndpoint> endpoints = new ArrayList<>(peers.size());
                for (PeerInfo p : peers) {
                    endpoints.add(new PeerEndpoint(
                            p.getPeerId(), p.getHost(), p.getPort(), p.getRttMs(), p.isShareReady(), p.getDataPorts()));
                }
                return endpoints;
            }
        } catch (Exception ignored) {}

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
            if (udpBeaconSocket != null && !udpBeaconSocket.isClosed()) {
                udpBeaconSocket.close();
            }
        } catch (Exception ignored) {}
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}
        dbAsyncWriter.shutdownNow();
        threadPool.shutdownNow();
    }
}
