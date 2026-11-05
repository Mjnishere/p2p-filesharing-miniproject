package com.example.demo.service;

import com.example.demo.FramingUtil;
import com.example.demo.NetworkService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import modell.NetworkMessage;
import modell.PeerInfo;
import modell.SearchResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.sql.*;
import java.util.*;

@Service
public class SearchManagerService {

   @Value("${db.url:jdbc:sqlite:peer.db}")
   private String dbUrl;
   
    @Value("${peer.id:peer-1}")
    private String myPeerId;

    @Value("${peer.tcp.port:9001}")
    private int myTcpPort;

    @Value("${peer.host:}")
    private String configuredHost;

    private final NetworkService networkService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public SearchManagerService(NetworkService networkService) {
        this.networkService = networkService;
    }

    public String getAdvertisedHost() {
        if (configuredHost != null && !configuredHost.isBlank()) {
            return configuredHost.trim();
        }
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                if (iface.isLoopback() || !iface.isUp()) continue;
                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
                        return addr.getHostAddress();
                    }
                }
            }
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    // Query local SQLite for files matching keyword or fileId (case-insensitive substring)
    public List<SearchResult> searchLocalFiles(String keyword) {
        List<SearchResult> results = new ArrayList<>();
        String sql = "SELECT file_id, name, size_bytes FROM files WHERE name LIKE ? OR file_id LIKE ?";

        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
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
                            myTcpPort
                    ));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error searching local SQLite files: " + e.getMessage());
        }
        return results;
    }

    public boolean isSelf(PeerInfo peer) {
        if (peer == null) return true;
        if (myPeerId != null && myPeerId.equals(peer.getPeerId())) return true;
        String host = peer.getHost();
        if (host == null) return true;
        boolean isLocalHost = "127.0.0.1".equals(host)
                || "localhost".equalsIgnoreCase(host)
                || "0.0.0.0".equals(host)
                || getAdvertisedHost().equalsIgnoreCase(host);
        return isLocalHost && (peer.getPort() == myTcpPort);
    }

    // Retrieve active neighbor peers registered in SQLite
    public List<PeerInfo> getKnownPeers() {
        List<PeerInfo> peers = new ArrayList<>();
        String sql = "SELECT peer_id, host, port FROM peers WHERE peer_id != ?";

        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, myPeerId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    PeerInfo p = new PeerInfo(
                            rs.getString("peer_id"),
                            rs.getString("host"),
                            rs.getInt("port")
                    );
                    if (!isSelf(p)) {
                        peers.add(p);
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("Error retrieving peers: " + e.getMessage());
        }
        return peers;
    }

    public void registerPeer(PeerInfo peer) {
        String sql = "INSERT OR REPLACE INTO peers (peer_id, host, port, last_seen) VALUES (?, ?, ?, CURRENT_TIMESTAMP)";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, peer.getPeerId());
            ps.setString(2, peer.getHost());
            ps.setInt(3, peer.getPort());
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error registering peer: " + e.getMessage());
        }
    }

    public boolean removePeer(String peerId) {
        String sql = "DELETE FROM peers WHERE peer_id = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, peerId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            System.err.println("Error removing peer: " + e.getMessage());
            return false;
        }
    }

    // Initiates distributed search: searches locally, then forwards to known peers if TTL > 0
    public List<SearchResult> executeDistributedSearch(String keyword, int initialTtl) {
        String requestId = UUID.randomUUID().toString();
        List<SearchResult> aggregatedResults = new ArrayList<>(searchLocalFiles(keyword));

        // Mark as processed locally to prevent self-looping
        networkService.hasProcessed(requestId);

        if (initialTtl <= 0) {
            return aggregatedResults;
        }

        NetworkMessage searchMsg = new NetworkMessage(
                "SEARCH",
                requestId,
                myPeerId,
                myPeerId,
                initialTtl,
                keyword
        );

        for (PeerInfo peer : getKnownPeers()) {
            if (isSelf(peer)) continue;
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(peer.getHost(), peer.getPort()), 2000);
                socket.setSoTimeout(2500);
                try (DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                     DataInputStream dis = new DataInputStream(socket.getInputStream())) {

                    FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(searchMsg));
                    String rawReply = FramingUtil.readFrame(dis);
                    NetworkMessage replyMsg = objectMapper.readValue(rawReply, NetworkMessage.class);

                    if ("SEARCH_RESULT".equals(replyMsg.getType()) && replyMsg.getPayload() != null) {
                        List<SearchResult> peerResults = objectMapper.readValue(
                                replyMsg.getPayload(),
                                new TypeReference<List<SearchResult>>() {}
                        );
                        if (peerResults != null) {
                            for (SearchResult sr : peerResults) {
                                if (sr.getSourceHost() == null || "127.0.0.1".equals(sr.getSourceHost()) || "localhost".equalsIgnoreCase(sr.getSourceHost()) || "0.0.0.0".equals(sr.getSourceHost())) {
                                    sr.setSourceHost(peer.getHost());
                                }
                            }
                            aggregatedResults.addAll(peerResults);
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Search forward failed to peer: " + peer.getPeerId() + " (" + e.getMessage() + ")");
            }
        }

        // Deduplicate results by fileId
        Map<String, SearchResult> deduped = new LinkedHashMap<>();
        for (SearchResult r : aggregatedResults) {
            deduped.putIfAbsent(r.getFileId(), r);
        }
        return new ArrayList<>(deduped.values());
    }

    // Handles incoming SEARCH messages routed from NetworkService
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

            for (PeerInfo peer : getKnownPeers()) {
                if (isSelf(peer)) continue;
                // Do not send back to direct sender or original initiator
                if (peer.getPeerId().equals(msg.getSenderPeerId()) || peer.getPeerId().equals(msg.getOriginPeerId())) {
                    continue;
                }

                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(peer.getHost(), peer.getPort()), 2000);
                    socket.setSoTimeout(2500);
                    try (DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                         DataInputStream dis = new DataInputStream(socket.getInputStream())) {

                        FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(forwardMsg));
                        String rawReply = FramingUtil.readFrame(dis);
                        NetworkMessage replyMsg = objectMapper.readValue(rawReply, NetworkMessage.class);

                        if ("SEARCH_RESULT".equals(replyMsg.getType()) && replyMsg.getPayload() != null) {
                            List<SearchResult> forwardedResults = objectMapper.readValue(
                                    replyMsg.getPayload(),
                                    new TypeReference<List<SearchResult>>() {}
                            );
                            if (forwardedResults != null) {
                                for (SearchResult sr : forwardedResults) {
                                    if (sr.getSourceHost() == null || "127.0.0.1".equals(sr.getSourceHost()) || "localhost".equalsIgnoreCase(sr.getSourceHost()) || "0.0.0.0".equals(sr.getSourceHost())) {
                                        sr.setSourceHost(peer.getHost());
                                    }
                                }
                                results.addAll(forwardedResults);
                            }
                        }
                    }
                } catch (Exception ignored) {}
            }
        }

        return results;
    }
}