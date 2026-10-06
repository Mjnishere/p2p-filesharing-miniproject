package com.example.demo;

import com.example.demo.service.SearchManagerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import modell.NetworkMessage;
import modell.PeerInfo;
import modell.SearchResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class NetworkService {

    @Value("${peer.tcp.port:9001}")
    private int tcpPort;

    @Value("${peer.id:peer-1}")
    private String myPeerId;

    private final ApplicationContext applicationContext;

    private ServerSocket serverSocket;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile boolean isRunning = true;


    public NetworkService(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    private final Set<String> processedRequestIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @PostConstruct
    public void startServer() {
        threadPool.submit(() -> {
            try {
                serverSocket = new ServerSocket(tcpPort);
                System.out.println(">>> TCP Network Server listening on port: " + tcpPort);

                while (isRunning && !serverSocket.isClosed()) {
                    Socket clientSocket = serverSocket.accept();
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

    private void handleIncomingConnection(Socket socket) {
        try (DataInputStream dis = new DataInputStream(socket.getInputStream());
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            while (isRunning && !socket.isClosed()) {
                String rawJson = FramingUtil.readFrame(dis);
                NetworkMessage msg = objectMapper.readValue(rawJson, NetworkMessage.class);

                // Duplicate Request Suppression
                if (msg.getRequestId() != null) {
                    if (processedRequestIds.contains(msg.getRequestId())) {
                        System.out.println(">>> Suppressed duplicate request: " + msg.getRequestId());
                        continue;
                    }
                    processedRequestIds.add(msg.getRequestId());
                }

                if ("HELLO".equals(msg.getType())) {
                    NetworkMessage reply = new NetworkMessage("HELLO_ACK", myPeerId, "Connection established");
                    FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(reply));
                } else if ("JOIN".equals(msg.getType())) {
                    SearchManagerService sms = getSearchManagerService();
                    if (msg.getPayload() != null) {
                        PeerInfo incomingPeer = objectMapper.readValue(msg.getPayload(), PeerInfo.class);
                        sms.registerPeer(incomingPeer);
                    }
                    List<PeerInfo> known = sms.getKnownPeers();
                    NetworkMessage reply = new NetworkMessage("PEER_LIST", myPeerId, objectMapper.writeValueAsString(known));
                    FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(reply));
                } else if ("SEARCH".equals(msg.getType())) {
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
                }
            }
        } catch (IOException ignored) {
        }
    }

    public String sendDirectMessage(String targetIp, int targetPort, NetworkMessage message) throws IOException {
        try (Socket socket = new Socket(targetIp, targetPort);
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
             DataInputStream dis = new DataInputStream(socket.getInputStream())) {

            String rawJson = objectMapper.writeValueAsString(message);
            FramingUtil.writeFrame(dos, rawJson);
            return FramingUtil.readFrame(dis);
        }
    }

    public boolean hasProcessed(String requestId) {
        return !processedRequestIds.add(requestId);
    }

    @PreDestroy
    public void shutdown() {
        isRunning = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}
        threadPool.shutdownNow();
    }
}