package com.example.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import modell.NetworkMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
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

    private ServerSocket serverSocket;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile boolean isRunning = true;

    // Slide 4: Visited / request cache to suppress duplicate searches
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
                    System.err.println("TCP Server socket error: " + e.getMessage());
                }
            }
        });
    }

    private void handleIncomingConnection(Socket socket) {
        try (DataInputStream dis = new DataInputStream(socket.getInputStream());
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            while (isRunning && !socket.isClosed()) {
                String rawJson = FramingUtil.readFrame(dis);
                NetworkMessage msg = objectMapper.readValue(rawJson, NetworkMessage.class);

                // Duplicate Request Suppression (Slide 4 & 8)
                if (msg.getRequestId() != null) {
                    if (processedRequestIds.contains(msg.getRequestId())) {
                        System.out.println(">>> Suppressed duplicate request: " + msg.getRequestId());
                        continue;
                    }
                    processedRequestIds.add(msg.getRequestId());
                }

                // Protocol Handshake (Slide 3)
                if ("HELLO".equals(msg.getType())) {
                    NetworkMessage reply = new NetworkMessage("HELLO_ACK", myPeerId, "Connection established");
                    FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(reply));
                }
            }
        } catch (IOException ignored) {
            // Connection closed by peer
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
        return processedRequestIds.contains(requestId);
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