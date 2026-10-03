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

    // Active outbound peer connections (peerId -> DataOutputStream)
    private final ConcurrentHashMap<String, DataOutputStream> activeOutbound = new ConcurrentHashMap<>();

    private void trackOutboundConnection(String peerKey, DataOutputStream outputStream) {
        if (peerKey != null && !peerKey.isBlank() && outputStream != null) {
            activeOutbound.put(peerKey, outputStream);
        }
    }

    private void untrackOutboundConnection(String peerKey) {
        if (peerKey != null && !peerKey.isBlank()) {
            activeOutbound.remove(peerKey);
        }
    }

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
                System.out.println(">>> Received TCP Message: [" + msg.getType() + "] from " + msg.getSenderPeerId());

                // Respond to HELLO handshake
                if ("HELLO".equals(msg.getType())) {
                    NetworkMessage reply = new NetworkMessage("HELLO_ACK", myPeerId, "Connection established");
                    FramingUtil.writeFrame(dos, objectMapper.writeValueAsString(reply));
                }
            }
        } catch (IOException e) {
            // Socket closed or connection terminated by peer
        }
    }

    public String sendDirectMessage(String targetIp, int targetPort, NetworkMessage message) throws IOException {
        String peerKey = targetIp + ":" + targetPort;
        try (Socket socket = new Socket(targetIp, targetPort);
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
             DataInputStream dis = new DataInputStream(socket.getInputStream())) {

            trackOutboundConnection(peerKey, dos);
            try {
                // Send framed JSON message
                String rawJson = objectMapper.writeValueAsString(message);
                FramingUtil.writeFrame(dos, rawJson);

                // Read response
                return FramingUtil.readFrame(dis);
            } finally {
                untrackOutboundConnection(peerKey);
            }
        }
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