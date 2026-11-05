package com.example.demo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class HealthController {

    @Value("${peer.id:peer-1}")
    private String peerId;

    @Value("${peer.tcp.port:9001}")
    private int tcpPort;

    @GetMapping("/health")
    public Map<String, Object> getHealth() {
        return Map.of(
            "status", "UP",
            "service", "P2P Peer Node",
            "peerId", peerId,
            "tcpPort", tcpPort
        );
    }
}
