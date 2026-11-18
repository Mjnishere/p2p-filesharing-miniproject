#pragma once

#include "Socket.hpp"
#include "TcpServer.hpp"
#include "TcpClient.hpp"
#include "Framing.hpp"

#include <string>
#include <memory>
#include <atomic>
#include <unordered_map>
#include <mutex>

namespace p2p {

struct NodeStats {
    uint64_t bytes_sent{0};
    uint64_t bytes_received{0};
    uint64_t frames_sent{0};
    uint64_t frames_received{0};
    uint32_t active_inbound_connections{0};
};

class PeerNode {
public:
    explicit PeerNode(std::string peer_id, int listen_port);
    ~PeerNode();

    // Prevent copying
    PeerNode(const PeerNode&) = delete;
    PeerNode& operator=(const PeerNode&) = delete;

    // Start server listener
    bool start();
    void stop();

    bool is_running() const;
    std::string get_peer_id() const;
    int get_listen_port() const;

    // Send a message to a remote peer with automatic connection establishment
    bool send_message(const std::string& target_host, int target_port, const P2PMessage& msg, P2PMessage* out_reply = nullptr, int timeout_ms = 3000);

    // Send a handshake (HELLO -> HELLO_ACK)
    bool ping_peer(const std::string& target_host, int target_port, int64_t* out_rtt_ms = nullptr);

    // Direct high-throughput raw socket binary stream benchmark
    struct StreamBenchmarkResult {
        size_t bytes_transferred{0};
        double elapsed_ms{0.0};
        double throughput_mbps{0.0};
        uint32_t crc32{0};
        bool success{false};
    };

    StreamBenchmarkResult send_benchmark_stream(const std::string& target_host, int target_port, size_t total_bytes);

    NodeStats get_stats() const;

private:
    void handle_inbound_connection(Socket socket);

    std::string peer_id_;
    int listen_port_;
    TcpServer server_;
    std::atomic<bool> is_running_;

    mutable std::mutex stats_mutex_;
    NodeStats stats_;
};

} // namespace p2p
