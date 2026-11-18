#include "PeerNode.hpp"

#include <iostream>
#include <chrono>

namespace p2p {

PeerNode::PeerNode(std::string peer_id, int listen_port)
    : peer_id_(std::move(peer_id)), listen_port_(listen_port), is_running_(false) {}

PeerNode::~PeerNode() {
    stop();
}

bool PeerNode::start() {
    if (is_running_.load()) return true;

    bool started = server_.start(listen_port_, "0.0.0.0", [this](Socket sock) {
        handle_inbound_connection(std::move(sock));
    });

    if (started) {
        listen_port_ = server_.get_port(); // Update with actual port in case port 0 was passed
        is_running_.store(true);
    }
    return started;
}

void PeerNode::stop() {
    if (!is_running_.exchange(false)) return;
    server_.stop();
}

bool PeerNode::is_running() const {
    return is_running_.load();
}

std::string PeerNode::get_peer_id() const {
    return peer_id_;
}

int PeerNode::get_listen_port() const {
    return listen_port_;
}

NodeStats PeerNode::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    NodeStats s = stats_;
    s.active_inbound_connections = static_cast<uint32_t>(server_.get_active_connections());
    return s;
}

void PeerNode::handle_inbound_connection(Socket socket) {
    while (socket.is_valid() && is_running_.load()) {
        std::string raw_frame;
        if (!socket.recv_frame(raw_frame)) {
            break; // Connection closed or socket error
        }

        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.frames_received++;
            stats_.bytes_received += (4 + raw_frame.size());
        }

        P2PMessage msg = P2PMessage::deserialize(raw_frame);

        if (msg.type == "HELLO") {
            P2PMessage reply;
            reply.type = "HELLO_ACK";
            reply.peer_id = peer_id_;
            reply.payload = "STATUS:ONLINE|PORT:" + std::to_string(listen_port_);

            std::string reply_str = reply.serialize();
            if (socket.send_frame(reply_str)) {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                stats_.frames_sent++;
                stats_.bytes_sent += (4 + reply_str.size());
            }
        } else if (msg.type == "PING") {
            P2PMessage reply;
            reply.type = "PONG";
            reply.peer_id = peer_id_;
            reply.sequence = msg.sequence;

            std::string reply_str = reply.serialize();
            if (socket.send_frame(reply_str)) {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                stats_.frames_sent++;
                stats_.bytes_sent += (4 + reply_str.size());
            }
        } else if (msg.type == "BENCHMARK_STREAM_REQ") {
            // Raw binary stream transfer test
            size_t stream_size = 0;
            try {
                stream_size = std::stoull(msg.payload);
            } catch (...) {
                stream_size = 1024 * 1024;
            }

            std::vector<uint8_t> buffer(Socket::DEFAULT_BUFFER_SIZE);
            size_t remaining = stream_size;
            uint32_t crc = 0;

            std::vector<uint8_t> stream_data;
            stream_data.reserve(stream_size);

            while (remaining > 0) {
                size_t to_read = std::min<size_t>(remaining, buffer.size());
                if (!socket.recv_exact(buffer.data(), to_read)) {
                    break;
                }
                stream_data.insert(stream_data.end(), buffer.begin(), buffer.begin() + to_read);
                remaining -= to_read;
            }

            crc = Framing::compute_crc32(stream_data.data(), stream_data.size());

            P2PMessage reply;
            reply.type = "BENCHMARK_STREAM_ACK";
            reply.peer_id = peer_id_;
            reply.payload = std::to_string(crc);
            std::string reply_str = reply.serialize();
            socket.send_frame(reply_str);
        } else {
            // Echo back for unhandled diagnostic types
            P2PMessage reply;
            reply.type = msg.type + "_ACK";
            reply.peer_id = peer_id_;
            reply.payload = "ECHO:" + msg.payload;
            std::string reply_str = reply.serialize();
            socket.send_frame(reply_str);
        }
    }
}

bool PeerNode::send_message(const std::string& target_host, int target_port, const P2PMessage& msg, P2PMessage* out_reply, int timeout_ms) {
    TcpClient client;
    if (!client.connect(target_host, target_port, timeout_ms)) {
        return false;
    }

    std::string serialized = msg.serialize();
    if (!client.send_frame(serialized)) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.frames_sent++;
        stats_.bytes_sent += (4 + serialized.size());
    }

    if (out_reply != nullptr) {
        std::string raw_reply;
        if (!client.recv_frame(raw_reply)) {
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.frames_received++;
            stats_.bytes_received += (4 + raw_reply.size());
        }

        *out_reply = P2PMessage::deserialize(raw_reply);
    }

    return true;
}

bool PeerNode::ping_peer(const std::string& target_host, int target_port, int64_t* out_rtt_ms) {
    auto start = std::chrono::high_resolution_clock::now();

    P2PMessage ping;
    ping.type = "PING";
    ping.peer_id = peer_id_;

    P2PMessage pong;
    if (!send_message(target_host, target_port, ping, &pong, 2000)) {
        return false;
    }

    auto end = std::chrono::high_resolution_clock::now();
    int64_t rtt = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    if (out_rtt_ms != nullptr) {
        *out_rtt_ms = rtt;
    }

    return (pong.type == "PONG");
}

PeerNode::StreamBenchmarkResult PeerNode::send_benchmark_stream(const std::string& target_host, int target_port, size_t total_bytes) {
    StreamBenchmarkResult res;
    res.bytes_transferred = total_bytes;

    TcpClient client;
    if (!client.connect(target_host, target_port, 3000)) {
        return res;
    }

    P2PMessage req;
    req.type = "BENCHMARK_STREAM_REQ";
    req.peer_id = peer_id_;
    req.payload = std::to_string(total_bytes);

    if (!client.send_frame(req.serialize())) {
        return res;
    }

    // Generate pseudo-random deterministic test data
    std::vector<uint8_t> payload(total_bytes);
    for (size_t i = 0; i < total_bytes; ++i) {
        payload[i] = static_cast<uint8_t>((i * 37 + 13) & 0xFF);
    }
    res.crc32 = Framing::compute_crc32(payload.data(), payload.size());

    auto start = std::chrono::high_resolution_clock::now();

    if (!client.send_raw(payload.data(), payload.size())) {
        return res;
    }

    std::string ack_raw;
    if (!client.recv_frame(ack_raw)) {
        return res;
    }

    auto end = std::chrono::high_resolution_clock::now();
    res.elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();

    if (res.elapsed_ms > 0) {
        double seconds = res.elapsed_ms / 1000.0;
        double mb = static_cast<double>(total_bytes) / (1024.0 * 1024.0);
        res.throughput_mbps = (mb * 8.0) / seconds;
    }

    P2PMessage ack = P2PMessage::deserialize(ack_raw);
    uint32_t remote_crc = 0;
    try {
        remote_crc = static_cast<uint32_t>(std::stoul(ack.payload));
    } catch (...) {}

    res.success = (ack.type == "BENCHMARK_STREAM_ACK" && remote_crc == res.crc32);
    return res;
}

} // namespace p2p
