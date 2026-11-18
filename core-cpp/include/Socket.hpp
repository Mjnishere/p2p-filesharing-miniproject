#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <chrono>

namespace p2p {

/**
 * SocketException represents any low-level POSIX socket errors.
 */
class SocketException : public std::runtime_error {
public:
    explicit SocketException(const std::string& message)
        : std::runtime_error(message) {}
};

/**
 * Low-level POSIX RAII Socket wrapper.
 * Handles exact boundary framing, TCP_NODELAY, buffer sizing, and timeouts.
 */
class Socket {
public:
    static constexpr int DEFAULT_BUFFER_SIZE = 65536; // 64 KB
    static constexpr uint32_t MAX_FRAME_SIZE = 32 * 1024 * 1024; // 32 MB safety boundary

    Socket();
    explicit Socket(int fd);
    ~Socket();

    // Move-only semantics (no accidental duplicate fd closures)
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    // Lifecycle
    void close();
    bool is_valid() const;
    int get_fd() const;

    // Socket Configuration
    void set_tcp_nodelay(bool enable = true);
    void set_reuse_addr(bool enable = true);
    void set_reuse_port(bool enable = true);
    void set_nonblocking(bool enable = true);
    void set_timeout(int timeout_ms);
    void set_buffer_sizes(int rcv_size = DEFAULT_BUFFER_SIZE, int snd_size = DEFAULT_BUFFER_SIZE);

    // Exact I/O operations (handles partial transfers over TCP byte streams)
    bool send_exact(const void* data, size_t length);
    bool recv_exact(void* buffer, size_t length);

    // 4-byte big-endian length-prefixed framing (matches network wire standard)
    bool send_frame(const std::string& payload);
    bool send_frame(const uint8_t* data, uint32_t length);
    bool recv_frame(std::string& out_payload);
    bool recv_frame(std::vector<uint8_t>& out_payload);

    // Connection metadata
    std::string get_peer_ip() const;
    int get_peer_port() const;

private:
    int fd_;
};

} // namespace p2p
