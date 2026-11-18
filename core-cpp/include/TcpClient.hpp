#pragma once

#include "Socket.hpp"
#include <string>
#include <chrono>

namespace p2p {

class TcpClient {
public:
    TcpClient();
    ~TcpClient();

    // Prevent copying
    TcpClient(const TcpClient&) = delete;
    TcpClient& operator=(const TcpClient&) = delete;

    // Connect to remote peer with configurable timeout (in milliseconds)
    bool connect(const std::string& host, int port, int timeout_ms = 3000);
    void disconnect();

    bool is_connected() const;
    Socket& get_socket();
    Socket take_socket(); // Moves the underlying socket out

    // Direct framed messaging
    bool send_frame(const std::string& payload);
    bool recv_frame(std::string& out_payload);

    bool send_frame(const uint8_t* data, uint32_t length);
    bool recv_frame(std::vector<uint8_t>& out_payload);

    // Exact raw byte I/O
    bool send_raw(const void* data, size_t length);
    bool recv_raw(void* buffer, size_t length);

    std::string get_host() const;
    int get_port() const;

private:
    Socket socket_;
    std::string host_;
    int port_;
    bool is_connected_;
};

} // namespace p2p
