#pragma once

#include "Socket.hpp"
#include <functional>
#include <thread>
#include <atomic>
#include <vector>
#include <mutex>

namespace p2p {

using ConnectionHandler = std::function<void(Socket peer_socket)>;

class TcpServer {
public:
    TcpServer();
    ~TcpServer();

    // Prevent copying
    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // Start listening on host and port. Dispatches accepted sockets to handler callback.
    bool start(int port, const std::string& host = "0.0.0.0", ConnectionHandler handler = nullptr);
    void stop();

    bool is_running() const;
    int get_port() const;
    int get_active_connections() const;

private:
    void accept_loop();

    Socket server_socket_;
    int port_;
    std::string host_;
    std::atomic<bool> is_running_;
    ConnectionHandler handler_;
    std::thread accept_thread_;
    std::vector<std::thread> worker_threads_;
    std::mutex workers_mutex_;
    std::atomic<int> active_connections_;
};

} // namespace p2p
