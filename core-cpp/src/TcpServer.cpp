#include "TcpServer.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <algorithm>

namespace p2p {

TcpServer::TcpServer()
    : port_(0), is_running_(false), active_connections_(0) {}

TcpServer::~TcpServer() {
    stop();
}

bool TcpServer::start(int port, const std::string& host, ConnectionHandler handler) {
    if (is_running_.load()) {
        return false;
    }

    port_ = port;
    host_ = host;
    handler_ = std::move(handler);

    server_socket_.set_reuse_addr(true);
    server_socket_.set_reuse_port(true);

    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port_));

    if (host_.empty() || host_ == "0.0.0.0") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) <= 0) {
            std::cerr << "[TcpServer] Invalid host IP address: " << host_ << std::endl;
            return false;
        }
    }

    if (::bind(server_socket_.get_fd(), reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "[TcpServer] Failed to bind to " << host_ << ":" << port_ 
                  << " - " << std::strerror(errno) << std::endl;
        return false;
    }

    // Retrieve actual bound port if port 0 was passed
    if (port_ == 0) {
        struct sockaddr_in bound_addr;
        socklen_t len = sizeof(bound_addr);
        if (::getsockname(server_socket_.get_fd(), reinterpret_cast<struct sockaddr*>(&bound_addr), &len) == 0) {
            port_ = ntohs(bound_addr.sin_port);
        }
    }

    if (::listen(server_socket_.get_fd(), 128) < 0) {
        std::cerr << "[TcpServer] Failed to listen on socket - " << std::strerror(errno) << std::endl;
        return false;
    }

    is_running_.store(true);
    accept_thread_ = std::thread(&TcpServer::accept_loop, this);

    return true;
}

void TcpServer::stop() {
    if (!is_running_.exchange(false)) {
        return;
    }

    server_socket_.close();

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    std::vector<std::thread> workers_to_join;
    {
        std::lock_guard<std::mutex> lock(workers_mutex_);
        workers_to_join = std::move(worker_threads_);
    }

    for (auto& t : workers_to_join) {
        if (t.joinable()) {
            t.join();
        }
    }
}

bool TcpServer::is_running() const {
    return is_running_.load();
}

int TcpServer::get_port() const {
    return port_;
}

int TcpServer::get_active_connections() const {
    return active_connections_.load();
}

void TcpServer::accept_loop() {
    while (is_running_.load()) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = ::accept(server_socket_.get_fd(),
                                 reinterpret_cast<struct sockaddr*>(&client_addr),
                                 &client_len);

        if (client_fd < 0) {
            if (!is_running_.load()) {
                break; // Server was stopped
            }
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            std::cerr << "[TcpServer] Accept failed: " << std::strerror(errno) << std::endl;
            break;
        }

        Socket client_sock(client_fd);
        client_sock.set_tcp_nodelay(true);
        client_sock.set_buffer_sizes(65536, 65536);

        if (handler_) {
            active_connections_++;
            std::lock_guard<std::mutex> lock(workers_mutex_);
            worker_threads_.emplace_back([this, s = std::move(client_sock)]() mutable {
                try {
                    handler_(std::move(s));
                } catch (const std::exception& e) {
                    std::cerr << "[TcpServer] Client handler exception: " << e.what() << std::endl;
                } catch (...) {
                    std::cerr << "[TcpServer] Unknown client handler exception" << std::endl;
                }
                active_connections_--;
            });
        }
    }
}

} // namespace p2p
