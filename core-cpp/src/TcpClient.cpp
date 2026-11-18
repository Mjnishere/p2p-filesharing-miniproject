#include "TcpClient.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <poll.h>
#include <cstring>
#include <cerrno>
#include <iostream>

namespace p2p {

TcpClient::TcpClient()
    : port_(0), is_connected_(false) {}

TcpClient::~TcpClient() {
    disconnect();
}

bool TcpClient::connect(const std::string& host, int port, int timeout_ms) {
    disconnect();

    try {
        socket_ = Socket(); // Fresh socket descriptor
    } catch (const SocketException& e) {
        std::cerr << "[TcpClient] Failed to create socket: " << e.what() << std::endl;
        return false;
    }

    host_ = host;
    port_ = port;

    struct sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(static_cast<uint16_t>(port_));

    if (::inet_pton(AF_INET, host_.c_str(), &server_addr.sin_addr) <= 0) {
        std::cerr << "[TcpClient] Invalid remote host IP: " << host_ << std::endl;
        return false;
    }

    // Connect with bounded non-blocking timeout
    socket_.set_nonblocking(true);

    int res = ::connect(socket_.get_fd(), reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr));
    if (res < 0) {
        if (errno != EINPROGRESS) {
            std::cerr << "[TcpClient] Immediate connect failure to " << host_ << ":" << port_ 
                      << " - " << std::strerror(errno) << std::endl;
            socket_.close();
            return false;
        }

        struct pollfd pfd;
        pfd.fd = socket_.get_fd();
        pfd.events = POLLOUT;

        int poll_res = ::poll(&pfd, 1, timeout_ms);
        if (poll_res <= 0) {
            // Timeout (0) or error (< 0)
            socket_.close();
            return false;
        }

        int so_error = 0;
        socklen_t len = sizeof(so_error);
        if (::getsockopt(socket_.get_fd(), SOL_SOCKET, SO_ERROR, &so_error, &len) < 0 || so_error != 0) {
            socket_.close();
            return false;
        }
    }

    socket_.set_nonblocking(false);
    socket_.set_tcp_nodelay(true);
    socket_.set_buffer_sizes(65536, 65536);
    socket_.set_timeout(5000); // 5-second socket read/write timeout

    is_connected_ = true;
    return true;
}

void TcpClient::disconnect() {
    if (is_connected_) {
        socket_.close();
        is_connected_ = false;
    }
}

bool TcpClient::is_connected() const {
    return is_connected_ && socket_.is_valid();
}

Socket& TcpClient::get_socket() {
    return socket_;
}

Socket TcpClient::take_socket() {
    is_connected_ = false;
    return std::move(socket_);
}

bool TcpClient::send_frame(const std::string& payload) {
    if (!is_connected()) return false;
    return socket_.send_frame(payload);
}

bool TcpClient::recv_frame(std::string& out_payload) {
    if (!is_connected()) return false;
    return socket_.recv_frame(out_payload);
}

bool TcpClient::send_frame(const uint8_t* data, uint32_t length) {
    if (!is_connected()) return false;
    return socket_.send_frame(data, length);
}

bool TcpClient::recv_frame(std::vector<uint8_t>& out_payload) {
    if (!is_connected()) return false;
    return socket_.recv_frame(out_payload);
}

bool TcpClient::send_raw(const void* data, size_t length) {
    if (!is_connected()) return false;
    return socket_.send_exact(data, length);
}

bool TcpClient::recv_raw(void* buffer, size_t length) {
    if (!is_connected()) return false;
    return socket_.recv_exact(buffer, length);
}

std::string TcpClient::get_host() const {
    return host_;
}

int TcpClient::get_port() const {
    return port_;
}

} // namespace p2p
