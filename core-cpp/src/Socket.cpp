#include "Socket.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <cstring>
#include <cerrno>
#include <iostream>

namespace p2p {

Socket::Socket() : fd_(-1) {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
        throw SocketException("Failed to create socket: " + std::string(std::strerror(errno)));
    }
}

Socket::Socket(int fd) : fd_(fd) {}

Socket::~Socket() {
    close();
}

Socket::Socket(Socket&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

void Socket::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool Socket::is_valid() const {
    return fd_ >= 0;
}

int Socket::get_fd() const {
    return fd_;
}

void Socket::set_tcp_nodelay(bool enable) {
    if (!is_valid()) return;
    int flag = enable ? 1 : 0;
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&flag), sizeof(flag));
}

void Socket::set_reuse_addr(bool enable) {
    if (!is_valid()) return;
    int flag = enable ? 1 : 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&flag), sizeof(flag));
}

void Socket::set_reuse_port(bool enable) {
    if (!is_valid()) return;
#ifdef SO_REUSEPORT
    int flag = enable ? 1 : 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, reinterpret_cast<const char*>(&flag), sizeof(flag));
#endif
}

void Socket::set_nonblocking(bool enable) {
    if (!is_valid()) return;
    int flags = ::fcntl(fd_, F_GETFL, 0);
    if (flags < 0) return;
    if (enable) {
        flags |= O_NONBLOCK;
    } else {
        flags &= ~O_NONBLOCK;
    }
    ::fcntl(fd_, F_SETFL, flags);
}

void Socket::set_timeout(int timeout_ms) {
    if (!is_valid() || timeout_ms <= 0) return;
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&tv), sizeof(tv));
    ::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&tv), sizeof(tv));
}

void Socket::set_buffer_sizes(int rcv_size, int snd_size) {
    if (!is_valid()) return;
    if (rcv_size > 0) {
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&rcv_size), sizeof(rcv_size));
    }
    if (snd_size > 0) {
        ::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&snd_size), sizeof(snd_size));
    }
}

bool Socket::send_exact(const void* data, size_t length) {
    if (!is_valid() || data == nullptr) return false;
    const uint8_t* ptr = static_cast<const uint8_t*>(data);
    size_t total_sent = 0;

    while (total_sent < length) {
        ssize_t sent = ::send(fd_, ptr + total_sent, length - total_sent, 0);
        if (sent <= 0) {
            if (sent < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) {
                continue;
            }
            return false;
        }
        total_sent += static_cast<size_t>(sent);
    }
    return true;
}

bool Socket::recv_exact(void* buffer, size_t length) {
    if (!is_valid() || buffer == nullptr) return false;
    uint8_t* ptr = static_cast<uint8_t*>(buffer);
    size_t total_received = 0;

    while (total_received < length) {
        ssize_t recvd = ::recv(fd_, ptr + total_received, length - total_received, 0);
        if (recvd <= 0) {
            if (recvd < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) {
                continue;
            }
            return false; // Connection closed or socket error
        }
        total_received += static_cast<size_t>(recvd);
    }
    return true;
}

bool Socket::send_frame(const std::string& payload) {
    return send_frame(reinterpret_cast<const uint8_t*>(payload.data()), static_cast<uint32_t>(payload.size()));
}

bool Socket::send_frame(const uint8_t* data, uint32_t length) {
    if (length > MAX_FRAME_SIZE) {
        return false;
    }
    uint32_t net_len = htonl(length);
    if (!send_exact(&net_len, sizeof(net_len))) {
        return false;
    }
    if (length > 0 && data != nullptr) {
        return send_exact(data, length);
    }
    return true;
}

bool Socket::recv_frame(std::string& out_payload) {
    std::vector<uint8_t> buffer;
    if (!recv_frame(buffer)) {
        return false;
    }
    out_payload.assign(reinterpret_cast<const char*>(buffer.data()), buffer.size());
    return true;
}

bool Socket::recv_frame(std::vector<uint8_t>& out_payload) {
    uint32_t net_len = 0;
    if (!recv_exact(&net_len, sizeof(net_len))) {
        return false;
    }
    uint32_t length = ntohl(net_len);
    if (length > MAX_FRAME_SIZE) {
        return false; // Frame size exceeded safety limit
    }

    out_payload.resize(length);
    if (length > 0) {
        if (!recv_exact(out_payload.data(), length)) {
            out_payload.clear();
            return false;
        }
    }
    return true;
}

std::string Socket::get_peer_ip() const {
    if (!is_valid()) return "";
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    if (::getpeername(fd_, reinterpret_cast<struct sockaddr*>(&addr), &addr_len) == 0) {
        char ip[INET_ADDRSTRLEN];
        if (::inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip)) != nullptr) {
            return std::string(ip);
        }
    }
    return "";
}

int Socket::get_peer_port() const {
    if (!is_valid()) return -1;
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    if (::getpeername(fd_, reinterpret_cast<struct sockaddr*>(&addr), &addr_len) == 0) {
        return ntohs(addr.sin_port);
    }
    return -1;
}

} // namespace p2p
