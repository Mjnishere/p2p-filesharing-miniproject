#include "PeerNode.hpp"

#include <iostream>
#include <csignal>
#include <thread>
#include <chrono>
#include <unistd.h>

static std::atomic<bool> g_running{true};

void handle_signal(int) {
    g_running.store(false);
}

int main(int argc, char* argv[]) {
    int port = 9101;
    std::string peer_id = "cpp-peer-server";

    if (argc >= 2) {
        port = std::stoi(argv[1]);
    }
    if (argc >= 3) {
        peer_id = argv[2];
    }

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    std::cout << "==========================================================" << std::endl;
    std::cout << " Starting Bare-Metal C++ Peer Server Node [" << peer_id << "]" << std::endl;
    std::cout << " Port: " << port << " | PID: " << ::getpid() << std::endl;
    std::cout << "==========================================================" << std::endl;

    p2p::PeerNode node(peer_id, port);
    if (!node.start()) {
        std::cerr << "[ERROR] Could not start server on port " << port << std::endl;
        return 1;
    }

    std::cout << "[INFO] Listening for raw socket peer connections on port " << node.get_listen_port() << "..." << std::endl;
    std::cout << "[INFO] Press Ctrl+C to terminate server." << std::endl;

    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    std::cout << "\n[INFO] Shutting down peer server node..." << std::endl;
    auto stats = node.get_stats();
    std::cout << "Final Stats: Frames In=" << stats.frames_received 
              << ", Frames Out=" << stats.frames_sent
              << ", Bytes In=" << stats.bytes_received 
              << ", Bytes Out=" << stats.bytes_sent << std::endl;

    node.stop();
    std::cout << "[INFO] Peer server cleanly stopped." << std::endl;
    return 0;
}
