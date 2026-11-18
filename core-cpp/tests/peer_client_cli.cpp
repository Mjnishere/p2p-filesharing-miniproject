#include "TcpClient.hpp"
#include "Framing.hpp"
#include "PeerNode.hpp"

#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int port = 9101;
    std::string action = "all"; // "ping", "hello", "stream", or "all"

    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = std::stoi(argv[2]);
    if (argc >= 4) action = argv[3];

    std::cout << "==========================================================" << std::endl;
    std::cout << " Bare-Metal C++ Raw Socket Client Diagnostic Tool        " << std::endl;
    std::cout << " Target: " << host << ":" << port << " | Action: " << action << std::endl;
    std::cout << "==========================================================" << std::endl;

    p2p::PeerNode client_node("cpp-client-diag", 0);

    if (action == "ping" || action == "all") {
        std::cout << "\n[1] Probing RTT (PING/PONG)..." << std::endl;
        int64_t rtt = -1;
        if (client_node.ping_peer(host, port, &rtt)) {
            std::cout << "  >>> SUCCESS: Peer responded to PING in " << rtt << " ms." << std::endl;
        } else {
            std::cerr << "  >>> FAILED: Peer did not respond to PING." << std::endl;
        }
    }

    if (action == "hello" || action == "all") {
        std::cout << "\n[2] Executing P2P Handshake (HELLO)..." << std::endl;
        p2p::P2PMessage hello;
        hello.type = "HELLO";
        hello.peer_id = "cpp-peer-client";

        p2p::P2PMessage reply;
        if (client_node.send_message(host, port, hello, &reply, 3000)) {
            std::cout << "  >>> SUCCESS: Received " << reply.type << " from peer [" 
                      << reply.peer_id << "] with payload: " << reply.payload << std::endl;
        } else {
            std::cerr << "  >>> FAILED: Handshake failed with " << host << ":" << port << std::endl;
        }
    }

    if (action == "stream" || action == "all") {
        std::cout << "\n[3] Testing Raw Socket Stream Throughput (5 MB payload)..." << std::endl;
        auto res = client_node.send_benchmark_stream(host, port, 5 * 1024 * 1024);
        if (res.success) {
            std::cout << "  >>> SUCCESS: Transferred 5 MB in " << res.elapsed_ms << " ms (" 
                      << res.throughput_mbps << " Mbps) | CRC32: 0x" << std::hex << res.crc32 << std::dec << std::endl;
        } else {
            std::cerr << "  >>> FAILED or Unsupported: Stream benchmark failed." << std::endl;
        }
    }

    std::cout << "\n[INFO] Client diagnostic completed." << std::endl;
    return 0;
}
