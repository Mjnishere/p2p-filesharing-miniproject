#include "Socket.hpp"
#include "TcpServer.hpp"
#include "TcpClient.hpp"
#include "Framing.hpp"
#include "PeerNode.hpp"

#include <iostream>
#include <cassert>
#include <chrono>
#include <thread>
#include <future>
#include <vector>

void test_framing_codec() {
    std::cout << "[Test 1/5] Testing 4-byte length-prefixed framing codec..." << std::endl;
    std::string test_msg = "Hello P2P Bare-Metal C++ Socket!";
    std::vector<uint8_t> frame = p2p::Framing::encode_frame(test_msg);

    assert(frame.size() == 4 + test_msg.size());
    uint32_t len = 0;
    std::memcpy(&len, frame.data(), 4);
    assert(ntohl(len) == test_msg.size());

    uint32_t crc1 = p2p::Framing::compute_crc32(test_msg.data(), test_msg.size());
    uint32_t crc2 = p2p::Framing::compute_crc32(test_msg.data(), test_msg.size());
    assert(crc1 == crc2 && crc1 != 0);

    std::cout << "  ✓ Framing codec & CRC32 verification passed." << std::endl;
}

void test_direct_raw_socket_transfer() {
    std::cout << "[Test 2/5] Testing direct raw TCP socket client-server transmission..." << std::endl;

    p2p::TcpServer server;
    std::promise<std::string> server_promise;
    auto server_future = server_promise.get_future();

    bool started = server.start(0, "127.0.0.1", [&](p2p::Socket client_sock) {
        std::string frame;
        if (client_sock.recv_frame(frame)) {
            server_promise.set_value(frame);
            client_sock.send_frame("SERVER_ACK: " + frame);
        }
    });
    assert(started);
    int port = server.get_port();
    assert(port > 0);

    p2p::TcpClient client;
    bool connected = client.connect("127.0.0.1", port, 2000);
    assert(connected);

    std::string sent_msg = "RAW_SOCKET_PAYLOAD_TEST_42";
    bool sent = client.send_frame(sent_msg);
    assert(sent);

    std::string received_by_server = server_future.get();
    assert(received_by_server == sent_msg);

    std::string reply_from_server;
    bool got_reply = client.recv_frame(reply_from_server);
    assert(got_reply);
    assert(reply_from_server == "SERVER_ACK: " + sent_msg);

    client.disconnect();
    server.stop();
    std::cout << "  ✓ Raw socket client-to-server framing exchange passed on port " << port << "." << std::endl;
}

void test_peer_node_handshake_and_ping() {
    std::cout << "[Test 3/5] Testing PeerNode bidirectional handshake (HELLO / PING)..." << std::endl;

    p2p::PeerNode node_a("node-alpha", 0);
    p2p::PeerNode node_b("node-beta", 0);

    assert(node_a.start());
    assert(node_b.start());

    int port_b = node_b.get_listen_port();

    // Node A pings Node B
    int64_t rtt_ms = -1;
    bool ping_ok = node_a.ping_peer("127.0.0.1", port_b, &rtt_ms);
    assert(ping_ok);
    assert(rtt_ms >= 0);
    std::cout << "  ✓ PING -> PONG round trip: " << rtt_ms << " ms" << std::endl;

    // Node A sends HELLO to Node B
    p2p::P2PMessage hello;
    hello.type = "HELLO";
    hello.peer_id = node_a.get_peer_id();

    p2p::P2PMessage ack;
    bool hello_ok = node_a.send_message("127.0.0.1", port_b, hello, &ack, 2000);
    assert(hello_ok);
    assert(ack.type == "HELLO_ACK");
    assert(ack.peer_id == "node-beta");
    std::cout << "  ✓ HELLO -> HELLO_ACK handshake validated from " << ack.peer_id << std::endl;

    node_a.stop();
    node_b.stop();
}

void test_high_speed_raw_binary_stream() {
    std::cout << "[Test 4/5] Testing high-speed socket-to-socket raw binary stream throughput..." << std::endl;

    p2p::PeerNode seeder("seeder-node", 0);
    p2p::PeerNode leecher("leecher-node", 0);

    assert(seeder.start());
    assert(leecher.start());

    size_t stream_size = 5 * 1024 * 1024; // 5 MB raw binary stream
    auto result = leecher.send_benchmark_stream("127.0.0.1", seeder.get_listen_port(), stream_size);

    assert(result.success);
    assert(result.bytes_transferred == stream_size);
    std::cout << "  ✓ Transferred " << (stream_size / (1024 * 1024)) << " MB in " 
              << result.elapsed_ms << " ms (~" << result.throughput_mbps << " Mbps) with CRC32: 0x"
              << std::hex << result.crc32 << std::dec << std::endl;

    seeder.stop();
    leecher.stop();
}

void test_concurrent_connections_stress() {
    std::cout << "[Test 5/5] Testing concurrent multi-connection socket stress..." << std::endl;

    p2p::PeerNode server_node("stress-server", 0);
    assert(server_node.start());
    int port = server_node.get_listen_port();

    const int num_clients = 20;
    std::vector<std::future<bool>> futures;

    for (int i = 0; i < num_clients; ++i) {
        futures.push_back(std::async(std::launch::async, [i, port]() {
            p2p::TcpClient client;
            if (!client.connect("127.0.0.1", port, 3000)) return false;

            p2p::P2PMessage msg;
            msg.type = "DATA";
            msg.peer_id = "client-" + std::to_string(i);
            msg.payload = "PACKET_BURST_" + std::to_string(i * 100);

            if (!client.send_frame(msg.serialize())) return false;

            std::string reply_raw;
            if (!client.recv_frame(reply_raw)) return false;

            p2p::P2PMessage reply = p2p::P2PMessage::deserialize(reply_raw);
            return (reply.type == "DATA_ACK");
        }));
    }

    for (auto& f : futures) {
        assert(f.get() == true);
    }

    auto stats = server_node.get_stats();
    assert(stats.frames_received >= num_clients);
    std::cout << "  ✓ All " << num_clients << " concurrent raw socket connections completed successfully." << std::endl;

    server_node.stop();
}

int main() {
    std::cout << "==========================================================" << std::endl;
    std::cout << "    Bare-Metal C++ Raw Socket-to-Socket Test Suite        " << std::endl;
    std::cout << "==========================================================" << std::endl;

    try {
        test_framing_codec();
        test_direct_raw_socket_transfer();
        test_peer_node_handshake_and_ping();
        test_high_speed_raw_binary_stream();
        test_concurrent_connections_stress();

        std::cout << "\n>>> ALL 5 BARE-METAL C++ RAW SOCKET TESTS PASSED! <<<\n" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n[FATAL ERROR] Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
