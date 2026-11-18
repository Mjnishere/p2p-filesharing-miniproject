#pragma once

#include <string>
#include <vector>
#include <cstdint>

namespace p2p {

/**
 * Message types supported across the P2P raw socket mesh.
 */
enum class MessageType {
    UNKNOWN,
    HELLO,
    HELLO_ACK,
    PING,
    PONG,
    SEARCH,
    SEARCH_RESULT,
    STREAM_REQ,
    STREAM_RES,
    CHUNK_REQ,
    CHUNK_RES
};

struct P2PMessage {
    std::string type;
    std::string peer_id;
    std::string payload;
    uint32_t sequence{0};
    uint64_t timestamp_ms{0};

    std::string serialize() const;
    static P2PMessage deserialize(const std::string& raw);
};

class Framing {
public:
    static constexpr uint32_t HEADER_SIZE = 4;
    static constexpr uint32_t MAX_PAYLOAD_SIZE = 32 * 1024 * 1024; // 32 MB

    // Encodes a payload into a wire-ready framed buffer (4-byte length prefix + payload)
    static std::vector<uint8_t> encode_frame(const void* data, uint32_t length);
    static std::vector<uint8_t> encode_frame(const std::string& str);

    // Fast hardware-friendly CRC32 computation for verifying raw socket transfers
    static uint32_t compute_crc32(const void* data, size_t length);

    // Converts string message type to enum
    static MessageType parse_message_type(const std::string& type_str);
    static std::string message_type_to_string(MessageType type);
};

} // namespace p2p
