#pragma once

#include "mini_dds/rtps/Types.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace mini_dds::rtps {

inline constexpr std::array<std::uint8_t, 4> protocol_magic{{'R', 'T', 'P', 'S'}};
inline constexpr std::size_t message_header_size = 20;
inline constexpr std::size_t submessage_header_size = 4;

struct MessageHeader {
    ProtocolVersion version;
    VendorId vendor_id;
    GuidPrefix guid_prefix;
};

enum class SubmessageKind : std::uint8_t {
    pad = 0x01,
    acknack = 0x06,
    heartbeat = 0x07,
    gap = 0x08,
    info_timestamp = 0x09,
    info_source = 0x0c,
    info_reply_ip4 = 0x0d,
    info_destination = 0x0e,
    info_reply = 0x0f,
    nack_frag = 0x12,
    heartbeat_frag = 0x13,
    data = 0x15,
    data_frag = 0x16,
};

struct SubmessageHeader {
    SubmessageKind kind{SubmessageKind::pad};
    std::uint8_t flags{0};
    std::uint16_t length{0};

    [[nodiscard]] bool little_endian() const noexcept
    {
        return (flags & 0x01U) != 0;
    }
};

std::array<std::uint8_t, message_header_size> encode_message_header(
    const MessageHeader& header);

bool decode_message_header(
    const std::uint8_t* data,
    std::size_t size,
    MessageHeader& header);

bool decode_message_header(
    const std::vector<std::uint8_t>& data,
    MessageHeader& header);

std::array<std::uint8_t, submessage_header_size> encode_submessage_header(
    const SubmessageHeader& header);

bool decode_submessage_header(
    const std::uint8_t* data,
    std::size_t size,
    SubmessageHeader& header);

} // namespace mini_dds::rtps
