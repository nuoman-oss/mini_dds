#include "mini_dds/rtps/Message.h"

#include <algorithm>

namespace mini_dds::rtps {

std::array<std::uint8_t, message_header_size> encode_message_header(
    const MessageHeader& header)
{
    std::array<std::uint8_t, message_header_size> bytes{};
    std::copy(protocol_magic.begin(), protocol_magic.end(), bytes.begin());
    bytes[4] = header.version.major;
    bytes[5] = header.version.minor;
    bytes[6] = header.vendor_id.value[0];
    bytes[7] = header.vendor_id.value[1];
    std::copy(
        header.guid_prefix.value.begin(),
        header.guid_prefix.value.end(),
        bytes.begin() + 8);
    return bytes;
}

bool decode_message_header(
    const std::uint8_t* data,
    std::size_t size,
    MessageHeader& header)
{
    if (data == nullptr || size < message_header_size ||
        !std::equal(protocol_magic.begin(), protocol_magic.end(), data)) {
        return false;
    }

    header.version = ProtocolVersion{data[4], data[5]};
    header.vendor_id = VendorId{{data[6], data[7]}};
    std::copy(data + 8, data + message_header_size, header.guid_prefix.value.begin());
    return true;
}

bool decode_message_header(
    const std::vector<std::uint8_t>& data,
    MessageHeader& header)
{
    return decode_message_header(data.data(), data.size(), header);
}

std::array<std::uint8_t, submessage_header_size> encode_submessage_header(
    const SubmessageHeader& header)
{
    std::array<std::uint8_t, submessage_header_size> bytes{};
    bytes[0] = static_cast<std::uint8_t>(header.kind);
    bytes[1] = header.flags;

    if (header.little_endian()) {
        bytes[2] = static_cast<std::uint8_t>(header.length);
        bytes[3] = static_cast<std::uint8_t>(header.length >> 8U);
    } else {
        bytes[2] = static_cast<std::uint8_t>(header.length >> 8U);
        bytes[3] = static_cast<std::uint8_t>(header.length);
    }

    return bytes;
}

bool decode_submessage_header(
    const std::uint8_t* data,
    std::size_t size,
    SubmessageHeader& header)
{
    if (data == nullptr || size < submessage_header_size) {
        return false;
    }

    header.kind = static_cast<SubmessageKind>(data[0]);
    header.flags = data[1];

    if (header.little_endian()) {
        header.length = static_cast<std::uint16_t>(
            data[2] | (static_cast<std::uint16_t>(data[3]) << 8U));
    } else {
        header.length = static_cast<std::uint16_t>(
            (static_cast<std::uint16_t>(data[2]) << 8U) | data[3]);
    }

    return true;
}

} // namespace mini_dds::rtps
