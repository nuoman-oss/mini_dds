#include "mini_dds/rtps/Data.h"

#include "mini_dds/serialization/Cdr.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

namespace mini_dds::rtps {
namespace {

std::int32_t sequence_high(SequenceNumber sequence_number)
{
    const auto bits = static_cast<std::uint64_t>(sequence_number.value);
    return static_cast<std::int32_t>(bits >> 32U);
}

std::uint32_t sequence_low(SequenceNumber sequence_number)
{
    return static_cast<std::uint32_t>(sequence_number.value);
}

SequenceNumber make_sequence_number(std::int32_t high, std::uint32_t low)
{
    const std::uint64_t bits =
        (static_cast<std::uint64_t>(static_cast<std::uint32_t>(high)) << 32U) |
        low;
    return SequenceNumber{static_cast<std::int64_t>(bits)};
}

bool copy_entity_id(
    serialization::CdrReader& reader,
    EntityId& entity_id)
{
    std::vector<std::uint8_t> bytes;
    if (!reader.read_bytes(entity_id.value.size(), bytes)) {
        return false;
    }
    std::copy(bytes.begin(), bytes.end(), entity_id.value.begin());
    return true;
}

} // namespace

bool encode_data_submessage(
    const DataSubmessage& data,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error)
{
    error.clear();
    bytes.clear();

    if (data.writer_sequence_number.value <= 0) {
        error = "RTPS DATA sequence number must be positive";
        return false;
    }

    serialization::CdrWriter content(endianness);
    content.write_uint16(0);
    content.write_uint16(data_octets_to_inline_qos);
    content.write_bytes(std::vector<std::uint8_t>(
        data.reader_id.value.begin(), data.reader_id.value.end()));
    content.write_bytes(std::vector<std::uint8_t>(
        data.writer_id.value.begin(), data.writer_id.value.end()));
    content.write_int32(sequence_high(data.writer_sequence_number));
    content.write_uint32(sequence_low(data.writer_sequence_number));
    content.write_bytes(data.serialized_payload);

    if (content.data().size() > std::numeric_limits<std::uint16_t>::max()) {
        error = "RTPS DATA submessage is too large; fragmentation is not implemented";
        return false;
    }

    const std::uint8_t flags =
        (endianness == serialization::Endianness::little ? 0x01U : 0x00U) |
        data_flag_data;
    const SubmessageHeader header{
        SubmessageKind::data,
        flags,
        static_cast<std::uint16_t>(content.data().size())};
    const auto encoded_header = encode_submessage_header(header);

    bytes.insert(bytes.end(), encoded_header.begin(), encoded_header.end());
    const auto& encoded_content = content.data();
    bytes.insert(bytes.end(), encoded_content.begin(), encoded_content.end());
    return true;
}

bool decode_data_submessage(
    const std::uint8_t* bytes,
    std::size_t size,
    DataSubmessage& data,
    std::string& error)
{
    error.clear();

    SubmessageHeader header;
    if (!decode_submessage_header(bytes, size, header)) {
        error = "truncated RTPS submessage header";
        return false;
    }

    if (header.kind != SubmessageKind::data) {
        error = "RTPS submessage is not DATA";
        return false;
    }

    if ((header.flags & data_flag_data) == 0 ||
        (header.flags & data_flag_key) != 0) {
        error = "unsupported RTPS DATA/KEY flag combination";
        return false;
    }

    if ((header.flags & (data_flag_inline_qos | data_flag_non_standard_payload)) != 0) {
        error = "inline QoS and non-standard payloads are not implemented";
        return false;
    }

    const std::size_t content_size = header.length == 0
        ? size - submessage_header_size
        : header.length;
    if (content_size < data_fixed_content_size ||
        size < submessage_header_size + content_size) {
        error = "truncated RTPS DATA content";
        return false;
    }

    const auto endianness = header.little_endian()
        ? serialization::Endianness::little
        : serialization::Endianness::big;
    serialization::CdrReader reader(
        bytes + submessage_header_size,
        content_size,
        endianness);

    std::uint16_t extra_flags = 0;
    std::uint16_t octets_to_inline_qos = 0;
    std::int32_t sequence_number_high = 0;
    std::uint32_t sequence_number_low = 0;

    if (!reader.read_uint16(extra_flags) ||
        !reader.read_uint16(octets_to_inline_qos) ||
        !copy_entity_id(reader, data.reader_id) ||
        !copy_entity_id(reader, data.writer_id) ||
        !reader.read_int32(sequence_number_high) ||
        !reader.read_uint32(sequence_number_low)) {
        error = "invalid RTPS DATA fixed fields";
        return false;
    }

    if (extra_flags != 0) {
        error = "unsupported RTPS DATA extra flags";
        return false;
    }

    if (octets_to_inline_qos < data_octets_to_inline_qos) {
        error = "invalid RTPS DATA octetsToInlineQos";
        return false;
    }

    const std::size_t payload_offset = 4U + octets_to_inline_qos;
    if (payload_offset > content_size) {
        error = "RTPS DATA octetsToInlineQos exceeds content length";
        return false;
    }

    data.writer_sequence_number =
        make_sequence_number(sequence_number_high, sequence_number_low);
    if (data.writer_sequence_number.value <= 0) {
        error = "RTPS DATA sequence number must be positive";
        return false;
    }

    data.serialized_payload.assign(
        bytes + submessage_header_size + payload_offset,
        bytes + submessage_header_size + content_size);
    return true;
}

bool encode_data_message(
    const DataMessage& message,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error)
{
    std::vector<std::uint8_t> submessage;
    if (!encode_data_submessage(message.data, endianness, submessage, error)) {
        bytes.clear();
        return false;
    }

    const auto header = encode_message_header(message.header);
    bytes.assign(header.begin(), header.end());
    bytes.insert(bytes.end(), submessage.begin(), submessage.end());
    return true;
}

bool decode_data_message(
    const std::vector<std::uint8_t>& bytes,
    DataMessage& message,
    std::string& error)
{
    error.clear();
    if (!decode_message_header(bytes, message.header)) {
        error = "invalid RTPS message header";
        return false;
    }

    if (bytes.size() <= message_header_size) {
        error = "RTPS message does not contain a submessage";
        return false;
    }

    return decode_data_submessage(
        bytes.data() + message_header_size,
        bytes.size() - message_header_size,
        message.data,
        error);
}

} // namespace mini_dds::rtps
