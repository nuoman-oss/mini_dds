#include "mini_dds/rtps/Reliability.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

namespace mini_dds::rtps {
namespace {

constexpr std::uint8_t endianness_flag = 0x01;
constexpr std::uint8_t heartbeat_supported_flags =
    endianness_flag | heartbeat_flag_final | heartbeat_flag_liveliness;
constexpr std::uint8_t acknack_supported_flags =
    endianness_flag | acknack_flag_final;
constexpr std::uint8_t gap_supported_flags = endianness_flag;

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

void write_entity_id(serialization::CdrWriter& writer, const EntityId& entity_id)
{
    writer.write_bytes(std::vector<std::uint8_t>(
        entity_id.value.begin(), entity_id.value.end()));
}

bool read_entity_id(serialization::CdrReader& reader, EntityId& entity_id)
{
    std::vector<std::uint8_t> value;
    if (!reader.read_bytes(entity_id.value.size(), value)) {
        return false;
    }
    std::copy(value.begin(), value.end(), entity_id.value.begin());
    return true;
}

void write_sequence_number(
    serialization::CdrWriter& writer,
    SequenceNumber sequence_number)
{
    writer.write_int32(sequence_high(sequence_number));
    writer.write_uint32(sequence_low(sequence_number));
}

bool read_sequence_number(
    serialization::CdrReader& reader,
    SequenceNumber& sequence_number)
{
    std::int32_t high = 0;
    std::uint32_t low = 0;
    if (!reader.read_int32(high) || !reader.read_uint32(low)) {
        return false;
    }
    sequence_number = make_sequence_number(high, low);
    return true;
}

std::size_t required_bitmap_words(std::uint32_t num_bits)
{
    return (static_cast<std::size_t>(num_bits) + 31U) / 32U;
}

bool validate_sequence_number_set(
    const SequenceNumberSet& set,
    std::string& error)
{
    if (set.bitmap_base.value <= 0) {
        error = "SequenceNumberSet bitmapBase must be positive";
        return false;
    }
    if (set.num_bits > sequence_number_set_max_bits) {
        error = "SequenceNumberSet exceeds the RTPS 256-bit limit";
        return false;
    }
    if (set.bitmap.size() != required_bitmap_words(set.num_bits)) {
        error = "SequenceNumberSet bitmap size does not match numBits";
        return false;
    }
    const auto remaining_bits = set.num_bits % 32U;
    if (remaining_bits != 0 && !set.bitmap.empty()) {
        const auto unused_mask =
            (std::uint32_t{1} << (32U - remaining_bits)) - 1U;
        if ((set.bitmap.back() & unused_mask) != 0) {
            error = "SequenceNumberSet has non-zero bits beyond numBits";
            return false;
        }
    }
    return true;
}

void write_sequence_number_set(
    serialization::CdrWriter& writer,
    const SequenceNumberSet& set)
{
    write_sequence_number(writer, set.bitmap_base);
    writer.write_uint32(set.num_bits);
    for (const auto word : set.bitmap) {
        writer.write_uint32(word);
    }
}

bool read_sequence_number_set(
    serialization::CdrReader& reader,
    SequenceNumberSet& set,
    std::string& error)
{
    if (!read_sequence_number(reader, set.bitmap_base) ||
        !reader.read_uint32(set.num_bits)) {
        error = "truncated SequenceNumberSet";
        return false;
    }
    if (set.bitmap_base.value <= 0) {
        error = "SequenceNumberSet bitmapBase must be positive";
        return false;
    }
    if (set.num_bits > sequence_number_set_max_bits) {
        error = "SequenceNumberSet exceeds the RTPS 256-bit limit";
        return false;
    }

    set.bitmap.clear();
    set.bitmap.reserve(required_bitmap_words(set.num_bits));
    for (std::size_t index = 0; index < required_bitmap_words(set.num_bits); ++index) {
        std::uint32_t word = 0;
        if (!reader.read_uint32(word)) {
            error = "truncated SequenceNumberSet bitmap";
            return false;
        }
        set.bitmap.push_back(word);
    }
    return validate_sequence_number_set(set, error);
}

bool content_size(
    const SubmessageHeader& header,
    std::size_t size,
    std::size_t minimum,
    std::size_t& result,
    std::string& error)
{
    result = header.length == 0
        ? size - submessage_header_size
        : header.length;
    if (result < minimum || size < submessage_header_size + result) {
        error = "truncated RTPS reliability submessage";
        return false;
    }
    return true;
}

bool append_submessage(
    const MessageHeader& header,
    std::vector<std::uint8_t> submessage,
    std::vector<std::uint8_t>& bytes)
{
    const auto encoded_header = encode_message_header(header);
    bytes.assign(encoded_header.begin(), encoded_header.end());
    bytes.insert(bytes.end(), submessage.begin(), submessage.end());
    return true;
}

} // namespace

bool SequenceNumberSet::contains(SequenceNumber sequence_number) const noexcept
{
    if (sequence_number.value < bitmap_base.value) {
        return false;
    }
    const auto offset = static_cast<std::uint64_t>(
        sequence_number.value - bitmap_base.value);
    if (offset >= num_bits) {
        return false;
    }
    const auto word = static_cast<std::size_t>(offset / 32U);
    const auto bit = static_cast<std::uint32_t>(offset % 32U);
    return word < bitmap.size() &&
           (bitmap[word] & (std::uint32_t{1} << (31U - bit))) != 0;
}

bool encode_heartbeat_submessage(
    const HeartbeatSubmessage& heartbeat,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error)
{
    error.clear();
    bytes.clear();
    if (heartbeat.first_sequence_number.value <= 0 ||
        heartbeat.last_sequence_number.value < heartbeat.first_sequence_number.value) {
        error = "HEARTBEAT sequence-number range is invalid";
        return false;
    }
    if (heartbeat.count <= 0) {
        error = "HEARTBEAT count must be positive";
        return false;
    }

    serialization::CdrWriter content(endianness);
    write_entity_id(content, heartbeat.reader_id);
    write_entity_id(content, heartbeat.writer_id);
    write_sequence_number(content, heartbeat.first_sequence_number);
    write_sequence_number(content, heartbeat.last_sequence_number);
    content.write_int32(heartbeat.count);

    std::uint8_t flags = endianness == serialization::Endianness::little
        ? endianness_flag
        : 0;
    if (heartbeat.final_flag) {
        flags |= heartbeat_flag_final;
    }
    if (heartbeat.liveliness_flag) {
        flags |= heartbeat_flag_liveliness;
    }
    const SubmessageHeader header{
        SubmessageKind::heartbeat,
        flags,
        static_cast<std::uint16_t>(content.data().size())};
    const auto encoded_header = encode_submessage_header(header);
    bytes.insert(bytes.end(), encoded_header.begin(), encoded_header.end());
    bytes.insert(bytes.end(), content.data().begin(), content.data().end());
    return true;
}

bool decode_heartbeat_submessage(
    const std::uint8_t* bytes,
    std::size_t size,
    HeartbeatSubmessage& heartbeat,
    std::string& error)
{
    error.clear();
    SubmessageHeader header;
    if (!decode_submessage_header(bytes, size, header)) {
        error = "truncated RTPS submessage header";
        return false;
    }
    if (header.kind != SubmessageKind::heartbeat) {
        error = "RTPS submessage is not HEARTBEAT";
        return false;
    }
    if ((header.flags & ~heartbeat_supported_flags) != 0) {
        error = "unsupported HEARTBEAT flags";
        return false;
    }

    std::size_t encoded_size = 0;
    if (!content_size(header, size, 28, encoded_size, error)) {
        return false;
    }
    const auto endianness = header.little_endian()
        ? serialization::Endianness::little
        : serialization::Endianness::big;
    serialization::CdrReader reader(
        bytes + submessage_header_size,
        encoded_size,
        endianness);
    if (!read_entity_id(reader, heartbeat.reader_id) ||
        !read_entity_id(reader, heartbeat.writer_id) ||
        !read_sequence_number(reader, heartbeat.first_sequence_number) ||
        !read_sequence_number(reader, heartbeat.last_sequence_number) ||
        !reader.read_int32(heartbeat.count)) {
        error = "invalid HEARTBEAT fields";
        return false;
    }
    if (heartbeat.first_sequence_number.value <= 0 ||
        heartbeat.last_sequence_number.value < heartbeat.first_sequence_number.value ||
        heartbeat.count <= 0) {
        error = "invalid HEARTBEAT values";
        return false;
    }
    heartbeat.final_flag = (header.flags & heartbeat_flag_final) != 0;
    heartbeat.liveliness_flag = (header.flags & heartbeat_flag_liveliness) != 0;
    return true;
}

bool encode_acknack_submessage(
    const AckNackSubmessage& acknack,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error)
{
    error.clear();
    bytes.clear();
    if (!validate_sequence_number_set(acknack.reader_state, error)) {
        return false;
    }
    if (acknack.count <= 0) {
        error = "ACKNACK count must be positive";
        return false;
    }

    serialization::CdrWriter content(endianness);
    write_entity_id(content, acknack.reader_id);
    write_entity_id(content, acknack.writer_id);
    write_sequence_number_set(content, acknack.reader_state);
    content.write_int32(acknack.count);

    std::uint8_t flags = endianness == serialization::Endianness::little
        ? endianness_flag
        : 0;
    if (acknack.final_flag) {
        flags |= acknack_flag_final;
    }
    const SubmessageHeader header{
        SubmessageKind::acknack,
        flags,
        static_cast<std::uint16_t>(content.data().size())};
    const auto encoded_header = encode_submessage_header(header);
    bytes.insert(bytes.end(), encoded_header.begin(), encoded_header.end());
    bytes.insert(bytes.end(), content.data().begin(), content.data().end());
    return true;
}

bool decode_acknack_submessage(
    const std::uint8_t* bytes,
    std::size_t size,
    AckNackSubmessage& acknack,
    std::string& error)
{
    error.clear();
    SubmessageHeader header;
    if (!decode_submessage_header(bytes, size, header)) {
        error = "truncated RTPS submessage header";
        return false;
    }
    if (header.kind != SubmessageKind::acknack) {
        error = "RTPS submessage is not ACKNACK";
        return false;
    }
    if ((header.flags & ~acknack_supported_flags) != 0) {
        error = "unsupported ACKNACK flags";
        return false;
    }

    std::size_t encoded_size = 0;
    if (!content_size(header, size, 24, encoded_size, error)) {
        return false;
    }
    const auto endianness = header.little_endian()
        ? serialization::Endianness::little
        : serialization::Endianness::big;
    serialization::CdrReader reader(
        bytes + submessage_header_size,
        encoded_size,
        endianness);
    if (!read_entity_id(reader, acknack.reader_id) ||
        !read_entity_id(reader, acknack.writer_id) ||
        !read_sequence_number_set(reader, acknack.reader_state, error) ||
        !reader.read_int32(acknack.count)) {
        if (error.empty()) {
            error = "invalid ACKNACK fields";
        }
        return false;
    }
    if (acknack.count <= 0) {
        error = "ACKNACK count must be positive";
        return false;
    }
    acknack.final_flag = (header.flags & acknack_flag_final) != 0;
    return true;
}

bool encode_gap_submessage(
    const GapSubmessage& gap,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error)
{
    error.clear();
    bytes.clear();
    if (gap.gap_start.value <= 0 ||
        gap.gap_list.bitmap_base.value < gap.gap_start.value) {
        error = "GAP sequence-number range is invalid";
        return false;
    }
    if (!validate_sequence_number_set(gap.gap_list, error)) {
        return false;
    }

    serialization::CdrWriter content(endianness);
    write_entity_id(content, gap.reader_id);
    write_entity_id(content, gap.writer_id);
    write_sequence_number(content, gap.gap_start);
    write_sequence_number_set(content, gap.gap_list);

    const std::uint8_t flags = endianness == serialization::Endianness::little
        ? endianness_flag
        : 0;
    const SubmessageHeader header{
        SubmessageKind::gap,
        flags,
        static_cast<std::uint16_t>(content.data().size())};
    const auto encoded_header = encode_submessage_header(header);
    bytes.insert(bytes.end(), encoded_header.begin(), encoded_header.end());
    bytes.insert(bytes.end(), content.data().begin(), content.data().end());
    return true;
}

bool decode_gap_submessage(
    const std::uint8_t* bytes,
    std::size_t size,
    GapSubmessage& gap,
    std::string& error)
{
    error.clear();
    SubmessageHeader header;
    if (!decode_submessage_header(bytes, size, header)) {
        error = "truncated RTPS submessage header";
        return false;
    }
    if (header.kind != SubmessageKind::gap) {
        error = "RTPS submessage is not GAP";
        return false;
    }
    if ((header.flags & ~gap_supported_flags) != 0) {
        error = "unsupported GAP flags";
        return false;
    }

    std::size_t encoded_size = 0;
    if (!content_size(header, size, 28, encoded_size, error)) {
        return false;
    }
    const auto endianness = header.little_endian()
        ? serialization::Endianness::little
        : serialization::Endianness::big;
    serialization::CdrReader reader(
        bytes + submessage_header_size,
        encoded_size,
        endianness);
    if (!read_entity_id(reader, gap.reader_id) ||
        !read_entity_id(reader, gap.writer_id) ||
        !read_sequence_number(reader, gap.gap_start) ||
        !read_sequence_number_set(reader, gap.gap_list, error)) {
        if (error.empty()) {
            error = "invalid GAP fields";
        }
        return false;
    }
    if (gap.gap_start.value <= 0 ||
        gap.gap_list.bitmap_base.value < gap.gap_start.value) {
        error = "GAP sequence-number range is invalid";
        return false;
    }
    return true;
}

bool encode_heartbeat_message(
    const MessageHeader& header,
    const HeartbeatSubmessage& heartbeat,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error)
{
    std::vector<std::uint8_t> submessage;
    if (!encode_heartbeat_submessage(heartbeat, endianness, submessage, error)) {
        bytes.clear();
        return false;
    }
    return append_submessage(header, std::move(submessage), bytes);
}

bool encode_acknack_message(
    const MessageHeader& header,
    const AckNackSubmessage& acknack,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error)
{
    std::vector<std::uint8_t> submessage;
    if (!encode_acknack_submessage(acknack, endianness, submessage, error)) {
        bytes.clear();
        return false;
    }
    return append_submessage(header, std::move(submessage), bytes);
}

bool encode_gap_message(
    const MessageHeader& header,
    const GapSubmessage& gap,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error)
{
    std::vector<std::uint8_t> submessage;
    if (!encode_gap_submessage(gap, endianness, submessage, error)) {
        bytes.clear();
        return false;
    }
    return append_submessage(header, std::move(submessage), bytes);
}

} // namespace mini_dds::rtps
