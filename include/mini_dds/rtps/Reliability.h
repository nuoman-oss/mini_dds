#pragma once

#include "mini_dds/rtps/Message.h"
#include "mini_dds/serialization/Cdr.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace mini_dds::rtps {

inline constexpr std::size_t sequence_number_set_max_bits = 256;
inline constexpr std::uint8_t acknack_flag_final = 0x02;
inline constexpr std::uint8_t heartbeat_flag_final = 0x02;
inline constexpr std::uint8_t heartbeat_flag_liveliness = 0x04;

struct SequenceNumberSet {
    SequenceNumber bitmap_base{1};
    std::uint32_t num_bits{0};
    std::vector<std::uint32_t> bitmap;

    [[nodiscard]] bool contains(SequenceNumber sequence_number) const noexcept;
};

struct HeartbeatSubmessage {
    EntityId reader_id;
    EntityId writer_id;
    SequenceNumber first_sequence_number{1};
    SequenceNumber last_sequence_number{1};
    std::int32_t count{1};
    bool final_flag{false};
    bool liveliness_flag{false};
};

struct AckNackSubmessage {
    EntityId reader_id;
    EntityId writer_id;
    SequenceNumberSet reader_state;
    std::int32_t count{1};
    bool final_flag{false};
};

struct GapSubmessage {
    EntityId reader_id;
    EntityId writer_id;
    SequenceNumber gap_start{1};
    SequenceNumberSet gap_list;
};

bool encode_heartbeat_submessage(
    const HeartbeatSubmessage& heartbeat,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error);

bool decode_heartbeat_submessage(
    const std::uint8_t* bytes,
    std::size_t size,
    HeartbeatSubmessage& heartbeat,
    std::string& error);

bool encode_acknack_submessage(
    const AckNackSubmessage& acknack,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error);

bool decode_acknack_submessage(
    const std::uint8_t* bytes,
    std::size_t size,
    AckNackSubmessage& acknack,
    std::string& error);

bool encode_gap_submessage(
    const GapSubmessage& gap,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error);

bool decode_gap_submessage(
    const std::uint8_t* bytes,
    std::size_t size,
    GapSubmessage& gap,
    std::string& error);

bool encode_heartbeat_message(
    const MessageHeader& header,
    const HeartbeatSubmessage& heartbeat,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error);

bool encode_acknack_message(
    const MessageHeader& header,
    const AckNackSubmessage& acknack,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error);

bool encode_gap_message(
    const MessageHeader& header,
    const GapSubmessage& gap,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error);

} // namespace mini_dds::rtps
