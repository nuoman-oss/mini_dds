#pragma once

#include "mini_dds/rtps/Message.h"
#include "mini_dds/serialization/Cdr.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace mini_dds::rtps {

inline constexpr std::uint8_t data_flag_inline_qos = 0x02;
inline constexpr std::uint8_t data_flag_data = 0x04;
inline constexpr std::uint8_t data_flag_key = 0x08;
inline constexpr std::uint8_t data_flag_non_standard_payload = 0x10;
inline constexpr std::uint16_t data_octets_to_inline_qos = 16;
inline constexpr std::size_t data_fixed_content_size = 20;

struct DataSubmessage {
    EntityId reader_id;
    EntityId writer_id;
    SequenceNumber writer_sequence_number;
    std::vector<std::uint8_t> serialized_payload;
};

struct DataMessage {
    MessageHeader header;
    DataSubmessage data;
};

bool encode_data_submessage(
    const DataSubmessage& data,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error);

bool decode_data_submessage(
    const std::uint8_t* bytes,
    std::size_t size,
    DataSubmessage& data,
    std::string& error);

bool encode_data_message(
    const DataMessage& message,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& bytes,
    std::string& error);

bool decode_data_message(
    const std::vector<std::uint8_t>& bytes,
    DataMessage& message,
    std::string& error);

} // namespace mini_dds::rtps
