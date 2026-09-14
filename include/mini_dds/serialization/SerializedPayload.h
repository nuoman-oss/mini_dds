#pragma once

#include "mini_dds/serialization/Cdr.h"

#include <cstdint>
#include <string>
#include <vector>

namespace mini_dds::serialization {

enum class RepresentationIdentifier : std::uint16_t {
    cdr_big_endian = 0x0000,
    cdr_little_endian = 0x0001,
};

std::vector<std::uint8_t> serialize_string_payload(
    const std::string& value,
    Endianness endianness = Endianness::little);

bool deserialize_string_payload(
    const std::vector<std::uint8_t>& payload,
    std::string& value,
    std::string& error);

} // namespace mini_dds::serialization
