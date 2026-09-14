#pragma once

#include "mini_dds/serialization/Cdr.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace mini_dds::discovery {

inline constexpr std::uint16_t parameter_id_pad = 0x0000;
inline constexpr std::uint16_t parameter_id_sentinel = 0x0001;

struct Parameter {
    std::uint16_t id{parameter_id_pad};
    std::vector<std::uint8_t> value;
};

using ParameterList = std::vector<Parameter>;

bool encode_parameter_list(
    const ParameterList& parameters,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& payload,
    std::string& error);

bool decode_parameter_list(
    const std::vector<std::uint8_t>& payload,
    ParameterList& parameters,
    serialization::Endianness& endianness,
    std::string& error);

const Parameter* find_parameter(
    const ParameterList& parameters,
    std::uint16_t id);

} // namespace mini_dds::discovery
