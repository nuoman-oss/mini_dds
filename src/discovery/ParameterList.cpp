#include "mini_dds/discovery/ParameterList.h"

#include <algorithm>
#include <limits>

namespace mini_dds::discovery {
namespace {

inline constexpr std::uint16_t representation_pl_cdr_big_endian = 0x0002;
inline constexpr std::uint16_t representation_pl_cdr_little_endian = 0x0003;

void append_uint16(
    std::vector<std::uint8_t>& bytes,
    std::uint16_t value,
    serialization::Endianness endianness)
{
    if (endianness == serialization::Endianness::little) {
        bytes.push_back(static_cast<std::uint8_t>(value));
        bytes.push_back(static_cast<std::uint8_t>(value >> 8U));
    } else {
        bytes.push_back(static_cast<std::uint8_t>(value >> 8U));
        bytes.push_back(static_cast<std::uint8_t>(value));
    }
}

std::uint16_t read_uint16(
    const std::uint8_t* bytes,
    serialization::Endianness endianness)
{
    if (endianness == serialization::Endianness::little) {
        return static_cast<std::uint16_t>(
            bytes[0] | (static_cast<std::uint16_t>(bytes[1]) << 8U));
    }
    return static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(bytes[0]) << 8U) | bytes[1]);
}

std::size_t aligned_size(std::size_t size)
{
    return (size + 3U) & ~std::size_t{3U};
}

} // namespace

bool encode_parameter_list(
    const ParameterList& parameters,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& payload,
    std::string& error)
{
    payload.clear();
    error.clear();

    const auto representation = endianness == serialization::Endianness::little
        ? representation_pl_cdr_little_endian
        : representation_pl_cdr_big_endian;
    payload.push_back(static_cast<std::uint8_t>(representation >> 8U));
    payload.push_back(static_cast<std::uint8_t>(representation));
    payload.push_back(0x00);
    payload.push_back(0x00);

    for (const auto& parameter : parameters) {
        if (parameter.id == parameter_id_sentinel) {
            error = "PID_SENTINEL is added automatically";
            payload.clear();
            return false;
        }

        const std::size_t wire_size = aligned_size(parameter.value.size());
        if (wire_size > std::numeric_limits<std::uint16_t>::max()) {
            error = "parameter value is too large";
            payload.clear();
            return false;
        }

        append_uint16(payload, parameter.id, endianness);
        append_uint16(payload, static_cast<std::uint16_t>(wire_size), endianness);
        payload.insert(payload.end(), parameter.value.begin(), parameter.value.end());
        payload.insert(payload.end(), wire_size - parameter.value.size(), 0x00);
    }

    append_uint16(payload, parameter_id_sentinel, endianness);
    append_uint16(payload, 0, endianness);
    return true;
}

bool decode_parameter_list(
    const std::vector<std::uint8_t>& payload,
    ParameterList& parameters,
    serialization::Endianness& endianness,
    std::string& error)
{
    parameters.clear();
    error.clear();

    if (payload.size() < 8) {
        error = "ParameterList is shorter than its encapsulation and sentinel";
        return false;
    }

    const auto representation = static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(payload[0]) << 8U) | payload[1]);
    if (representation == representation_pl_cdr_little_endian) {
        endianness = serialization::Endianness::little;
    } else if (representation == representation_pl_cdr_big_endian) {
        endianness = serialization::Endianness::big;
    } else {
        error = "unsupported ParameterList representation";
        return false;
    }

    std::size_t offset = 4;
    while (offset + 4U <= payload.size()) {
        const std::uint16_t id = read_uint16(payload.data() + offset, endianness);
        const std::uint16_t length =
            read_uint16(payload.data() + offset + 2U, endianness);
        offset += 4U;

        if (id == parameter_id_sentinel) {
            if (length != 0) {
                error = "PID_SENTINEL has a non-zero length";
                parameters.clear();
                return false;
            }
            return true;
        }

        if ((length % 4U) != 0U || length > payload.size() - offset) {
            error = "invalid or truncated ParameterList value";
            parameters.clear();
            return false;
        }

        if (id != parameter_id_pad) {
            parameters.push_back(Parameter{
                id,
                std::vector<std::uint8_t>(
                    payload.begin() + static_cast<std::ptrdiff_t>(offset),
                    payload.begin() + static_cast<std::ptrdiff_t>(offset + length))});
        }
        offset += length;
    }

    error = "ParameterList does not contain PID_SENTINEL";
    parameters.clear();
    return false;
}

const Parameter* find_parameter(
    const ParameterList& parameters,
    std::uint16_t id)
{
    const auto match = std::find_if(
        parameters.begin(),
        parameters.end(),
        [id](const Parameter& parameter) {
            return parameter.id == id;
        });
    return match == parameters.end() ? nullptr : &*match;
}

} // namespace mini_dds::discovery
