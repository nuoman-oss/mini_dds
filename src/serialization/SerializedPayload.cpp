#include "mini_dds/serialization/SerializedPayload.h"

#include <algorithm>
#include <iterator>
#include <utility>

namespace mini_dds::serialization {

std::vector<std::uint8_t> serialize_string_payload(
    const std::string& value,
    Endianness endianness)
{
    CdrWriter writer(endianness);
    writer.write_string(value);

    const auto representation = endianness == Endianness::little
        ? RepresentationIdentifier::cdr_little_endian
        : RepresentationIdentifier::cdr_big_endian;

    std::vector<std::uint8_t> payload{
        static_cast<std::uint8_t>(static_cast<std::uint16_t>(representation) >> 8U),
        static_cast<std::uint8_t>(static_cast<std::uint16_t>(representation)),
        0x00,
        0x00};

    auto encoded_value = writer.take_data();
    payload.insert(
        payload.end(),
        std::make_move_iterator(encoded_value.begin()),
        std::make_move_iterator(encoded_value.end()));
    return payload;
}

bool deserialize_string_payload(
    const std::vector<std::uint8_t>& payload,
    std::string& value,
    std::string& error)
{
    error.clear();

    if (payload.size() < 4) {
        error = "serialized payload is shorter than its encapsulation header";
        return false;
    }

    const auto representation = static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(payload[0]) << 8U) | payload[1]);

    Endianness endianness;
    if (representation ==
        static_cast<std::uint16_t>(RepresentationIdentifier::cdr_little_endian)) {
        endianness = Endianness::little;
    } else if (representation ==
               static_cast<std::uint16_t>(RepresentationIdentifier::cdr_big_endian)) {
        endianness = Endianness::big;
    } else {
        error = "unsupported serialized payload representation";
        return false;
    }

    CdrReader reader(payload.data() + 4, payload.size() - 4, endianness);
    if (!reader.read_string(value)) {
        error = "invalid CDR string payload";
        return false;
    }

    const auto trailing_size = reader.remaining();
    if (trailing_size > 3) {
        error = "serialized payload contains too many trailing bytes";
        return false;
    }

    std::vector<std::uint8_t> padding;
    if (!reader.read_bytes(trailing_size, padding) ||
        !std::all_of(padding.begin(), padding.end(), [](std::uint8_t byte) {
            return byte == 0;
        })) {
        error = "serialized payload contains non-zero trailing bytes";
        return false;
    }

    return true;
}

} // namespace mini_dds::serialization
