#pragma once

#include <array>
#include <cstdint>

namespace mini_dds::rtps {

struct ProtocolVersion {
    std::uint8_t major{2};
    std::uint8_t minor{3};
};

inline bool operator==(const ProtocolVersion& lhs, const ProtocolVersion& rhs)
{
    return lhs.major == rhs.major && lhs.minor == rhs.minor;
}

struct VendorId {
    std::array<std::uint8_t, 2> value{{0x00, 0x00}};
};

inline bool operator==(const VendorId& lhs, const VendorId& rhs)
{
    return lhs.value == rhs.value;
}

struct GuidPrefix {
    std::array<std::uint8_t, 12> value{};
};

inline bool operator==(const GuidPrefix& lhs, const GuidPrefix& rhs)
{
    return lhs.value == rhs.value;
}

struct EntityId {
    std::array<std::uint8_t, 4> value{};
};

inline bool operator==(const EntityId& lhs, const EntityId& rhs)
{
    return lhs.value == rhs.value;
}

struct Guid {
    GuidPrefix prefix;
    EntityId entity_id;
};

inline bool operator==(const Guid& lhs, const Guid& rhs)
{
    return lhs.prefix == rhs.prefix && lhs.entity_id == rhs.entity_id;
}

struct SequenceNumber {
    std::int64_t value{0};
};

inline bool operator==(const SequenceNumber& lhs, const SequenceNumber& rhs)
{
    return lhs.value == rhs.value;
}

} // namespace mini_dds::rtps
