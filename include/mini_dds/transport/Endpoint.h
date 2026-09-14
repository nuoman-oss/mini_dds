#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace mini_dds::transport {

struct Endpoint {
    std::string address{"0.0.0.0"};
    std::uint16_t port{0};
};

inline bool operator==(const Endpoint& lhs, const Endpoint& rhs)
{
    return lhs.address == rhs.address && lhs.port == rhs.port;
}

inline bool operator!=(const Endpoint& lhs, const Endpoint& rhs)
{
    return !(lhs == rhs);
}

struct Datagram {
    std::vector<std::uint8_t> payload;
    Endpoint source;
};

} // namespace mini_dds::transport
