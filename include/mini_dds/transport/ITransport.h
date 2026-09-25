#pragma once

#include "mini_dds/transport/Endpoint.h"

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace mini_dds::transport {

enum class ReceiveStatus {
    ok,
    timeout,
    error,
};

struct ReceiveResult {
    ReceiveStatus status{ReceiveStatus::error};
    Datagram datagram;
    std::string error;

    [[nodiscard]] bool ok() const noexcept
    {
        return status == ReceiveStatus::ok;
    }
};

class ITransport {
public:
    virtual ~ITransport() = default;

    virtual bool send(
        const std::vector<std::uint8_t>& data,
        const Endpoint& destination) = 0;

    virtual ReceiveResult receive(std::chrono::milliseconds timeout) = 0;

    [[nodiscard]] virtual bool is_open() const noexcept = 0;
    [[nodiscard]] virtual Endpoint local_endpoint() const = 0;
    [[nodiscard]] virtual std::string last_error() const = 0;
};

} // namespace mini_dds::transport
