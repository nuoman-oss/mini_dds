#pragma once

#include "mini_dds/transport/ITransport.h"

#include <memory>
#include <string>

namespace mini_dds::transport {

class UdpTransport final : public ITransport {
public:
    explicit UdpTransport(
        std::uint16_t local_port,
        std::string bind_address = "0.0.0.0");

    ~UdpTransport() override;

    UdpTransport(const UdpTransport&) = delete;
    UdpTransport& operator=(const UdpTransport&) = delete;
    UdpTransport(UdpTransport&&) = delete;
    UdpTransport& operator=(UdpTransport&&) = delete;

    bool send(
        const std::vector<std::uint8_t>& data,
        const Endpoint& destination) override;

    bool send(
        const std::vector<std::uint8_t>& data,
        const std::string& address,
        std::uint16_t port);

    bool join_multicast_group(
        const std::string& group_address,
        const std::string& interface_address = "0.0.0.0");

    bool set_multicast_interface(const std::string& interface_address);
    bool set_multicast_loopback(bool enabled);

    // Blocking compatibility overload for the original prototype API.
    std::vector<std::uint8_t> receive();

    ReceiveResult receive(std::chrono::milliseconds timeout) override;

    [[nodiscard]] bool is_open() const noexcept override;
    [[nodiscard]] Endpoint local_endpoint() const override;
    [[nodiscard]] std::string last_error() const override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace mini_dds::transport
