#pragma once

#include <cstdint>
#include <optional>

namespace mini_dds::discovery {

struct PortMappingConfig {
    std::uint32_t port_base{7400};
    std::uint32_t domain_gain{250};
    std::uint32_t participant_gain{2};
    std::uint32_t spdp_multicast_offset{0};
    std::uint32_t spdp_unicast_offset{10};
    std::uint32_t user_multicast_offset{1};
    std::uint32_t user_unicast_offset{11};
};

class PortMapping {
public:
    explicit PortMapping(PortMappingConfig config = {});

    [[nodiscard]] std::optional<std::uint16_t> spdp_multicast_port(
        std::uint32_t domain_id) const;

    [[nodiscard]] std::optional<std::uint16_t> spdp_unicast_port(
        std::uint32_t domain_id,
        std::uint32_t participant_id) const;

    [[nodiscard]] std::optional<std::uint16_t> user_multicast_port(
        std::uint32_t domain_id) const;

    [[nodiscard]] std::optional<std::uint16_t> user_unicast_port(
        std::uint32_t domain_id,
        std::uint32_t participant_id) const;

private:
    std::optional<std::uint16_t> calculate(
        std::uint32_t domain_id,
        std::uint32_t offset,
        std::uint32_t participant_id,
        bool include_participant) const;

    PortMappingConfig config_;
};

} // namespace mini_dds::discovery
