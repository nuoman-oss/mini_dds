#include "mini_dds/discovery/PortMapping.h"

#include <limits>

namespace mini_dds::discovery {

PortMapping::PortMapping(PortMappingConfig config)
    : config_(config)
{
}

std::optional<std::uint16_t> PortMapping::calculate(
    std::uint32_t domain_id,
    std::uint32_t offset,
    std::uint32_t participant_id,
    bool include_participant) const
{
    const std::uint64_t value =
        static_cast<std::uint64_t>(config_.port_base) +
        static_cast<std::uint64_t>(config_.domain_gain) * domain_id +
        offset +
        (include_participant
             ? static_cast<std::uint64_t>(config_.participant_gain) * participant_id
             : 0U);

    if (value > std::numeric_limits<std::uint16_t>::max()) {
        return std::nullopt;
    }
    return static_cast<std::uint16_t>(value);
}

std::optional<std::uint16_t> PortMapping::spdp_multicast_port(
    std::uint32_t domain_id) const
{
    return calculate(domain_id, config_.spdp_multicast_offset, 0, false);
}

std::optional<std::uint16_t> PortMapping::spdp_unicast_port(
    std::uint32_t domain_id,
    std::uint32_t participant_id) const
{
    return calculate(
        domain_id,
        config_.spdp_unicast_offset,
        participant_id,
        true);
}

std::optional<std::uint16_t> PortMapping::user_multicast_port(
    std::uint32_t domain_id) const
{
    return calculate(domain_id, config_.user_multicast_offset, 0, false);
}

std::optional<std::uint16_t> PortMapping::user_unicast_port(
    std::uint32_t domain_id,
    std::uint32_t participant_id) const
{
    return calculate(
        domain_id,
        config_.user_unicast_offset,
        participant_id,
        true);
}

} // namespace mini_dds::discovery
