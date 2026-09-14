#pragma once

#include "mini_dds/discovery/DiscoveryData.h"
#include "mini_dds/discovery/PortMapping.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace mini_dds::discovery {

struct DiscoveryConfig {
    std::uint32_t domain_id{0};
    std::uint32_t participant_id{0};
    rtps::GuidPrefix guid_prefix;
    transport::Endpoint user_unicast_locator;
    std::string bind_address{"0.0.0.0"};
    std::string advertised_address{"127.0.0.1"};
    std::string multicast_group{"239.255.0.1"};
    std::string participant_name{"mini_dds"};
    std::chrono::milliseconds announcement_period{1000};
    std::chrono::milliseconds lease_duration{10000};
    PortMappingConfig port_mapping;
};

class DiscoveryService {
public:
    explicit DiscoveryService(DiscoveryConfig config);
    ~DiscoveryService();

    DiscoveryService(const DiscoveryService&) = delete;
    DiscoveryService& operator=(const DiscoveryService&) = delete;
    DiscoveryService(DiscoveryService&&) = delete;
    DiscoveryService& operator=(DiscoveryService&&) = delete;

    [[nodiscard]] bool is_valid() const noexcept;
    [[nodiscard]] std::string last_error() const;

    void announce_local_endpoint(EndpointDiscoveryData endpoint);
    void remove_local_endpoint(const rtps::Guid& endpoint_guid);

    std::optional<EndpointDiscoveryData> wait_for_reader(
        const std::string& topic_name,
        const std::string& type_name,
        bool writer_is_reliable,
        std::chrono::milliseconds timeout);

    bool wait_for_participant(
        const rtps::GuidPrefix& participant_prefix,
        std::chrono::milliseconds timeout);

    [[nodiscard]] std::size_t remote_participant_count() const;
    [[nodiscard]] std::size_t remote_endpoint_count() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace mini_dds::discovery
