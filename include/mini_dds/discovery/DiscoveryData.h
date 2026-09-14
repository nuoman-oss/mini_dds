#pragma once

#include "mini_dds/rtps/Types.h"
#include "mini_dds/serialization/Cdr.h"
#include "mini_dds/transport/Endpoint.h"

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace mini_dds::discovery {

inline constexpr std::uint16_t pid_topic_name = 0x0005;
inline constexpr std::uint16_t pid_type_name = 0x0007;
inline constexpr std::uint16_t pid_reliability = 0x001a;
inline constexpr std::uint16_t pid_protocol_version = 0x0015;
inline constexpr std::uint16_t pid_vendor_id = 0x0016;
inline constexpr std::uint16_t pid_unicast_locator = 0x002f;
inline constexpr std::uint16_t pid_default_unicast_locator = 0x0031;
inline constexpr std::uint16_t pid_metatraffic_unicast_locator = 0x0032;
inline constexpr std::uint16_t pid_participant_lease_duration = 0x0002;
inline constexpr std::uint16_t pid_participant_guid = 0x0050;
inline constexpr std::uint16_t pid_builtin_endpoint_set = 0x0058;
inline constexpr std::uint16_t pid_endpoint_guid = 0x005a;
inline constexpr std::uint16_t pid_entity_name = 0x0062;

inline constexpr rtps::EntityId entity_id_participant{{0x00, 0x00, 0x01, 0xc1}};
inline constexpr rtps::EntityId entity_id_spdp_announcer{{0x00, 0x01, 0x00, 0xc2}};
inline constexpr rtps::EntityId entity_id_spdp_detector{{0x00, 0x01, 0x00, 0xc7}};
inline constexpr rtps::EntityId entity_id_sedp_publications_announcer{{0x00, 0x00, 0x03, 0xc2}};
inline constexpr rtps::EntityId entity_id_sedp_publications_detector{{0x00, 0x00, 0x03, 0xc7}};
inline constexpr rtps::EntityId entity_id_sedp_subscriptions_announcer{{0x00, 0x00, 0x04, 0xc2}};
inline constexpr rtps::EntityId entity_id_sedp_subscriptions_detector{{0x00, 0x00, 0x04, 0xc7}};

inline constexpr std::uint32_t builtin_endpoint_set_minimal = 0x0000003fU;

struct ParticipantDiscoveryData {
    rtps::Guid participant_guid;
    rtps::ProtocolVersion protocol_version{2, 3};
    rtps::VendorId vendor_id{};
    transport::Endpoint metatraffic_unicast_locator;
    transport::Endpoint default_unicast_locator;
    std::uint32_t available_builtin_endpoints{builtin_endpoint_set_minimal};
    std::chrono::milliseconds lease_duration{100000};
    std::string name;
};

enum class DiscoveredEndpointKind {
    writer,
    reader,
};

struct EndpointDiscoveryData {
    DiscoveredEndpointKind kind{DiscoveredEndpointKind::writer};
    rtps::Guid endpoint_guid;
    rtps::Guid participant_guid;
    std::string topic_name;
    std::string type_name;
    transport::Endpoint unicast_locator;
    bool reliable{false};
};

bool encode_participant_discovery_data(
    const ParticipantDiscoveryData& data,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& payload,
    std::string& error);

bool decode_participant_discovery_data(
    const std::vector<std::uint8_t>& payload,
    ParticipantDiscoveryData& data,
    std::string& error);

bool encode_endpoint_discovery_data(
    const EndpointDiscoveryData& data,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& payload,
    std::string& error);

bool decode_endpoint_discovery_data(
    const std::vector<std::uint8_t>& payload,
    DiscoveredEndpointKind kind,
    EndpointDiscoveryData& data,
    std::string& error);

} // namespace mini_dds::discovery
