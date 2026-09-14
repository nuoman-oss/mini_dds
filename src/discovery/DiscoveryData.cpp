#include "mini_dds/discovery/DiscoveryData.h"

#include "mini_dds/discovery/ParameterList.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

namespace mini_dds::discovery {
namespace {

constexpr std::int32_t locator_kind_udp_v4 = 1;
constexpr std::int32_t reliability_best_effort = 1;
constexpr std::int32_t reliability_reliable = 2;

std::vector<std::uint8_t> encode_guid(const rtps::Guid& guid)
{
    std::vector<std::uint8_t> value;
    value.reserve(16);
    value.insert(value.end(), guid.prefix.value.begin(), guid.prefix.value.end());
    value.insert(value.end(), guid.entity_id.value.begin(), guid.entity_id.value.end());
    return value;
}

bool decode_guid(const Parameter& parameter, rtps::Guid& guid)
{
    if (parameter.value.size() < 16) {
        return false;
    }
    std::copy_n(parameter.value.begin(), 12, guid.prefix.value.begin());
    std::copy_n(parameter.value.begin() + 12, 4, guid.entity_id.value.begin());
    return true;
}

std::vector<std::uint8_t> encode_uint32(
    std::uint32_t value,
    serialization::Endianness endianness)
{
    serialization::CdrWriter writer(endianness);
    writer.write_uint32(value);
    return writer.take_data();
}

bool decode_uint32(
    const Parameter& parameter,
    serialization::Endianness endianness,
    std::uint32_t& value)
{
    serialization::CdrReader reader(parameter.value, endianness);
    return reader.read_uint32(value);
}

std::vector<std::uint8_t> encode_string(
    const std::string& value,
    serialization::Endianness endianness)
{
    serialization::CdrWriter writer(endianness);
    writer.write_string(value);
    return writer.take_data();
}

bool only_zero_padding(
    serialization::CdrReader& reader)
{
    if (reader.remaining() > 3) {
        return false;
    }
    std::vector<std::uint8_t> padding;
    return reader.read_bytes(reader.remaining(), padding) &&
           std::all_of(padding.begin(), padding.end(), [](std::uint8_t byte) {
               return byte == 0;
           });
}

bool decode_string(
    const Parameter& parameter,
    serialization::Endianness endianness,
    std::string& value)
{
    serialization::CdrReader reader(parameter.value, endianness);
    return reader.read_string(value) && only_zero_padding(reader);
}

bool parse_ipv4(
    const std::string& address,
    std::array<std::uint8_t, 4>& octets)
{
    std::istringstream stream(address);
    std::array<unsigned, 4> values{};
    char separator1 = 0;
    char separator2 = 0;
    char separator3 = 0;
    if (!(stream >> values[0] >> separator1 >> values[1] >> separator2 >>
          values[2] >> separator3 >> values[3]) ||
        separator1 != '.' || separator2 != '.' || separator3 != '.' ||
        std::any_of(values.begin(), values.end(), [](unsigned value) {
            return value > 255U;
        })) {
        return false;
    }

    stream >> std::ws;
    if (!stream.eof()) {
        return false;
    }

    std::transform(
        values.begin(),
        values.end(),
        octets.begin(),
        [](unsigned value) {
            return static_cast<std::uint8_t>(value);
        });
    return true;
}

bool encode_locator(
    const transport::Endpoint& endpoint,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& value,
    std::string& error)
{
    std::array<std::uint8_t, 4> ipv4{};
    if (endpoint.port == 0 || !parse_ipv4(endpoint.address, ipv4)) {
        error = "invalid UDPv4 discovery locator";
        return false;
    }

    serialization::CdrWriter writer(endianness);
    writer.write_int32(locator_kind_udp_v4);
    writer.write_uint32(endpoint.port);
    writer.write_bytes(std::vector<std::uint8_t>(12, 0));
    writer.write_bytes(std::vector<std::uint8_t>(ipv4.begin(), ipv4.end()));
    value = writer.take_data();
    return true;
}

bool decode_locator(
    const Parameter& parameter,
    serialization::Endianness endianness,
    transport::Endpoint& endpoint)
{
    serialization::CdrReader reader(parameter.value, endianness);
    std::int32_t kind = 0;
    std::uint32_t port = 0;
    std::vector<std::uint8_t> address;
    if (!reader.read_int32(kind) ||
        !reader.read_uint32(port) ||
        !reader.read_bytes(16, address) ||
        kind != locator_kind_udp_v4 ||
        port == 0 ||
        port > std::numeric_limits<std::uint16_t>::max() ||
        !std::all_of(address.begin(), address.begin() + 12, [](std::uint8_t byte) {
            return byte == 0;
        })) {
        return false;
    }

    endpoint.address =
        std::to_string(address[12]) + '.' +
        std::to_string(address[13]) + '.' +
        std::to_string(address[14]) + '.' +
        std::to_string(address[15]);
    endpoint.port = static_cast<std::uint16_t>(port);
    return true;
}

bool encode_duration(
    std::chrono::milliseconds duration,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& value,
    std::string& error)
{
    if (duration.count() <= 0) {
        error = "discovery lease duration must be positive";
        return false;
    }

    const auto seconds = duration.count() / 1000;
    const auto remainder = duration.count() % 1000;
    if (seconds > std::numeric_limits<std::int32_t>::max()) {
        error = "discovery lease duration is too large";
        return false;
    }

    const auto fraction = static_cast<std::uint32_t>(
        (static_cast<std::uint64_t>(remainder) << 32U) / 1000U);
    serialization::CdrWriter writer(endianness);
    writer.write_int32(static_cast<std::int32_t>(seconds));
    writer.write_uint32(fraction);
    value = writer.take_data();
    return true;
}

bool decode_duration(
    const Parameter& parameter,
    serialization::Endianness endianness,
    std::chrono::milliseconds& duration)
{
    serialization::CdrReader reader(parameter.value, endianness);
    std::int32_t seconds = 0;
    std::uint32_t fraction = 0;
    if (!reader.read_int32(seconds) ||
        !reader.read_uint32(fraction) ||
        seconds < 0) {
        return false;
    }

    const auto fractional_milliseconds = static_cast<std::int64_t>(
        (static_cast<std::uint64_t>(fraction) * 1000U) >> 32U);
    duration = std::chrono::seconds(seconds) +
        std::chrono::milliseconds(fractional_milliseconds);
    return duration.count() > 0;
}

std::vector<std::uint8_t> encode_reliability(
    bool reliable,
    serialization::Endianness endianness)
{
    serialization::CdrWriter writer(endianness);
    writer.write_int32(reliable ? reliability_reliable : reliability_best_effort);
    writer.write_int32(0);
    writer.write_uint32(0);
    return writer.take_data();
}

bool decode_reliability(
    const Parameter& parameter,
    serialization::Endianness endianness,
    bool& reliable)
{
    serialization::CdrReader reader(parameter.value, endianness);
    std::int32_t kind = 0;
    if (!reader.read_int32(kind) ||
        (kind != reliability_best_effort && kind != reliability_reliable)) {
        return false;
    }
    reliable = kind == reliability_reliable;
    return true;
}

const Parameter* require_parameter(
    const ParameterList& parameters,
    std::uint16_t id,
    const char* name,
    std::string& error)
{
    const Parameter* parameter = find_parameter(parameters, id);
    if (parameter == nullptr) {
        error = std::string("missing discovery parameter: ") + name;
    }
    return parameter;
}

} // namespace

bool encode_participant_discovery_data(
    const ParticipantDiscoveryData& data,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& payload,
    std::string& error)
{
    error.clear();

    std::vector<std::uint8_t> metatraffic_locator;
    std::vector<std::uint8_t> default_locator;
    std::vector<std::uint8_t> lease_duration;
    if (!encode_locator(
            data.metatraffic_unicast_locator,
            endianness,
            metatraffic_locator,
            error) ||
        !encode_locator(
            data.default_unicast_locator,
            endianness,
            default_locator,
            error) ||
        !encode_duration(
            data.lease_duration,
            endianness,
            lease_duration,
            error)) {
        return false;
    }

    ParameterList parameters{
        {pid_protocol_version,
         {data.protocol_version.major, data.protocol_version.minor}},
        {pid_vendor_id,
         {data.vendor_id.value[0], data.vendor_id.value[1]}},
        {pid_participant_guid, encode_guid(data.participant_guid)},
        {pid_builtin_endpoint_set,
         encode_uint32(data.available_builtin_endpoints, endianness)},
        {pid_metatraffic_unicast_locator, std::move(metatraffic_locator)},
        {pid_default_unicast_locator, std::move(default_locator)},
        {pid_participant_lease_duration, std::move(lease_duration)}};

    if (!data.name.empty()) {
        parameters.push_back({pid_entity_name, encode_string(data.name, endianness)});
    }

    return encode_parameter_list(parameters, endianness, payload, error);
}

bool decode_participant_discovery_data(
    const std::vector<std::uint8_t>& payload,
    ParticipantDiscoveryData& data,
    std::string& error)
{
    ParameterList parameters;
    serialization::Endianness endianness;
    if (!decode_parameter_list(payload, parameters, endianness, error)) {
        return false;
    }

    const auto* guid = require_parameter(
        parameters, pid_participant_guid, "PID_PARTICIPANT_GUID", error);
    const auto* metatraffic = require_parameter(
        parameters,
        pid_metatraffic_unicast_locator,
        "PID_METATRAFFIC_UNICAST_LOCATOR",
        error);
    const auto* default_locator = require_parameter(
        parameters,
        pid_default_unicast_locator,
        "PID_DEFAULT_UNICAST_LOCATOR",
        error);
    if (guid == nullptr || metatraffic == nullptr || default_locator == nullptr) {
        return false;
    }

    if (!decode_guid(*guid, data.participant_guid) ||
        !decode_locator(*metatraffic, endianness, data.metatraffic_unicast_locator) ||
        !decode_locator(*default_locator, endianness, data.default_unicast_locator)) {
        error = "invalid participant GUID or locator";
        return false;
    }

    if (const auto* version = find_parameter(parameters, pid_protocol_version)) {
        if (version->value.size() < 2) {
            error = "invalid PID_PROTOCOL_VERSION";
            return false;
        }
        data.protocol_version = {version->value[0], version->value[1]};
    }

    if (const auto* vendor = find_parameter(parameters, pid_vendor_id)) {
        if (vendor->value.size() < 2) {
            error = "invalid PID_VENDORID";
            return false;
        }
        data.vendor_id = {{vendor->value[0], vendor->value[1]}};
    }

    if (const auto* endpoints = find_parameter(parameters, pid_builtin_endpoint_set)) {
        if (!decode_uint32(
                *endpoints,
                endianness,
                data.available_builtin_endpoints)) {
            error = "invalid PID_BUILTIN_ENDPOINT_SET";
            return false;
        }
    }

    if (const auto* lease = find_parameter(parameters, pid_participant_lease_duration)) {
        if (!decode_duration(*lease, endianness, data.lease_duration)) {
            error = "invalid PID_PARTICIPANT_LEASE_DURATION";
            return false;
        }
    }

    if (const auto* name = find_parameter(parameters, pid_entity_name)) {
        if (!decode_string(*name, endianness, data.name)) {
            error = "invalid PID_ENTITY_NAME";
            return false;
        }
    }

    return true;
}

bool encode_endpoint_discovery_data(
    const EndpointDiscoveryData& data,
    serialization::Endianness endianness,
    std::vector<std::uint8_t>& payload,
    std::string& error)
{
    error.clear();
    if (data.topic_name.empty() || data.type_name.empty()) {
        error = "endpoint discovery requires topic and type names";
        return false;
    }

    std::vector<std::uint8_t> locator;
    if (!encode_locator(data.unicast_locator, endianness, locator, error)) {
        return false;
    }

    const ParameterList parameters{
        {pid_endpoint_guid, encode_guid(data.endpoint_guid)},
        {pid_participant_guid, encode_guid(data.participant_guid)},
        {pid_topic_name, encode_string(data.topic_name, endianness)},
        {pid_type_name, encode_string(data.type_name, endianness)},
        {pid_unicast_locator, std::move(locator)},
        {pid_reliability, encode_reliability(data.reliable, endianness)}};
    return encode_parameter_list(parameters, endianness, payload, error);
}

bool decode_endpoint_discovery_data(
    const std::vector<std::uint8_t>& payload,
    DiscoveredEndpointKind kind,
    EndpointDiscoveryData& data,
    std::string& error)
{
    ParameterList parameters;
    serialization::Endianness endianness;
    if (!decode_parameter_list(payload, parameters, endianness, error)) {
        return false;
    }

    const auto* endpoint_guid = require_parameter(
        parameters, pid_endpoint_guid, "PID_ENDPOINT_GUID", error);
    const auto* topic = require_parameter(
        parameters, pid_topic_name, "PID_TOPIC_NAME", error);
    const auto* type = require_parameter(
        parameters, pid_type_name, "PID_TYPE_NAME", error);
    const auto* locator = require_parameter(
        parameters, pid_unicast_locator, "PID_UNICAST_LOCATOR", error);
    if (endpoint_guid == nullptr || topic == nullptr ||
        type == nullptr || locator == nullptr) {
        return false;
    }

    data.kind = kind;
    if (!decode_guid(*endpoint_guid, data.endpoint_guid) ||
        !decode_string(*topic, endianness, data.topic_name) ||
        !decode_string(*type, endianness, data.type_name) ||
        !decode_locator(*locator, endianness, data.unicast_locator)) {
        error = "invalid endpoint discovery GUID, name, type, or locator";
        return false;
    }

    data.participant_guid = rtps::Guid{
        data.endpoint_guid.prefix,
        entity_id_participant};
    if (const auto* participant = find_parameter(parameters, pid_participant_guid)) {
        if (!decode_guid(*participant, data.participant_guid)) {
            error = "invalid PID_PARTICIPANT_GUID";
            return false;
        }
    }

    data.reliable = false;
    if (const auto* reliability = find_parameter(parameters, pid_reliability)) {
        if (!decode_reliability(*reliability, endianness, data.reliable)) {
            error = "invalid PID_RELIABILITY";
            return false;
        }
    }
    return true;
}

} // namespace mini_dds::discovery
