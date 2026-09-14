#include "mini_dds/discovery/DiscoveryService.h"

#include "mini_dds/rtps/Data.h"
#include "mini_dds/transport/UDPTransport.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mini_dds::discovery {
namespace {

using Clock = std::chrono::steady_clock;

struct RemoteParticipant {
    ParticipantDiscoveryData data;
    Clock::time_point last_seen;
};

struct RemoteEndpoint {
    EndpointDiscoveryData data;
    Clock::time_point last_seen;
};

struct LocalEndpoint {
    EndpointDiscoveryData data;
    rtps::SequenceNumber sequence_number;
};

bool same_guid(const rtps::Guid& lhs, const rtps::Guid& rhs)
{
    return lhs == rhs;
}

bool same_prefix(const rtps::GuidPrefix& lhs, const rtps::GuidPrefix& rhs)
{
    return lhs == rhs;
}

} // namespace

class DiscoveryService::Impl {
public:
    explicit Impl(DiscoveryConfig input_config)
        : config(std::move(input_config))
    {
        initialize();
    }

    ~Impl()
    {
        stop.store(true);
        condition.notify_all();
        if (worker.joinable()) {
            worker.join();
        }
    }

    void initialize()
    {
        if (config.announcement_period.count() <= 0 ||
            config.lease_duration <= config.announcement_period) {
            set_error("discovery lease must be longer than the announcement period");
            return;
        }

        if (config.user_unicast_locator.port == 0 ||
            config.advertised_address.empty()) {
            set_error("discovery requires an advertised user unicast locator");
            return;
        }

        const PortMapping ports(config.port_mapping);
        const auto multicast_port = ports.spdp_multicast_port(config.domain_id);
        const auto metatraffic_port =
            ports.spdp_unicast_port(config.domain_id, config.participant_id);
        if (!multicast_port || !metatraffic_port) {
            set_error("RTPS discovery port calculation overflowed");
            return;
        }

        metatraffic_transport = std::make_unique<transport::UdpTransport>(
            *metatraffic_port,
            config.bind_address);
        if (!metatraffic_transport->is_open()) {
            set_error("metatraffic transport: " + metatraffic_transport->last_error());
            return;
        }

        spdp_transport = std::make_unique<transport::UdpTransport>(
            *multicast_port,
            "0.0.0.0");
        if (!spdp_transport->is_open()) {
            set_error("SPDP multicast transport: " + spdp_transport->last_error());
            return;
        }

        if (!spdp_transport->join_multicast_group(
                config.multicast_group,
                config.advertised_address)) {
            set_error("SPDP multicast join: " + spdp_transport->last_error());
            return;
        }

        if (!spdp_transport->set_multicast_interface(config.advertised_address) ||
            !spdp_transport->set_multicast_loopback(true)) {
            set_error("SPDP multicast configuration: " + spdp_transport->last_error());
            return;
        }

        multicast_destination = {config.multicast_group, *multicast_port};
        metatraffic_locator = {config.advertised_address, *metatraffic_port};
        valid.store(true);
        worker = std::thread([this] {
            run();
        });
    }

    void set_error(std::string message)
    {
        std::lock_guard<std::mutex> lock(mutex);
        error = std::move(message);
    }

    void run()
    {
        auto next_announcement = Clock::now();
        while (!stop.load()) {
            const auto now = Clock::now();
            const bool periodic_announcement = now >= next_announcement;
            if (periodic_announcement) {
                send_participant_announcement();
                next_announcement = now + config.announcement_period;
                announce_requested.store(true);
            }

            poll_spdp();
            poll_metatraffic();

            if (announce_requested.exchange(false)) {
                send_all_endpoint_announcements();
            }
            purge_expired_entries();
        }
    }

    void send_participant_announcement()
    {
        ParticipantDiscoveryData participant;
        participant.participant_guid = {
            config.guid_prefix,
            entity_id_participant};
        participant.metatraffic_unicast_locator = metatraffic_locator;
        participant.default_unicast_locator = config.user_unicast_locator;
        participant.lease_duration = config.lease_duration;
        participant.name = config.participant_name;

        std::vector<std::uint8_t> payload;
        std::string encode_error;
        if (!encode_participant_discovery_data(
                participant,
                serialization::Endianness::little,
                payload,
                encode_error)) {
            set_error(std::move(encode_error));
            return;
        }

        const rtps::DataMessage message{
            rtps::MessageHeader{
                rtps::ProtocolVersion{2, 3},
                rtps::VendorId{},
                config.guid_prefix},
            rtps::DataSubmessage{
                rtps::EntityId{},
                entity_id_spdp_announcer,
                rtps::SequenceNumber{1},
                std::move(payload)}};

        std::vector<std::uint8_t> bytes;
        if (!rtps::encode_data_message(
                message,
                serialization::Endianness::little,
                bytes,
                encode_error)) {
            set_error(std::move(encode_error));
            return;
        }

        if (!spdp_transport->send(bytes, multicast_destination)) {
            set_error("SPDP send: " + spdp_transport->last_error());
        }
    }

    void poll_spdp()
    {
        auto received = spdp_transport->receive(std::chrono::milliseconds(20));
        if (received.status == transport::ReceiveStatus::timeout) {
            return;
        }
        if (!received.ok()) {
            set_error("SPDP receive: " + received.error);
            return;
        }

        rtps::DataMessage message;
        std::string decode_error;
        if (!rtps::decode_data_message(
                received.datagram.payload,
                message,
                decode_error) ||
            !(message.data.writer_id == entity_id_spdp_announcer) ||
            same_prefix(message.header.guid_prefix, config.guid_prefix)) {
            return;
        }

        ParticipantDiscoveryData participant;
        if (!decode_participant_discovery_data(
                message.data.serialized_payload,
                participant,
                decode_error) ||
            !same_prefix(
                participant.participant_guid.prefix,
                message.header.guid_prefix)) {
            return;
        }

        bool is_new = false;
        {
            std::lock_guard<std::mutex> lock(mutex);
            const auto match = std::find_if(
                remote_participants.begin(),
                remote_participants.end(),
                [&participant](const RemoteParticipant& current) {
                    return current.data.participant_guid ==
                        participant.participant_guid;
                });
            if (match == remote_participants.end()) {
                remote_participants.push_back({participant, Clock::now()});
                is_new = true;
            } else {
                match->data = participant;
                match->last_seen = Clock::now();
            }
        }

        condition.notify_all();
        if (is_new) {
            announce_requested.store(true);
        }
    }

    void poll_metatraffic()
    {
        auto received = metatraffic_transport->receive(std::chrono::milliseconds(20));
        if (received.status == transport::ReceiveStatus::timeout) {
            return;
        }
        if (!received.ok()) {
            set_error("SEDP receive: " + received.error);
            return;
        }

        rtps::DataMessage message;
        std::string decode_error;
        if (!rtps::decode_data_message(
                received.datagram.payload,
                message,
                decode_error) ||
            same_prefix(message.header.guid_prefix, config.guid_prefix)) {
            return;
        }

        DiscoveredEndpointKind kind;
        if (message.data.writer_id == entity_id_sedp_publications_announcer) {
            kind = DiscoveredEndpointKind::writer;
        } else if (message.data.writer_id == entity_id_sedp_subscriptions_announcer) {
            kind = DiscoveredEndpointKind::reader;
        } else {
            return;
        }

        EndpointDiscoveryData endpoint;
        if (!decode_endpoint_discovery_data(
                message.data.serialized_payload,
                kind,
                endpoint,
                decode_error) ||
            !same_prefix(endpoint.endpoint_guid.prefix, message.header.guid_prefix)) {
            return;
        }

        {
            std::lock_guard<std::mutex> lock(mutex);
            const auto match = std::find_if(
                remote_endpoints.begin(),
                remote_endpoints.end(),
                [&endpoint](const RemoteEndpoint& current) {
                    return same_guid(
                        current.data.endpoint_guid,
                        endpoint.endpoint_guid);
                });
            if (match == remote_endpoints.end()) {
                remote_endpoints.push_back({std::move(endpoint), Clock::now()});
            } else {
                match->data = std::move(endpoint);
                match->last_seen = Clock::now();
            }
        }
        condition.notify_all();
    }

    void send_all_endpoint_announcements()
    {
        std::vector<LocalEndpoint> endpoints;
        std::vector<transport::Endpoint> destinations;
        {
            std::lock_guard<std::mutex> lock(mutex);
            endpoints = local_endpoints;
            destinations.reserve(remote_participants.size());
            for (const auto& participant : remote_participants) {
                destinations.push_back(
                    participant.data.metatraffic_unicast_locator);
            }
        }

        for (const auto& destination : destinations) {
            for (const auto& endpoint : endpoints) {
                send_endpoint_announcement(endpoint, destination);
            }
        }
    }

    void send_endpoint_announcement(
        const LocalEndpoint& endpoint,
        const transport::Endpoint& destination)
    {
        std::vector<std::uint8_t> payload;
        std::string encode_error;
        if (!encode_endpoint_discovery_data(
                endpoint.data,
                serialization::Endianness::little,
                payload,
                encode_error)) {
            set_error(std::move(encode_error));
            return;
        }

        const bool publication =
            endpoint.data.kind == DiscoveredEndpointKind::writer;
        const rtps::DataMessage message{
            rtps::MessageHeader{
                rtps::ProtocolVersion{2, 3},
                rtps::VendorId{},
                config.guid_prefix},
            rtps::DataSubmessage{
                publication
                    ? entity_id_sedp_publications_detector
                    : entity_id_sedp_subscriptions_detector,
                publication
                    ? entity_id_sedp_publications_announcer
                    : entity_id_sedp_subscriptions_announcer,
                endpoint.sequence_number,
                std::move(payload)}};

        std::vector<std::uint8_t> bytes;
        if (!rtps::encode_data_message(
                message,
                serialization::Endianness::little,
                bytes,
                encode_error)) {
            set_error(std::move(encode_error));
            return;
        }

        if (!metatraffic_transport->send(bytes, destination)) {
            set_error("SEDP send: " + metatraffic_transport->last_error());
        }
    }

    void purge_expired_entries()
    {
        const auto now = Clock::now();
        std::vector<rtps::GuidPrefix> expired_prefixes;
        {
            std::lock_guard<std::mutex> lock(mutex);
            remote_participants.erase(
                std::remove_if(
                    remote_participants.begin(),
                    remote_participants.end(),
                    [&now, &expired_prefixes](const RemoteParticipant& participant) {
                        if (now - participant.last_seen >
                            participant.data.lease_duration) {
                            expired_prefixes.push_back(
                                participant.data.participant_guid.prefix);
                            return true;
                        }
                        return false;
                    }),
                remote_participants.end());

            if (!expired_prefixes.empty()) {
                remote_endpoints.erase(
                    std::remove_if(
                        remote_endpoints.begin(),
                        remote_endpoints.end(),
                        [&expired_prefixes](const RemoteEndpoint& endpoint) {
                            return std::any_of(
                                expired_prefixes.begin(),
                                expired_prefixes.end(),
                                [&endpoint](const rtps::GuidPrefix& prefix) {
                                    return prefix == endpoint.data.endpoint_guid.prefix;
                                });
                        }),
                    remote_endpoints.end());
            }
        }
        if (!expired_prefixes.empty()) {
            condition.notify_all();
        }
    }

    void add_local_endpoint(EndpointDiscoveryData endpoint)
    {
        endpoint.participant_guid = {
            config.guid_prefix,
            entity_id_participant};
        endpoint.unicast_locator = config.user_unicast_locator;

        std::lock_guard<std::mutex> lock(mutex);
        const auto match = std::find_if(
            local_endpoints.begin(),
            local_endpoints.end(),
            [&endpoint](const LocalEndpoint& current) {
                return same_guid(
                    current.data.endpoint_guid,
                    endpoint.endpoint_guid);
            });
        if (match != local_endpoints.end()) {
            match->data = std::move(endpoint);
        } else {
            const bool publication =
                endpoint.kind == DiscoveredEndpointKind::writer;
            auto& next_sequence = publication
                ? next_publication_sequence
                : next_subscription_sequence;
            local_endpoints.push_back(LocalEndpoint{
                std::move(endpoint),
                rtps::SequenceNumber{next_sequence++}});
        }
        announce_requested.store(true);
    }

    void remove_local(const rtps::Guid& endpoint_guid)
    {
        std::lock_guard<std::mutex> lock(mutex);
        local_endpoints.erase(
            std::remove_if(
                local_endpoints.begin(),
                local_endpoints.end(),
                [&endpoint_guid](const LocalEndpoint& endpoint) {
                    return same_guid(endpoint.data.endpoint_guid, endpoint_guid);
                }),
            local_endpoints.end());
    }

    std::vector<EndpointDiscoveryData> find_readers(
        const std::string& topic_name,
        const std::string& type_name,
        bool writer_is_reliable) const
    {
        std::vector<EndpointDiscoveryData> readers;
        for (const auto& endpoint : remote_endpoints) {
            if (endpoint.data.kind == DiscoveredEndpointKind::reader &&
                endpoint.data.topic_name == topic_name &&
                endpoint.data.type_name == type_name &&
                endpoint.data.reliable == writer_is_reliable) {
                readers.push_back(endpoint.data);
            }
        }
        return readers;
    }

    DiscoveryConfig config;
    std::unique_ptr<transport::UdpTransport> spdp_transport;
    std::unique_ptr<transport::UdpTransport> metatraffic_transport;
    transport::Endpoint multicast_destination;
    transport::Endpoint metatraffic_locator;

    mutable std::mutex mutex;
    std::condition_variable condition;
    std::string error;
    std::vector<RemoteParticipant> remote_participants;
    std::vector<RemoteEndpoint> remote_endpoints;
    std::vector<LocalEndpoint> local_endpoints;
    std::int64_t next_publication_sequence{1};
    std::int64_t next_subscription_sequence{1};

    std::atomic<bool> valid{false};
    std::atomic<bool> stop{false};
    std::atomic<bool> announce_requested{false};
    std::thread worker;
};

DiscoveryService::DiscoveryService(DiscoveryConfig config)
    : impl_(std::make_unique<Impl>(std::move(config)))
{
}

DiscoveryService::~DiscoveryService() = default;

bool DiscoveryService::is_valid() const noexcept
{
    return impl_->valid.load();
}

std::string DiscoveryService::last_error() const
{
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->error;
}

void DiscoveryService::announce_local_endpoint(EndpointDiscoveryData endpoint)
{
    impl_->add_local_endpoint(std::move(endpoint));
}

void DiscoveryService::remove_local_endpoint(const rtps::Guid& endpoint_guid)
{
    impl_->remove_local(endpoint_guid);
}

std::optional<EndpointDiscoveryData> DiscoveryService::wait_for_reader(
    const std::string& topic_name,
    const std::string& type_name,
    bool writer_is_reliable,
    std::chrono::milliseconds timeout)
{
    auto readers = wait_for_readers(
        topic_name,
        type_name,
        writer_is_reliable,
        timeout);
    if (readers.empty()) {
        return std::nullopt;
    }
    return std::move(readers.front());
}

std::vector<EndpointDiscoveryData> DiscoveryService::wait_for_readers(
    const std::string& topic_name,
    const std::string& type_name,
    bool writer_is_reliable,
    std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(impl_->mutex);
    const auto predicate = [&] {
        return impl_->stop.load() ||
               !impl_->find_readers(
                    topic_name,
                    type_name,
                    writer_is_reliable).empty();
    };

    if (timeout.count() < 0) {
        impl_->condition.wait(lock, predicate);
    } else if (!impl_->condition.wait_for(lock, timeout, predicate)) {
        return {};
    }
    return impl_->find_readers(topic_name, type_name, writer_is_reliable);
}

std::vector<EndpointDiscoveryData> DiscoveryService::matching_readers(
    const std::string& topic_name,
    const std::string& type_name,
    bool writer_is_reliable) const
{
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->find_readers(topic_name, type_name, writer_is_reliable);
}

bool DiscoveryService::wait_for_participant(
    const rtps::GuidPrefix& participant_prefix,
    std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(impl_->mutex);
    const auto predicate = [&] {
        return impl_->stop.load() ||
               std::any_of(
                   impl_->remote_participants.begin(),
                   impl_->remote_participants.end(),
                   [&participant_prefix](const RemoteParticipant& participant) {
                       return participant.data.participant_guid.prefix ==
                           participant_prefix;
                   });
    };

    if (timeout.count() < 0) {
        impl_->condition.wait(lock, predicate);
        return predicate() && !impl_->stop.load();
    }
    return impl_->condition.wait_for(lock, timeout, predicate) &&
           !impl_->stop.load();
}

std::size_t DiscoveryService::remote_participant_count() const
{
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->remote_participants.size();
}

std::size_t DiscoveryService::remote_endpoint_count() const
{
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->remote_endpoints.size();
}

} // namespace mini_dds::discovery
