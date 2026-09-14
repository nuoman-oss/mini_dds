#pragma once

#include "mini_dds/dds/Qos.h"
#include "mini_dds/dds/Topic.h"
#include "mini_dds/discovery/DiscoveryService.h"
#include "mini_dds/rtps/ReliableEndpoint.h"
#include "mini_dds/rtps/StatelessEndpoint.h"
#include "mini_dds/transport/UDPTransport.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mini_dds::dds {

struct ParticipantConfig {
    std::uint32_t domain_id{0};
    std::uint32_t participant_id{0};
    std::string bind_address{"0.0.0.0"};
    std::uint16_t listen_port{0};
    bool enable_discovery{false};
    std::string advertised_address{"127.0.0.1"};
    std::string participant_name{"mini_dds"};
    std::chrono::milliseconds announcement_period{1000};
    std::chrono::milliseconds lease_duration{10000};
};

struct DataWriterConfig {
    transport::Endpoint remote_endpoint;
    rtps::EntityId remote_reader_id{};
    std::optional<rtps::EntityId> writer_id;
    EndpointQos qos;
    std::chrono::milliseconds discovery_timeout{3000};
};

struct DataReaderConfig {
    std::optional<rtps::EntityId> reader_id;
    EndpointQos qos;
};

enum class TakeStatus {
    sample,
    timeout,
    error,
};

struct TakeResult {
    TakeStatus status{TakeStatus::error};
    std::string error;

    [[nodiscard]] bool ok() const noexcept
    {
        return status == TakeStatus::sample;
    }
};

namespace detail {

struct ParticipantState {
    ParticipantConfig config;
    rtps::GuidPrefix guid_prefix;
    std::shared_ptr<transport::UdpTransport> transport;
    std::shared_ptr<discovery::DiscoveryService> discovery;
    std::atomic<std::uint32_t> next_writer_key{1};
    std::atomic<std::uint32_t> next_reader_key{1};

    rtps::EntityId allocate_writer_id();
    rtps::EntityId allocate_reader_id();
};

} // namespace detail

template<typename T>
class DataWriter;

template<typename T>
class DataReader;

class Publisher {
public:
    template<typename T>
    std::unique_ptr<DataWriter<T>> create_datawriter(
        Topic<T> topic,
        DataWriterConfig config) const;

private:
    friend class DomainParticipant;
    explicit Publisher(std::shared_ptr<detail::ParticipantState> state);

    std::shared_ptr<detail::ParticipantState> state_;
};

class Subscriber {
public:
    template<typename T>
    std::unique_ptr<DataReader<T>> create_datareader(
        Topic<T> topic,
        DataReaderConfig config = {}) const;

private:
    friend class DomainParticipant;
    explicit Subscriber(std::shared_ptr<detail::ParticipantState> state);

    std::shared_ptr<detail::ParticipantState> state_;
};

class DomainParticipant {
public:
    explicit DomainParticipant(ParticipantConfig config = {});

    [[nodiscard]] bool is_valid() const noexcept;
    [[nodiscard]] const std::string& last_error() const noexcept;
    [[nodiscard]] std::uint32_t domain_id() const noexcept;
    [[nodiscard]] rtps::GuidPrefix guid_prefix() const noexcept;
    [[nodiscard]] transport::Endpoint local_endpoint() const;
    [[nodiscard]] bool discovery_enabled() const noexcept;
    [[nodiscard]] std::size_t discovered_participant_count() const;

    Publisher create_publisher() const;
    Subscriber create_subscriber() const;

    template<typename T>
    Topic<T> create_topic(
        std::string name,
        std::shared_ptr<const TypeSupport<T>> type_support) const
    {
        return Topic<T>(std::move(name), std::move(type_support));
    }

private:
    std::shared_ptr<detail::ParticipantState> state_;
    std::string last_error_;
};

template<typename T>
class DataWriter {
public:
    DataWriter(
        std::shared_ptr<detail::ParticipantState> state,
        Topic<T> topic,
        DataWriterConfig config)
        : state_(std::move(state)),
          topic_(std::move(topic)),
          qos_(config.qos),
          writer_id_(config.writer_id
              ? *config.writer_id
              : state_->allocate_writer_id()),
          remote_reader_id_(config.remote_reader_id),
          discovery_timeout_(config.discovery_timeout)
    {
        if (config.remote_endpoint.port != 0) {
            create_writer(std::move(config.remote_endpoint), remote_reader_id_);
        } else if (!state_->discovery || !state_->discovery->is_valid()) {
            initialization_error_ =
                "DataWriter requires a fixed remote endpoint or enabled discovery";
        }

        if (state_->discovery && state_->discovery->is_valid()) {
            discovery::EndpointDiscoveryData endpoint;
            endpoint.kind = discovery::DiscoveredEndpointKind::writer;
            endpoint.endpoint_guid = {state_->guid_prefix, writer_id_};
            endpoint.topic_name = topic_.name();
            endpoint.type_name = topic_.type_name();
            endpoint.reliable = qos_.reliability == ReliabilityKind::reliable;
            state_->discovery->announce_local_endpoint(std::move(endpoint));
            registered_with_discovery_ = true;
        }
    }

    ~DataWriter()
    {
        if (registered_with_discovery_ && state_->discovery) {
            state_->discovery->remove_local_endpoint(
                rtps::Guid{state_->guid_prefix, writer_id_});
        }
    }

    bool write(const T& value)
    {
        last_error_.clear();
        if (!initialization_error_.empty()) {
            last_error_ = initialization_error_;
            return false;
        }

        if (!best_effort_writer_ && !reliable_writer_) {
            const auto reader = state_->discovery->wait_for_reader(
                topic_.name(),
                topic_.type_name(),
                qos_.reliability == ReliabilityKind::reliable,
                discovery_timeout_);
            if (!reader) {
                last_error_ = "timed out waiting for a matching DataReader";
                const auto discovery_error = state_->discovery->last_error();
                if (!discovery_error.empty()) {
                    last_error_ += ": " + discovery_error;
                }
                return false;
            }

            create_writer(
                reader->unicast_locator,
                reader->endpoint_guid.entity_id);
        }

        std::vector<std::uint8_t> payload;
        if (!topic_.type_support().serialize(value, payload, last_error_)) {
            return false;
        }

        if (reliable_writer_) {
            if (!reliable_writer_->write(std::move(payload))) {
                last_error_ = reliable_writer_->last_error();
                return false;
            }
        } else {
            if (!best_effort_writer_->write(std::move(payload))) {
                last_error_ = best_effort_writer_->last_error();
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] const std::string& last_error() const noexcept
    {
        return last_error_;
    }

    [[nodiscard]] const std::string& topic_name() const noexcept
    {
        return topic_.name();
    }

    [[nodiscard]] rtps::EntityId entity_id() const noexcept
    {
        return writer_id_;
    }

private:
    void create_writer(
        transport::Endpoint remote_endpoint,
        rtps::EntityId remote_reader_id)
    {
        if (qos_.reliability == ReliabilityKind::reliable) {
            rtps::ReliableWriterConfig reliable_config;
            reliable_config.acknowledgment_timeout = qos_.acknowledgment_timeout;
            reliable_config.max_retries = qos_.max_retries;
            reliable_config.history_depth = qos_.history_depth;
            reliable_writer_ = std::make_unique<rtps::ReliableWriter>(
                *state_->transport,
                state_->guid_prefix,
                writer_id_,
                std::move(remote_endpoint),
                remote_reader_id,
                reliable_config);
        } else {
            best_effort_writer_ = std::make_unique<rtps::StatelessWriter>(
                *state_->transport,
                state_->guid_prefix,
                writer_id_,
                std::move(remote_endpoint),
                remote_reader_id,
                qos_.history_depth);
        }
    }

    std::shared_ptr<detail::ParticipantState> state_;
    Topic<T> topic_;
    EndpointQos qos_;
    rtps::EntityId writer_id_;
    rtps::EntityId remote_reader_id_;
    std::chrono::milliseconds discovery_timeout_;
    std::unique_ptr<rtps::StatelessWriter> best_effort_writer_;
    std::unique_ptr<rtps::ReliableWriter> reliable_writer_;
    std::string initialization_error_;
    std::string last_error_;
    bool registered_with_discovery_{false};
};

template<typename T>
class DataReader {
public:
    DataReader(
        std::shared_ptr<detail::ParticipantState> state,
        Topic<T> topic,
        DataReaderConfig config)
        : state_(std::move(state)),
          topic_(std::move(topic)),
          qos_(config.qos),
          reader_id_(config.reader_id
              ? *config.reader_id
              : state_->allocate_reader_id())
    {
        if (qos_.reliability == ReliabilityKind::reliable) {
            reliable_reader_ = std::make_unique<rtps::ReliableReader>(
                *state_->transport,
                state_->guid_prefix,
                reader_id_,
                qos_.history_depth);
        } else {
            best_effort_reader_ = std::make_unique<rtps::StatelessReader>(
                *state_->transport,
                reader_id_,
                qos_.history_depth);
        }

        if (state_->discovery && state_->discovery->is_valid()) {
            discovery::EndpointDiscoveryData endpoint;
            endpoint.kind = discovery::DiscoveredEndpointKind::reader;
            endpoint.endpoint_guid = {state_->guid_prefix, reader_id_};
            endpoint.topic_name = topic_.name();
            endpoint.type_name = topic_.type_name();
            endpoint.reliable = qos_.reliability == ReliabilityKind::reliable;
            state_->discovery->announce_local_endpoint(std::move(endpoint));
            registered_with_discovery_ = true;
        }
    }

    ~DataReader()
    {
        if (registered_with_discovery_ && state_->discovery) {
            state_->discovery->remove_local_endpoint(
                rtps::Guid{state_->guid_prefix, reader_id_});
        }
    }

    TakeResult take(T& value, std::chrono::milliseconds timeout)
    {
        const bool wait_forever = timeout.count() < 0;
        const auto deadline = std::chrono::steady_clock::now() +
            (wait_forever ? std::chrono::milliseconds(0) : timeout);
        bool attempted_receive = false;

        while (true) {
            auto cached = reliable_reader_
                ? reliable_reader_->take()
                : best_effort_reader_->take();
            if (cached) {
                std::string error;
                if (!topic_.type_support().deserialize(
                        cached->serialized_payload,
                        value,
                        error)) {
                    return TakeResult{TakeStatus::error, std::move(error)};
                }
                return TakeResult{TakeStatus::sample, {}};
            }

            std::chrono::milliseconds remaining{-1};
            if (!wait_forever) {
                const auto now = std::chrono::steady_clock::now();
                if (attempted_receive && now >= deadline) {
                    return TakeResult{TakeStatus::timeout, {}};
                }
                remaining = now >= deadline
                    ? std::chrono::milliseconds(0)
                    : std::chrono::duration_cast<std::chrono::milliseconds>(
                          deadline - now);
            }

            const auto receive_result = reliable_reader_
                ? reliable_reader_->receive_once(remaining)
                : best_effort_reader_->receive_once(remaining);
            attempted_receive = true;
            if (receive_result.status == rtps::ReaderReceiveStatus::timeout) {
                return TakeResult{TakeStatus::timeout, {}};
            }
            if (receive_result.status == rtps::ReaderReceiveStatus::error) {
                return TakeResult{TakeStatus::error, receive_result.error};
            }
        }
    }

    [[nodiscard]] const std::string& topic_name() const noexcept
    {
        return topic_.name();
    }

    [[nodiscard]] rtps::EntityId entity_id() const noexcept
    {
        return reader_id_;
    }

private:
    std::shared_ptr<detail::ParticipantState> state_;
    Topic<T> topic_;
    EndpointQos qos_;
    rtps::EntityId reader_id_;
    std::unique_ptr<rtps::StatelessReader> best_effort_reader_;
    std::unique_ptr<rtps::ReliableReader> reliable_reader_;
    bool registered_with_discovery_{false};
};

template<typename T>
std::unique_ptr<DataWriter<T>> Publisher::create_datawriter(
    Topic<T> topic,
    DataWriterConfig config) const
{
    return std::make_unique<DataWriter<T>>(
        state_,
        std::move(topic),
        std::move(config));
}

template<typename T>
std::unique_ptr<DataReader<T>> Subscriber::create_datareader(
    Topic<T> topic,
    DataReaderConfig config) const
{
    return std::make_unique<DataReader<T>>(
        state_,
        std::move(topic),
        std::move(config));
}

} // namespace mini_dds::dds
