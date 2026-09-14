#pragma once

#include "mini_dds/dds/Qos.h"
#include "mini_dds/dds/Topic.h"
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
};

struct DataWriterConfig {
    transport::Endpoint remote_endpoint;
    rtps::EntityId remote_reader_id{};
    std::optional<rtps::EntityId> writer_id;
    EndpointQos qos;
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
          writer_(
              *state_->transport,
              state_->guid_prefix,
              writer_id_,
              std::move(config.remote_endpoint),
              config.remote_reader_id,
              qos_.history_depth)
    {
    }

    bool write(const T& value)
    {
        last_error_.clear();
        if (qos_.reliability != ReliabilityKind::best_effort) {
            last_error_ = "reliable DataWriter is not implemented";
            return false;
        }

        std::vector<std::uint8_t> payload;
        if (!topic_.type_support().serialize(value, payload, last_error_)) {
            return false;
        }

        if (!writer_.write(std::move(payload))) {
            last_error_ = writer_.last_error();
            return false;
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
    std::shared_ptr<detail::ParticipantState> state_;
    Topic<T> topic_;
    EndpointQos qos_;
    rtps::EntityId writer_id_;
    rtps::StatelessWriter writer_;
    std::string last_error_;
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
              : state_->allocate_reader_id()),
          reader_(*state_->transport, reader_id_, qos_.history_depth)
    {
    }

    TakeResult take(T& value, std::chrono::milliseconds timeout)
    {
        if (qos_.reliability != ReliabilityKind::best_effort) {
            return TakeResult{TakeStatus::error, "reliable DataReader is not implemented"};
        }

        const bool wait_forever = timeout.count() < 0;
        const auto deadline = std::chrono::steady_clock::now() +
            (wait_forever ? std::chrono::milliseconds(0) : timeout);
        bool attempted_receive = false;

        while (true) {
            if (auto cached = reader_.take()) {
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

            const auto receive_result = reader_.receive_once(remaining);
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
    rtps::StatelessReader reader_;
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
