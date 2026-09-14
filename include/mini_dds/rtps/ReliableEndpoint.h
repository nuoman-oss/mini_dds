#pragma once

#include "mini_dds/history/History.h"
#include "mini_dds/rtps/Reliability.h"
#include "mini_dds/rtps/StatelessEndpoint.h"
#include "mini_dds/transport/ITransport.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace mini_dds::rtps {

struct ReliableWriterConfig {
    std::chrono::milliseconds acknowledgment_timeout{500};
    std::size_t max_retries{3};
    std::size_t history_depth{16};
};

class ReliableWriter {
public:
    ReliableWriter(
        transport::ITransport& transport,
        GuidPrefix participant_prefix,
        EntityId writer_id,
        transport::Endpoint remote_endpoint,
        EntityId reader_id,
        ReliableWriterConfig config = {});

    bool write(std::vector<std::uint8_t> serialized_payload);

    [[nodiscard]] const std::string& last_error() const noexcept;
    [[nodiscard]] const history::WriterHistory& history() const noexcept;
    [[nodiscard]] std::size_t retransmission_count() const noexcept;

private:
    bool send_change(const history::CacheChange& change);
    bool send_heartbeat(SequenceNumber sequence_number);

    transport::ITransport& transport_;
    MessageHeader message_header_;
    EntityId writer_id_;
    EntityId reader_id_;
    transport::Endpoint remote_endpoint_;
    ReliableWriterConfig config_;
    history::WriterHistory history_;
    std::int32_t heartbeat_count_{0};
    std::size_t retransmission_count_{0};
    std::string last_error_;
};

class ReliableReader {
public:
    ReliableReader(
        transport::ITransport& transport,
        GuidPrefix participant_prefix,
        EntityId reader_id,
        std::size_t history_depth = 16);

    ReaderReceiveResult receive_once(std::chrono::milliseconds timeout);
    std::optional<history::CacheChange> take();

    [[nodiscard]] const history::ReaderHistory& history() const noexcept;

private:
    struct WriterState {
        Guid writer_guid;
        transport::Endpoint reply_endpoint;
        std::int64_t highest_contiguous{0};
        std::int64_t last_announced{0};
        bool range_initialized{false};
        std::set<std::int64_t> received;
        std::set<std::int64_t> irrelevant;
        std::map<std::int64_t, history::CacheChange> pending;
    };

    WriterState& writer_state(
        const Guid& writer_guid,
        const transport::Endpoint& reply_endpoint);
    std::size_t commit_contiguous(WriterState& state);
    bool send_acknack(WriterState& state, const EntityId& writer_id);

    transport::ITransport& transport_;
    MessageHeader message_header_;
    EntityId reader_id_;
    history::ReaderHistory history_;
    std::vector<WriterState> writer_states_;
    std::int32_t acknack_count_{0};
    std::string last_error_;
};

} // namespace mini_dds::rtps
