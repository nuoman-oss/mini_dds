#pragma once

#include "mini_dds/history/History.h"
#include "mini_dds/rtps/Reliability.h"
#include "mini_dds/rtps/StatelessEndpoint.h"
#include "mini_dds/transport/ITransport.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace mini_dds::rtps {

struct ReliableWriterConfig {
    std::chrono::milliseconds acknowledgment_timeout{500};
    std::chrono::milliseconds heartbeat_period{100};
    std::size_t max_retries{3};
    std::size_t history_depth{16};
    bool asynchronous{false};
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

    ReliableWriter(
        transport::ITransport& transport,
        GuidPrefix participant_prefix,
        EntityId writer_id,
        std::vector<ReaderProxy> readers,
        ReliableWriterConfig config = {});

    ~ReliableWriter();

    ReliableWriter(const ReliableWriter&) = delete;
    ReliableWriter& operator=(const ReliableWriter&) = delete;
    ReliableWriter(ReliableWriter&&) = delete;
    ReliableWriter& operator=(ReliableWriter&&) = delete;

    bool write(std::vector<std::uint8_t> serialized_payload);
    void set_readers(std::vector<ReaderProxy> readers);
    bool wait_for_acknowledgments(std::chrono::milliseconds timeout);

    [[nodiscard]] std::string last_error() const;
    [[nodiscard]] const history::WriterHistory& history() const noexcept;
    [[nodiscard]] std::size_t retransmission_count() const noexcept;
    [[nodiscard]] std::size_t matched_reader_count() const;
    [[nodiscard]] std::size_t pending_change_count() const;
    [[nodiscard]] bool asynchronous() const noexcept;

private:
    struct QueuedChange {
        history::CacheChange change;
        std::vector<ReaderProxy> readers;
        std::uint64_t submission_id{0};
    };

    bool deliver_change(
        const history::CacheChange& change,
        const std::vector<ReaderProxy>& readers,
        std::string& error);
    bool send_change(
        const history::CacheChange& change,
        const ReaderProxy& reader,
        std::string& error);
    bool send_heartbeat(
        SequenceNumber sequence_number,
        const ReaderProxy& reader,
        std::string& error);
    void worker_loop();

    transport::ITransport& transport_;
    MessageHeader message_header_;
    EntityId writer_id_;
    ReliableWriterConfig config_;
    history::WriterHistory history_;

    mutable std::mutex state_mutex_;
    std::mutex write_mutex_;
    std::condition_variable state_changed_;
    std::vector<ReaderProxy> readers_;
    std::deque<QueuedChange> pending_changes_;
    std::thread worker_;
    std::atomic<bool> stopping_{false};
    std::uint64_t submitted_count_{0};
    std::uint64_t completed_count_{0};
    std::size_t failed_count_{0};
    std::size_t observed_failure_count_{0};
    std::int32_t heartbeat_count_{0};
    std::atomic<std::size_t> retransmission_count_{0};
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
