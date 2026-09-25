#include "mini_dds/rtps/ReliableEndpoint.h"

#include "mini_dds/rtps/Data.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <utility>

namespace mini_dds::rtps {
namespace {

bool is_unknown_entity(const EntityId& entity_id)
{
    return entity_id == EntityId{};
}

bool targets_entity(const EntityId& target, const EntityId& local)
{
    return is_unknown_entity(target) || target == local;
}

bool acknack_acknowledges(
    const AckNackSubmessage& acknack,
    SequenceNumber sequence_number)
{
    if (sequence_number.value < acknack.reader_state.bitmap_base.value) {
        return true;
    }
    const auto offset = sequence_number.value -
        acknack.reader_state.bitmap_base.value;
    return offset >= 0 &&
           static_cast<std::uint64_t>(offset) < acknack.reader_state.num_bits &&
           !acknack.reader_state.contains(sequence_number);
}

bool acknack_has_requests(const AckNackSubmessage& acknack)
{
    return std::any_of(
        acknack.reader_state.bitmap.begin(),
        acknack.reader_state.bitmap.end(),
        [](std::uint32_t word) { return word != 0; });
}

std::vector<ReaderProxy> unique_readers(std::vector<ReaderProxy> readers)
{
    std::vector<ReaderProxy> result;
    result.reserve(readers.size());
    for (auto& reader : readers) {
        if (std::find(result.begin(), result.end(), reader) == result.end()) {
            result.push_back(std::move(reader));
        }
    }
    return result;
}

bool acknack_matches_reader(
    const AckNackSubmessage& acknack,
    const transport::Endpoint& source,
    const ReaderProxy& reader)
{
    return source == reader.endpoint &&
           (is_unknown_entity(reader.reader_id) ||
            targets_entity(acknack.reader_id, reader.reader_id));
}

} // namespace

ReliableWriter::ReliableWriter(
    transport::ITransport& transport,
    GuidPrefix participant_prefix,
    EntityId writer_id,
    transport::Endpoint remote_endpoint,
    EntityId reader_id,
    ReliableWriterConfig config)
    : ReliableWriter(
          transport,
          participant_prefix,
          writer_id,
          std::vector<ReaderProxy>{{std::move(remote_endpoint), reader_id}},
          config)
{
}

ReliableWriter::ReliableWriter(
    transport::ITransport& transport,
    GuidPrefix participant_prefix,
    EntityId writer_id,
    std::vector<ReaderProxy> readers,
    ReliableWriterConfig config)
    : transport_(transport),
      message_header_{ProtocolVersion{2, 3}, VendorId{}, participant_prefix},
      writer_id_(writer_id),
      config_(config),
      history_(Guid{participant_prefix, writer_id}, config.history_depth),
      readers_(unique_readers(std::move(readers)))
{
    if (config_.acknowledgment_timeout.count() <= 0) {
        config_.acknowledgment_timeout = std::chrono::milliseconds(1);
    }
    if (config_.heartbeat_period.count() <= 0) {
        config_.heartbeat_period = std::chrono::milliseconds(1);
    }
    if (config_.asynchronous) {
        worker_ = std::thread([this] {
            worker_loop();
        });
    }
}

ReliableWriter::~ReliableWriter()
{
    stopping_.store(true);
    state_changed_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
}

bool ReliableWriter::send_change(
    const history::CacheChange& change,
    const ReaderProxy& reader,
    std::string& error)
{
    const DataMessage message{
        message_header_,
        DataSubmessage{
            reader.reader_id,
            writer_id_,
            change.sequence_number,
            change.serialized_payload}};

    std::vector<std::uint8_t> bytes;
    if (!encode_data_message(
            message,
            serialization::Endianness::little,
            bytes,
            error)) {
        return false;
    }
    if (!transport_.send(bytes, reader.endpoint)) {
        error = transport_.last_error();
        return false;
    }
    return true;
}

bool ReliableWriter::send_heartbeat(
    SequenceNumber sequence_number,
    const ReaderProxy& reader,
    std::string& error)
{
    if (heartbeat_count_ == std::numeric_limits<std::int32_t>::max()) {
        heartbeat_count_ = 0;
    }
    HeartbeatSubmessage heartbeat;
    heartbeat.reader_id = reader.reader_id;
    heartbeat.writer_id = writer_id_;
    heartbeat.first_sequence_number = sequence_number;
    heartbeat.last_sequence_number = sequence_number;
    heartbeat.count = ++heartbeat_count_;

    std::vector<std::uint8_t> bytes;
    if (!encode_heartbeat_message(
            message_header_,
            heartbeat,
            serialization::Endianness::little,
            bytes,
            error)) {
        return false;
    }
    if (!transport_.send(bytes, reader.endpoint)) {
        error = transport_.last_error();
        return false;
    }
    return true;
}

bool ReliableWriter::deliver_change(
    const history::CacheChange& change,
    const std::vector<ReaderProxy>& readers,
    std::string& error)
{
    struct ReaderWriteState {
        ReaderProxy reader;
        bool acknowledged{false};
        std::size_t retries{0};
        std::chrono::steady_clock::time_point retry_deadline{};
        std::chrono::steady_clock::time_point heartbeat_deadline{};
    };
    std::vector<ReaderWriteState> states;
    states.reserve(readers.size());
    for (const auto& reader : readers) {
        states.push_back(ReaderWriteState{reader});
    }

    const auto all_acknowledged = [&states] {
        return std::all_of(
            states.begin(),
            states.end(),
            [](const ReaderWriteState& state) {
                return state.acknowledged;
            });
    };
    const auto pending_count = [&states] {
        return static_cast<std::size_t>(std::count_if(
            states.begin(),
            states.end(),
            [](const ReaderWriteState& state) {
                return !state.acknowledged;
            }));
    };

    auto now = std::chrono::steady_clock::now();
    for (auto& state : states) {
        if (!send_change(change, state.reader, error) ||
            !send_heartbeat(change.sequence_number, state.reader, error)) {
            return false;
        }
        state.retry_deadline = now + config_.acknowledgment_timeout;
        state.heartbeat_deadline = now + config_.heartbeat_period;
    }

    while (!all_acknowledged()) {
        if (stopping_.load()) {
            error = "ReliableWriter stopped before acknowledgment completed";
            return false;
        }

        now = std::chrono::steady_clock::now();
        for (auto& state : states) {
            if (state.acknowledged) {
                continue;
            }

            if (now >= state.retry_deadline) {
                if (state.retries == config_.max_retries) {
                    error = "timed out waiting for ACKNACK from " +
                        std::to_string(pending_count()) +
                        " reader(s) after the initial transmission and " +
                        std::to_string(config_.max_retries) + " retries";
                    return false;
                }
                ++state.retries;
                retransmission_count_.fetch_add(1);
                if (!send_change(change, state.reader, error) ||
                    !send_heartbeat(
                        change.sequence_number,
                        state.reader,
                        error)) {
                    return false;
                }
                state.retry_deadline = now + config_.acknowledgment_timeout;
                state.heartbeat_deadline = now + config_.heartbeat_period;
            } else if (now >= state.heartbeat_deadline) {
                if (!send_heartbeat(
                        change.sequence_number,
                        state.reader,
                        error)) {
                    return false;
                }
                state.heartbeat_deadline = now + config_.heartbeat_period;
            }
        }

        if (all_acknowledged()) {
            return true;
        }

        auto next_event = std::chrono::steady_clock::time_point::max();
        for (const auto& state : states) {
            if (!state.acknowledged) {
                next_event = std::min(
                    next_event,
                    std::min(state.retry_deadline, state.heartbeat_deadline));
            }
        }
        now = std::chrono::steady_clock::now();
        auto receive_timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
            next_event - now);
        if (receive_timeout.count() <= 0) {
            receive_timeout = std::chrono::milliseconds(1);
        }
        receive_timeout = std::min(
            receive_timeout,
            std::chrono::milliseconds(50));

        const auto received = transport_.receive(receive_timeout);
        if (received.status == transport::ReceiveStatus::timeout) {
            continue;
        }
        if (!received.ok()) {
            error = received.error;
            return false;
        }

        MessageHeader header;
        if (!decode_message_header(received.datagram.payload, header) ||
            header.version.major != 2 ||
            received.datagram.payload.size() <= message_header_size) {
            continue;
        }
        SubmessageHeader submessage_header;
        const auto* submessage =
            received.datagram.payload.data() + message_header_size;
        const auto submessage_size =
            received.datagram.payload.size() - message_header_size;
        if (!decode_submessage_header(
                submessage,
                submessage_size,
                submessage_header) ||
            submessage_header.kind != SubmessageKind::acknack) {
            continue;
        }

        AckNackSubmessage acknack;
        std::string decode_error;
        if (!decode_acknack_submessage(
                submessage,
                submessage_size,
                acknack,
                decode_error) ||
            !(acknack.writer_id == writer_id_) ||
            is_unknown_entity(acknack.reader_id)) {
            continue;
        }

        const auto state = std::find_if(
            states.begin(),
            states.end(),
            [&](const ReaderWriteState& current) {
                return !current.acknowledged &&
                       acknack_matches_reader(
                           acknack,
                           received.datagram.source,
                           current.reader);
            });
        if (state == states.end()) {
            continue;
        }
        if (acknack_acknowledges(acknack, change.sequence_number) &&
            !acknack_has_requests(acknack)) {
            state->acknowledged = true;
            continue;
        }
        if (acknack.reader_state.contains(change.sequence_number)) {
            if (state->retries == config_.max_retries) {
                error = "reader requested DATA after the retry limit was reached";
                return false;
            }
            ++state->retries;
            retransmission_count_.fetch_add(1);
            if (!send_change(change, state->reader, error) ||
                !send_heartbeat(
                    change.sequence_number,
                    state->reader,
                    error)) {
                return false;
            }
            now = std::chrono::steady_clock::now();
            state->retry_deadline = now + config_.acknowledgment_timeout;
            state->heartbeat_deadline = now + config_.heartbeat_period;
        }
    }

    return true;
}

bool ReliableWriter::write(std::vector<std::uint8_t> serialized_payload)
{
    std::lock_guard<std::mutex> write_lock(write_mutex_);

    std::vector<ReaderProxy> readers;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (readers_.empty()) {
            last_error_ = "ReliableWriter has no matched readers";
            return false;
        }
        if (stopping_.load()) {
            last_error_ = "ReliableWriter is stopping";
            return false;
        }
        readers = readers_;
        if (failed_count_ == observed_failure_count_) {
            last_error_.clear();
        }
    }

    auto change = history_.add(std::move(serialized_payload));
    if (!config_.asynchronous) {
        std::string error;
        const auto delivered = deliver_change(change, readers, error);
        std::lock_guard<std::mutex> lock(state_mutex_);
        last_error_ = std::move(error);
        return delivered;
    }

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        pending_changes_.push_back(QueuedChange{
            std::move(change),
            std::move(readers),
            ++submitted_count_});
    }
    state_changed_.notify_all();
    return true;
}

void ReliableWriter::worker_loop()
{
    while (!stopping_.load()) {
        QueuedChange queued;
        {
            std::unique_lock<std::mutex> lock(state_mutex_);
            state_changed_.wait(lock, [this] {
                return stopping_.load() || !pending_changes_.empty();
            });
            if (stopping_.load()) {
                return;
            }
            queued = std::move(pending_changes_.front());
            pending_changes_.pop_front();
        }

        std::string error;
        const auto delivered = deliver_change(
            queued.change,
            queued.readers,
            error);

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            completed_count_ = queued.submission_id;
            if (!delivered && !stopping_.load()) {
                ++failed_count_;
                last_error_ = std::move(error);
            }
        }
        state_changed_.notify_all();
    }
}

void ReliableWriter::set_readers(std::vector<ReaderProxy> readers)
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    readers_ = unique_readers(std::move(readers));
}

bool ReliableWriter::wait_for_acknowledgments(
    std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(state_mutex_);
    if (!config_.asynchronous) {
        return last_error_.empty();
    }

    const auto target = submitted_count_;
    const auto completed = [this, target] {
        return stopping_.load() || completed_count_ >= target;
    };
    bool ready = true;
    if (timeout.count() < 0) {
        state_changed_.wait(lock, completed);
    } else {
        ready = state_changed_.wait_for(lock, timeout, completed);
    }
    if (!ready) {
        last_error_ = "timed out waiting for asynchronous acknowledgments";
        return false;
    }
    if (stopping_.load()) {
        last_error_ = "ReliableWriter stopped before acknowledgment completed";
        return false;
    }
    if (failed_count_ != observed_failure_count_) {
        observed_failure_count_ = failed_count_;
        return false;
    }

    last_error_.clear();
    return true;
}

std::string ReliableWriter::last_error() const
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    return last_error_;
}

const history::WriterHistory& ReliableWriter::history() const noexcept
{
    return history_;
}

std::size_t ReliableWriter::retransmission_count() const noexcept
{
    return retransmission_count_.load();
}

std::size_t ReliableWriter::matched_reader_count() const
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    return readers_.size();
}

std::size_t ReliableWriter::pending_change_count() const
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    return static_cast<std::size_t>(submitted_count_ - completed_count_);
}

bool ReliableWriter::asynchronous() const noexcept
{
    return config_.asynchronous;
}

ReliableReader::ReliableReader(
    transport::ITransport& transport,
    GuidPrefix participant_prefix,
    EntityId reader_id,
    std::size_t history_depth)
    : transport_(transport),
      message_header_{ProtocolVersion{2, 3}, VendorId{}, participant_prefix},
      reader_id_(reader_id),
      history_(history_depth)
{
}

ReliableReader::WriterState& ReliableReader::writer_state(
    const Guid& writer_guid,
    const transport::Endpoint& reply_endpoint)
{
    const auto match = std::find_if(
        writer_states_.begin(),
        writer_states_.end(),
        [&writer_guid](const WriterState& state) {
            return state.writer_guid == writer_guid;
        });
    if (match != writer_states_.end()) {
        match->reply_endpoint = reply_endpoint;
        return *match;
    }

    writer_states_.push_back(WriterState{});
    auto& state = writer_states_.back();
    state.writer_guid = writer_guid;
    state.reply_endpoint = reply_endpoint;
    return state;
}

std::size_t ReliableReader::commit_contiguous(WriterState& state)
{
    std::size_t committed = 0;
    while (state.highest_contiguous < std::numeric_limits<std::int64_t>::max()) {
        const auto next = state.highest_contiguous + 1;
        const auto irrelevant = state.irrelevant.find(next);
        if (irrelevant != state.irrelevant.end()) {
            state.irrelevant.erase(irrelevant);
            state.received.erase(next);
            state.pending.erase(next);
            state.highest_contiguous = next;
            continue;
        }

        const auto pending = state.pending.find(next);
        if (pending == state.pending.end()) {
            break;
        }
        history_.add(std::move(pending->second));
        state.pending.erase(pending);
        state.received.erase(next);
        state.highest_contiguous = next;
        ++committed;
    }
    return committed;
}

bool ReliableReader::send_acknack(
    WriterState& state,
    const EntityId& writer_id)
{
    if (acknack_count_ == std::numeric_limits<std::int32_t>::max()) {
        acknack_count_ = 0;
    }

    AckNackSubmessage acknack;
    acknack.reader_id = reader_id_;
    acknack.writer_id = writer_id;
    acknack.count = ++acknack_count_;
    acknack.reader_state.bitmap_base = {
        state.highest_contiguous == std::numeric_limits<std::int64_t>::max()
            ? state.highest_contiguous
            : state.highest_contiguous + 1};

    std::int64_t last_known = state.last_announced;
    if (!state.pending.empty()) {
        last_known = std::max(last_known, state.pending.rbegin()->first);
    }
    if (last_known >= acknack.reader_state.bitmap_base.value) {
        const auto span = static_cast<std::uint64_t>(
            last_known - acknack.reader_state.bitmap_base.value) + 1U;
        acknack.reader_state.num_bits = static_cast<std::uint32_t>(
            std::min<std::uint64_t>(span, sequence_number_set_max_bits));
        acknack.reader_state.bitmap.assign(
            (acknack.reader_state.num_bits + 31U) / 32U,
            0);
        for (std::uint32_t offset = 0;
             offset < acknack.reader_state.num_bits;
             ++offset) {
            const auto sequence =
                acknack.reader_state.bitmap_base.value + offset;
            if (state.received.count(sequence) != 0 ||
                state.irrelevant.count(sequence) != 0) {
                continue;
            }
            acknack.reader_state.bitmap[offset / 32U] |=
                std::uint32_t{1} << (31U - (offset % 32U));
        }
    }

    acknack.final_flag = std::none_of(
        acknack.reader_state.bitmap.begin(),
        acknack.reader_state.bitmap.end(),
        [](std::uint32_t word) { return word != 0; });

    std::vector<std::uint8_t> bytes;
    if (!encode_acknack_message(
            message_header_,
            acknack,
            serialization::Endianness::little,
            bytes,
            last_error_)) {
        return false;
    }
    if (!transport_.send(bytes, state.reply_endpoint)) {
        last_error_ = transport_.last_error();
        return false;
    }
    return true;
}

ReaderReceiveResult ReliableReader::receive_once(
    std::chrono::milliseconds timeout)
{
    last_error_.clear();
    const auto received = transport_.receive(timeout);
    if (received.status == transport::ReceiveStatus::timeout) {
        return ReaderReceiveResult{ReaderReceiveStatus::timeout, {}};
    }
    if (!received.ok()) {
        return ReaderReceiveResult{ReaderReceiveStatus::error, received.error};
    }

    MessageHeader header;
    if (!decode_message_header(received.datagram.payload, header)) {
        return ReaderReceiveResult{
            ReaderReceiveStatus::error,
            "invalid RTPS message header"};
    }
    if (header.version.major != 2) {
        return ReaderReceiveResult{
            ReaderReceiveStatus::error,
            "unsupported RTPS major version"};
    }
    if (received.datagram.payload.size() <= message_header_size) {
        return ReaderReceiveResult{
            ReaderReceiveStatus::error,
            "RTPS message does not contain a submessage"};
    }

    const auto* submessage =
        received.datagram.payload.data() + message_header_size;
    const auto submessage_size =
        received.datagram.payload.size() - message_header_size;
    SubmessageHeader submessage_header;
    if (!decode_submessage_header(
            submessage,
            submessage_size,
            submessage_header)) {
        return ReaderReceiveResult{
            ReaderReceiveStatus::error,
            "truncated RTPS submessage header"};
    }

    if (submessage_header.kind == SubmessageKind::data) {
        DataSubmessage data;
        std::string error;
        if (!decode_data_submessage(
                submessage,
                submessage_size,
                data,
                error)) {
            return ReaderReceiveResult{ReaderReceiveStatus::error, std::move(error)};
        }
        if (!targets_entity(data.reader_id, reader_id_)) {
            return ReaderReceiveResult{ReaderReceiveStatus::ignored, {}};
        }

        auto& state = writer_state(
            Guid{header.guid_prefix, data.writer_id},
            received.datagram.source);
        const auto sequence = data.writer_sequence_number.value;
        if (!state.range_initialized && sequence == 1) {
            state.range_initialized = true;
        }
        state.last_announced = std::max(state.last_announced, sequence);
        if (sequence > state.highest_contiguous &&
            state.received.insert(sequence).second) {
            state.pending.emplace(
                sequence,
                history::CacheChange{
                    history::ChangeKind::alive,
                    Guid{header.guid_prefix, data.writer_id},
                    data.writer_sequence_number,
                    std::move(data.serialized_payload)});
        }
        const auto committed = state.range_initialized
            ? commit_contiguous(state)
            : 0;
        if (!send_acknack(state, data.writer_id)) {
            return ReaderReceiveResult{ReaderReceiveStatus::error, last_error_};
        }
        return ReaderReceiveResult{
            committed == 0
                ? ReaderReceiveStatus::ignored
                : ReaderReceiveStatus::sample,
            {}};
    }

    if (submessage_header.kind == SubmessageKind::heartbeat) {
        HeartbeatSubmessage heartbeat;
        std::string error;
        if (!decode_heartbeat_submessage(
                submessage,
                submessage_size,
                heartbeat,
                error)) {
            return ReaderReceiveResult{ReaderReceiveStatus::error, std::move(error)};
        }
        if (!targets_entity(heartbeat.reader_id, reader_id_)) {
            return ReaderReceiveResult{ReaderReceiveStatus::ignored, {}};
        }

        auto& state = writer_state(
            Guid{header.guid_prefix, heartbeat.writer_id},
            received.datagram.source);
        if (!state.range_initialized) {
            state.highest_contiguous =
                heartbeat.first_sequence_number.value - 1;
            state.range_initialized = true;
        } else if (heartbeat.first_sequence_number.value >
                   state.highest_contiguous + 1) {
            const auto first_available = heartbeat.first_sequence_number.value;
            for (auto iterator = state.pending.begin();
                 iterator != state.pending.end() && iterator->first < first_available;) {
                state.received.erase(iterator->first);
                iterator = state.pending.erase(iterator);
            }
            state.highest_contiguous = first_available - 1;
        }
        state.last_announced = std::max(
            state.last_announced,
            heartbeat.last_sequence_number.value);
        const auto committed = commit_contiguous(state);
        if (!send_acknack(state, heartbeat.writer_id)) {
            return ReaderReceiveResult{ReaderReceiveStatus::error, last_error_};
        }
        return ReaderReceiveResult{
            committed == 0
                ? ReaderReceiveStatus::ignored
                : ReaderReceiveStatus::sample,
            {}};
    }

    if (submessage_header.kind == SubmessageKind::gap) {
        GapSubmessage gap;
        std::string error;
        if (!decode_gap_submessage(
                submessage,
                submessage_size,
                gap,
                error)) {
            return ReaderReceiveResult{ReaderReceiveStatus::error, std::move(error)};
        }
        if (!targets_entity(gap.reader_id, reader_id_)) {
            return ReaderReceiveResult{ReaderReceiveStatus::ignored, {}};
        }

        auto& state = writer_state(
            Guid{header.guid_prefix, gap.writer_id},
            received.datagram.source);
        const auto gap_end = gap.gap_list.bitmap_base.value - 1;
        if (gap.gap_start.value <= state.highest_contiguous + 1 &&
            gap_end >= state.highest_contiguous + 1) {
            for (auto iterator = state.pending.begin();
                 iterator != state.pending.end() && iterator->first <= gap_end;) {
                state.received.erase(iterator->first);
                iterator = state.pending.erase(iterator);
            }
            state.highest_contiguous = gap_end;
            state.range_initialized = true;
        } else {
            const auto maximum_start_without_overflow =
                std::numeric_limits<std::int64_t>::max() -
                static_cast<std::int64_t>(sequence_number_set_max_bits) + 1;
            const auto remembered_end = gap.gap_start.value >
                    maximum_start_without_overflow
                ? gap_end
                : std::min<std::int64_t>(
                      gap_end,
                      gap.gap_start.value +
                          static_cast<std::int64_t>(sequence_number_set_max_bits) - 1);
            for (auto sequence = gap.gap_start.value;
                 sequence <= remembered_end;
                 ++sequence) {
                state.irrelevant.insert(sequence);
            }
        }
        for (std::uint32_t offset = 0; offset < gap.gap_list.num_bits; ++offset) {
            const SequenceNumber sequence{
                gap.gap_list.bitmap_base.value + offset};
            if (gap.gap_list.contains(sequence)) {
                state.irrelevant.insert(sequence.value);
            }
        }
        const auto committed = state.range_initialized
            ? commit_contiguous(state)
            : 0;
        if (!send_acknack(state, gap.writer_id)) {
            return ReaderReceiveResult{ReaderReceiveStatus::error, last_error_};
        }
        return ReaderReceiveResult{
            committed == 0
                ? ReaderReceiveStatus::ignored
                : ReaderReceiveStatus::sample,
            {}};
    }

    return ReaderReceiveResult{ReaderReceiveStatus::ignored, {}};
}

std::optional<history::CacheChange> ReliableReader::take()
{
    return history_.take();
}

const history::ReaderHistory& ReliableReader::history() const noexcept
{
    return history_;
}

} // namespace mini_dds::rtps
