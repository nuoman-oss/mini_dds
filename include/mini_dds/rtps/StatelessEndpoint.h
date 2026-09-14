#pragma once

#include "mini_dds/history/History.h"
#include "mini_dds/rtps/Data.h"
#include "mini_dds/transport/ITransport.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace mini_dds::rtps {

struct ReaderProxy {
    transport::Endpoint endpoint;
    EntityId reader_id;
};

inline bool operator==(const ReaderProxy& lhs, const ReaderProxy& rhs)
{
    return lhs.endpoint == rhs.endpoint && lhs.reader_id == rhs.reader_id;
}

class StatelessWriter {
public:
    StatelessWriter(
        transport::ITransport& transport,
        GuidPrefix participant_prefix,
        EntityId writer_id,
        transport::Endpoint remote_endpoint,
        EntityId reader_id = {},
        std::size_t history_depth = 1);

    StatelessWriter(
        transport::ITransport& transport,
        GuidPrefix participant_prefix,
        EntityId writer_id,
        std::vector<ReaderProxy> readers,
        std::size_t history_depth = 1);

    bool write(std::vector<std::uint8_t> serialized_payload);
    void set_readers(std::vector<ReaderProxy> readers);

    [[nodiscard]] const std::string& last_error() const noexcept;
    [[nodiscard]] const history::WriterHistory& history() const noexcept;
    [[nodiscard]] std::size_t matched_reader_count() const noexcept;

private:
    transport::ITransport& transport_;
    MessageHeader message_header_;
    EntityId writer_id_;
    std::vector<ReaderProxy> readers_;
    history::WriterHistory history_;
    std::string last_error_;
};

enum class ReaderReceiveStatus {
    sample,
    timeout,
    ignored,
    error,
};

struct ReaderReceiveResult {
    ReaderReceiveStatus status{ReaderReceiveStatus::error};
    std::string error;
};

class StatelessReader {
public:
    StatelessReader(
        transport::ITransport& transport,
        EntityId reader_id,
        std::size_t history_depth = 1);

    ReaderReceiveResult receive_once(std::chrono::milliseconds timeout);
    std::optional<history::CacheChange> take();

    [[nodiscard]] const history::ReaderHistory& history() const noexcept;

private:
    transport::ITransport& transport_;
    EntityId reader_id_;
    history::ReaderHistory history_;
};

} // namespace mini_dds::rtps
