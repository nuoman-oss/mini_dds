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

class StatelessWriter {
public:
    StatelessWriter(
        transport::ITransport& transport,
        GuidPrefix participant_prefix,
        EntityId writer_id,
        transport::Endpoint remote_endpoint,
        EntityId reader_id = {},
        std::size_t history_depth = 1);

    bool write(std::vector<std::uint8_t> serialized_payload);

    [[nodiscard]] const std::string& last_error() const noexcept;
    [[nodiscard]] const history::WriterHistory& history() const noexcept;

private:
    transport::ITransport& transport_;
    MessageHeader message_header_;
    EntityId writer_id_;
    EntityId reader_id_;
    transport::Endpoint remote_endpoint_;
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
