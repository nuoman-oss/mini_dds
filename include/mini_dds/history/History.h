#pragma once

#include "mini_dds/history/CacheChange.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

namespace mini_dds::history {

class WriterHistory {
public:
    explicit WriterHistory(rtps::Guid writer_guid, std::size_t depth = 1);

    CacheChange add(std::vector<std::uint8_t> serialized_payload);
    [[nodiscard]] std::optional<CacheChange> find(
        rtps::SequenceNumber sequence_number) const;
    [[nodiscard]] std::size_t size() const;

private:
    rtps::Guid writer_guid_;
    std::size_t depth_;
    std::int64_t next_sequence_number_{1};
    mutable std::mutex mutex_;
    std::deque<CacheChange> changes_;
};

class ReaderHistory {
public:
    explicit ReaderHistory(std::size_t depth = 1);

    bool add(CacheChange change);
    std::optional<CacheChange> take();
    [[nodiscard]] bool contains(
        const rtps::Guid& writer_guid,
        rtps::SequenceNumber sequence_number) const;
    [[nodiscard]] std::size_t size() const;

private:
    std::size_t depth_;
    mutable std::mutex mutex_;
    std::deque<CacheChange> changes_;
};

} // namespace mini_dds::history
