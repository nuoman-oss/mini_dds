#include "mini_dds/history/History.h"

#include <algorithm>
#include <utility>

namespace mini_dds::history {

WriterHistory::WriterHistory(rtps::Guid writer_guid, std::size_t depth)
    : writer_guid_(writer_guid),
      depth_(std::max<std::size_t>(1, depth))
{
}

CacheChange WriterHistory::add(std::vector<std::uint8_t> serialized_payload)
{
    std::lock_guard<std::mutex> lock(mutex_);
    CacheChange change{
        ChangeKind::alive,
        writer_guid_,
        rtps::SequenceNumber{next_sequence_number_++},
        std::move(serialized_payload)};

    changes_.push_back(change);
    while (changes_.size() > depth_) {
        changes_.pop_front();
    }
    return change;
}

std::optional<CacheChange> WriterHistory::find(
    rtps::SequenceNumber sequence_number) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto match = std::find_if(
        changes_.begin(),
        changes_.end(),
        [sequence_number](const CacheChange& change) {
            return change.sequence_number == sequence_number;
        });
    return match == changes_.end()
        ? std::nullopt
        : std::optional<CacheChange>(*match);
}

std::size_t WriterHistory::size() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return changes_.size();
}

ReaderHistory::ReaderHistory(std::size_t depth)
    : depth_(std::max<std::size_t>(1, depth))
{
}

bool ReaderHistory::add(CacheChange change)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto duplicate = std::find_if(
        changes_.begin(),
        changes_.end(),
        [&change](const CacheChange& current) {
            return current.writer_guid == change.writer_guid &&
                   current.sequence_number == change.sequence_number;
        });
    if (duplicate != changes_.end()) {
        return false;
    }

    changes_.push_back(std::move(change));
    while (changes_.size() > depth_) {
        changes_.pop_front();
    }
    return true;
}

std::optional<CacheChange> ReaderHistory::take()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (changes_.empty()) {
        return std::nullopt;
    }

    CacheChange change = std::move(changes_.front());
    changes_.pop_front();
    return change;
}

bool ReaderHistory::contains(
    const rtps::Guid& writer_guid,
    rtps::SequenceNumber sequence_number) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return std::any_of(
        changes_.begin(),
        changes_.end(),
        [&writer_guid, sequence_number](const CacheChange& change) {
            return change.writer_guid == writer_guid &&
                   change.sequence_number == sequence_number;
        });
}

std::size_t ReaderHistory::size() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return changes_.size();
}

} // namespace mini_dds::history
