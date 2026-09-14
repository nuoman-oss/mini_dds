#include "mini_dds/rtps/StatelessEndpoint.h"

#include <algorithm>
#include <utility>

namespace mini_dds::rtps {
namespace {

bool is_unknown_entity(const EntityId& entity_id)
{
    return entity_id == EntityId{};
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

} // namespace

StatelessWriter::StatelessWriter(
    transport::ITransport& transport,
    GuidPrefix participant_prefix,
    EntityId writer_id,
    transport::Endpoint remote_endpoint,
    EntityId reader_id,
    std::size_t history_depth)
    : StatelessWriter(
          transport,
          participant_prefix,
          writer_id,
          std::vector<ReaderProxy>{{std::move(remote_endpoint), reader_id}},
          history_depth)
{
}

StatelessWriter::StatelessWriter(
    transport::ITransport& transport,
    GuidPrefix participant_prefix,
    EntityId writer_id,
    std::vector<ReaderProxy> readers,
    std::size_t history_depth)
    : transport_(transport),
      message_header_{ProtocolVersion{2, 3}, VendorId{}, participant_prefix},
      writer_id_(writer_id),
      readers_(unique_readers(std::move(readers))),
      history_(Guid{participant_prefix, writer_id}, history_depth)
{
}

bool StatelessWriter::write(std::vector<std::uint8_t> serialized_payload)
{
    last_error_.clear();
    if (readers_.empty()) {
        last_error_ = "StatelessWriter has no matched readers";
        return false;
    }
    const auto change = history_.add(std::move(serialized_payload));

    for (const auto& reader : readers_) {
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
                last_error_)) {
            return false;
        }

        if (!transport_.send(bytes, reader.endpoint)) {
            last_error_ = transport_.last_error();
            return false;
        }
    }

    return true;
}

void StatelessWriter::set_readers(std::vector<ReaderProxy> readers)
{
    readers_ = unique_readers(std::move(readers));
}

const std::string& StatelessWriter::last_error() const noexcept
{
    return last_error_;
}

const history::WriterHistory& StatelessWriter::history() const noexcept
{
    return history_;
}

std::size_t StatelessWriter::matched_reader_count() const noexcept
{
    return readers_.size();
}

StatelessReader::StatelessReader(
    transport::ITransport& transport,
    EntityId reader_id,
    std::size_t history_depth)
    : transport_(transport),
      reader_id_(reader_id),
      history_(history_depth)
{
}

ReaderReceiveResult StatelessReader::receive_once(std::chrono::milliseconds timeout)
{
    const auto transport_result = transport_.receive(timeout);
    if (transport_result.status == transport::ReceiveStatus::timeout) {
        return ReaderReceiveResult{ReaderReceiveStatus::timeout, {}};
    }

    if (!transport_result.ok()) {
        return ReaderReceiveResult{
            ReaderReceiveStatus::error,
            transport_result.error};
    }

    DataMessage message;
    std::string error;
    if (!decode_data_message(transport_result.datagram.payload, message, error)) {
        return ReaderReceiveResult{ReaderReceiveStatus::error, std::move(error)};
    }

    if (message.header.version.major != 2) {
        return ReaderReceiveResult{
            ReaderReceiveStatus::error,
            "unsupported RTPS major version"};
    }

    if (!is_unknown_entity(message.data.reader_id) &&
        !(message.data.reader_id == reader_id_)) {
        return ReaderReceiveResult{ReaderReceiveStatus::ignored, {}};
    }

    history::CacheChange change{
        history::ChangeKind::alive,
        Guid{message.header.guid_prefix, message.data.writer_id},
        message.data.writer_sequence_number,
        std::move(message.data.serialized_payload)};

    if (!history_.add(std::move(change))) {
        return ReaderReceiveResult{ReaderReceiveStatus::ignored, {}};
    }

    return ReaderReceiveResult{ReaderReceiveStatus::sample, {}};
}

std::optional<history::CacheChange> StatelessReader::take()
{
    return history_.take();
}

const history::ReaderHistory& StatelessReader::history() const noexcept
{
    return history_;
}

} // namespace mini_dds::rtps
