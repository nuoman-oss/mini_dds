#include "mini_dds/dds/DomainParticipant.h"
#include "mini_dds/dds/TypeSupport.h"
#include "mini_dds/discovery/DiscoveryData.h"
#include "mini_dds/discovery/ParameterList.h"
#include "mini_dds/discovery/PortMapping.h"
#include "mini_dds/discovery/DiscoveryService.h"
#include "mini_dds/history/History.h"
#include "mini_dds/rtps/Data.h"
#include "mini_dds/rtps/Message.h"
#include "mini_dds/rtps/Reliability.h"
#include "mini_dds/rtps/ReliableEndpoint.h"
#include "mini_dds/rtps/StatelessEndpoint.h"
#include "mini_dds/serialization/Cdr.h"
#include "mini_dds/serialization/SerializedPayload.h"
#include "mini_dds/transport/UDPTransport.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

int failures = 0;

void check(bool condition, const std::string& message)
{
    if (!condition) {
        ++failures;
        std::cerr << "FAILED: " << message << '\n';
    }
}

struct InMemoryTransportState {
    std::mutex mutex;
    std::condition_variable ready;
    std::array<std::deque<mini_dds::transport::Datagram>, 3> queues;
    std::array<std::size_t, 3> data_send_counts{};
    bool drop_first_writer_data{false};
    std::uint16_t drop_destination_port{0};
    std::size_t dropped_data_count{0};
};

class InMemoryTransport final : public mini_dds::transport::ITransport {
public:
    InMemoryTransport(
        std::shared_ptr<InMemoryTransportState> state,
        std::size_t side)
        : state_(std::move(state)),
          side_(side),
          local_{"memory", static_cast<std::uint16_t>(31000U + side)}
    {
    }

    bool send(
        const std::vector<std::uint8_t>& data,
        const mini_dds::transport::Endpoint& destination) override
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        if (destination.address != "memory" || destination.port < 31000U ||
            destination.port >= 31000U + state_->queues.size()) {
            last_error_ = "invalid in-memory destination";
            return false;
        }
        const auto destination_side = static_cast<std::size_t>(
            destination.port - 31000U);
        const bool is_data =
            data.size() > mini_dds::rtps::message_header_size &&
            data[mini_dds::rtps::message_header_size] ==
                static_cast<std::uint8_t>(mini_dds::rtps::SubmessageKind::data);
        if (side_ == 0 && is_data) {
            ++state_->data_send_counts[destination_side];
        }
        if (side_ == 0 && is_data && state_->drop_first_writer_data &&
            state_->dropped_data_count == 0 &&
            (state_->drop_destination_port == 0 ||
             state_->drop_destination_port == destination.port)) {
            ++state_->dropped_data_count;
            return true;
        }

        state_->queues[destination_side].push_back({data, local_});
        state_->ready.notify_all();
        return true;
    }

    mini_dds::transport::ReceiveResult receive(
        std::chrono::milliseconds timeout) override
    {
        std::unique_lock<std::mutex> lock(state_->mutex);
        const auto available = [this] {
            return !state_->queues[side_].empty();
        };
        if (timeout.count() < 0) {
            state_->ready.wait(lock, available);
        } else if (!state_->ready.wait_for(lock, timeout, available)) {
            return {mini_dds::transport::ReceiveStatus::timeout, {}, {}};
        }

        auto datagram = std::move(state_->queues[side_].front());
        state_->queues[side_].pop_front();
        return {
            mini_dds::transport::ReceiveStatus::ok,
            std::move(datagram),
            {}};
    }

    [[nodiscard]] bool is_open() const noexcept override
    {
        return true;
    }

    [[nodiscard]] mini_dds::transport::Endpoint local_endpoint() const override
    {
        return local_;
    }

    [[nodiscard]] const std::string& last_error() const noexcept override
    {
        return last_error_;
    }

private:
    std::shared_ptr<InMemoryTransportState> state_;
    std::size_t side_;
    mini_dds::transport::Endpoint local_;
    std::string last_error_;
};

void test_cdr_little_endian_round_trip()
{
    using namespace mini_dds::serialization;

    CdrWriter writer(Endianness::little);
    writer.write_uint8(0xaa);
    writer.write_uint32(0x11223344);
    writer.write_string("hi");

    const std::vector<std::uint8_t> expected{
        0xaa, 0x00, 0x00, 0x00,
        0x44, 0x33, 0x22, 0x11,
        0x03, 0x00, 0x00, 0x00,
        'h', 'i', 0x00};
    check(writer.data() == expected, "CDR little-endian bytes and alignment");

    CdrReader reader(writer.data(), Endianness::little);
    std::uint8_t first = 0;
    std::uint32_t number = 0;
    std::string text;
    check(reader.read_uint8(first) && first == 0xaa, "CDR uint8 round trip");
    check(
        reader.read_uint32(number) && number == 0x11223344,
        "CDR uint32 round trip");
    check(reader.read_string(text) && text == "hi", "CDR string round trip");
    check(reader.valid() && reader.remaining() == 0, "CDR reader consumes input");
}

void test_cdr_big_endian_and_invalid_input()
{
    using namespace mini_dds::serialization;

    CdrWriter writer(Endianness::big);
    writer.write_uint16(0x1234);
    writer.write_uint32(0x55667788);
    const std::vector<std::uint8_t> expected{
        0x12, 0x34, 0x00, 0x00, 0x55, 0x66, 0x77, 0x88};
    check(writer.data() == expected, "CDR big-endian bytes and alignment");

    const std::vector<std::uint8_t> truncated{0x01, 0x00};
    CdrReader reader(truncated, Endianness::little);
    std::uint32_t value = 0;
    check(!reader.read_uint32(value) && !reader.valid(), "CDR rejects truncated input");
}

void test_rtps_message_header()
{
    using namespace mini_dds::rtps;

    MessageHeader header;
    header.version = {2, 3};
    header.vendor_id = {{0x01, 0x02}};
    for (std::size_t index = 0; index < header.guid_prefix.value.size(); ++index) {
        header.guid_prefix.value[index] = static_cast<std::uint8_t>(index + 1U);
    }

    const auto bytes = encode_message_header(header);
    check(
        std::equal(protocol_magic.begin(), protocol_magic.end(), bytes.begin()),
        "RTPS header starts with magic");
    check(bytes[4] == 2 && bytes[5] == 3, "RTPS version bytes");
    check(bytes[6] == 0x01 && bytes[7] == 0x02, "RTPS vendor bytes");

    MessageHeader decoded;
    check(
        decode_message_header(bytes.data(), bytes.size(), decoded),
        "RTPS header decodes");
    check(decoded.version == header.version, "RTPS version round trip");
    check(decoded.vendor_id == header.vendor_id, "RTPS vendor round trip");
    check(decoded.guid_prefix == header.guid_prefix, "RTPS GUID prefix round trip");

    auto invalid = bytes;
    invalid[0] = 'X';
    check(
        !decode_message_header(invalid.data(), invalid.size(), decoded),
        "RTPS header rejects invalid magic");
    check(
        !decode_message_header(bytes.data(), message_header_size - 1U, decoded),
        "RTPS header rejects truncated input");
}

void test_rtps_submessage_header()
{
    using namespace mini_dds::rtps;

    const SubmessageHeader little{SubmessageKind::data, 0x01, 0x1234};
    const auto little_bytes = encode_submessage_header(little);
    check(
        little_bytes == std::array<std::uint8_t, 4>{0x15, 0x01, 0x34, 0x12},
        "little-endian submessage header bytes");

    SubmessageHeader decoded;
    check(
        decode_submessage_header(little_bytes.data(), little_bytes.size(), decoded),
        "little-endian submessage header decodes");
    check(
        decoded.kind == SubmessageKind::data && decoded.length == 0x1234,
        "little-endian submessage header round trip");

    const SubmessageHeader big{SubmessageKind::heartbeat, 0x00, 0x1234};
    const auto big_bytes = encode_submessage_header(big);
    check(
        big_bytes == std::array<std::uint8_t, 4>{0x07, 0x00, 0x12, 0x34},
        "big-endian submessage header bytes");
}

void test_serialized_string_payload()
{
    using namespace mini_dds::serialization;

    const auto little = serialize_string_payload("hello", Endianness::little);
    check(
        little.size() >= 4 && little[0] == 0x00 && little[1] == 0x01,
        "CDR little-endian encapsulation header");

    std::string decoded;
    std::string error;
    check(
        deserialize_string_payload(little, decoded, error) && decoded == "hello",
        "serialized string payload round trip: " + error);

    const auto big = serialize_string_payload("world", Endianness::big);
    check(
        big.size() >= 4 && big[0] == 0x00 && big[1] == 0x00,
        "CDR big-endian encapsulation header");
    check(
        deserialize_string_payload(big, decoded, error) && decoded == "world",
        "big-endian string payload round trip: " + error);

    auto padded = little;
    padded.insert(padded.end(), {0x00, 0x00, 0x00});
    check(
        deserialize_string_payload(padded, decoded, error) && decoded == "hello",
        "serialized payload accepts RTPS alignment padding: " + error);

    std::vector<std::uint8_t> unsupported{0x12, 0x34, 0x00, 0x00};
    check(
        !deserialize_string_payload(unsupported, decoded, error),
        "serialized payload rejects unknown representation");
}

void test_rtps_data_message()
{
    using namespace mini_dds::rtps;

    DataMessage message;
    message.header.version = {2, 3};
    message.header.vendor_id = {{0x00, 0x00}};
    message.header.guid_prefix.value = {
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
        0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c};
    message.data.reader_id.value = {0x00, 0x00, 0x01, 0x04};
    message.data.writer_id.value = {0x00, 0x00, 0x02, 0x03};
    message.data.writer_sequence_number = {0x0000000211223344LL};
    message.data.serialized_payload = {0x00, 0x01, 0x00, 0x00, 0xaa, 0xbb};

    std::vector<std::uint8_t> bytes;
    std::string error;
    check(
        encode_data_message(
            message,
            mini_dds::serialization::Endianness::little,
            bytes,
            error),
        "RTPS DATA message encodes: " + error);
    check(bytes.size() == 20U + 4U + 20U + 6U, "RTPS DATA encoded size");
    check(
        bytes.size() > 24U && bytes[20] == 0x15 && bytes[21] == 0x05,
        "RTPS DATA kind and flags");
    check(bytes[26] == 0x10 && bytes[27] == 0x00, "RTPS octetsToInlineQos");

    DataMessage decoded;
    check(decode_data_message(bytes, decoded, error), "RTPS DATA decodes: " + error);
    check(decoded.header.guid_prefix == message.header.guid_prefix, "DATA GUID prefix");
    check(decoded.data.reader_id == message.data.reader_id, "DATA reader EntityId");
    check(decoded.data.writer_id == message.data.writer_id, "DATA writer EntityId");
    check(
        decoded.data.writer_sequence_number == message.data.writer_sequence_number,
        "DATA SequenceNumber");
    check(
        decoded.data.serialized_payload == message.data.serialized_payload,
        "DATA serialized payload");

    bytes[21] = 0x0d;
    check(
        !decode_data_message(bytes, decoded, error),
        "RTPS DATA rejects simultaneous data and key flags");
}

void test_rtps_reliability_messages()
{
    using namespace mini_dds;

    rtps::SequenceNumberSet sequence_set;
    sequence_set.bitmap_base = {5};
    sequence_set.num_bits = 34;
    sequence_set.bitmap = {0x80000001U, 0x80000000U};
    check(sequence_set.contains({5}), "SequenceNumberSet maps its first bit");
    check(sequence_set.contains({36}), "SequenceNumberSet uses MSB-first bitmap order");
    check(sequence_set.contains({37}), "SequenceNumberSet maps its second word");
    check(!sequence_set.contains({6}), "SequenceNumberSet reports a clear bit");

    const rtps::EntityId reader_id{{0x00, 0x00, 0x01, 0x04}};
    const rtps::EntityId writer_id{{0x00, 0x00, 0x02, 0x03}};
    std::vector<std::uint8_t> bytes;
    std::string error;

    rtps::HeartbeatSubmessage heartbeat;
    heartbeat.reader_id = reader_id;
    heartbeat.writer_id = writer_id;
    heartbeat.first_sequence_number = {5};
    heartbeat.last_sequence_number = {9};
    heartbeat.count = 7;
    heartbeat.final_flag = true;
    check(
        rtps::encode_heartbeat_submessage(
            heartbeat,
            serialization::Endianness::little,
            bytes,
            error),
        "HEARTBEAT encodes: " + error);
    check(bytes.size() == 32, "HEARTBEAT has the RTPS fixed wire size");
    check(
        bytes.size() >= 4 && bytes[0] == 0x07 && bytes[1] == 0x03 &&
            bytes[2] == 0x1c && bytes[3] == 0x00,
        "HEARTBEAT submessage header bytes");
    rtps::HeartbeatSubmessage decoded_heartbeat;
    check(
        rtps::decode_heartbeat_submessage(
            bytes.data(), bytes.size(), decoded_heartbeat, error),
        "HEARTBEAT decodes: " + error);
    check(
        decoded_heartbeat.reader_id == reader_id &&
            decoded_heartbeat.writer_id == writer_id &&
            decoded_heartbeat.first_sequence_number.value == 5 &&
            decoded_heartbeat.last_sequence_number.value == 9 &&
            decoded_heartbeat.count == 7 && decoded_heartbeat.final_flag,
        "HEARTBEAT fields round trip");

    rtps::AckNackSubmessage acknack;
    acknack.reader_id = reader_id;
    acknack.writer_id = writer_id;
    acknack.reader_state = sequence_set;
    acknack.count = 11;
    check(
        rtps::encode_acknack_submessage(
            acknack,
            serialization::Endianness::big,
            bytes,
            error),
        "ACKNACK encodes: " + error);
    rtps::AckNackSubmessage decoded_acknack;
    check(
        rtps::decode_acknack_submessage(
            bytes.data(), bytes.size(), decoded_acknack, error),
        "ACKNACK decodes: " + error);
    check(
        decoded_acknack.reader_state.bitmap == sequence_set.bitmap &&
            decoded_acknack.reader_state.num_bits == sequence_set.num_bits &&
            decoded_acknack.count == acknack.count,
        "ACKNACK fields round trip");

    rtps::GapSubmessage gap;
    gap.reader_id = reader_id;
    gap.writer_id = writer_id;
    gap.gap_start = {2};
    gap.gap_list.bitmap_base = {5};
    gap.gap_list.num_bits = 1;
    gap.gap_list.bitmap = {0x80000000U};
    check(
        rtps::encode_gap_submessage(
            gap,
            serialization::Endianness::little,
            bytes,
            error),
        "GAP encodes: " + error);
    rtps::GapSubmessage decoded_gap;
    check(
        rtps::decode_gap_submessage(
            bytes.data(), bytes.size(), decoded_gap, error),
        "GAP decodes: " + error);
    check(
        decoded_gap.gap_start.value == 2 &&
            decoded_gap.gap_list.bitmap_base.value == 5 &&
            decoded_gap.gap_list.contains({5}),
        "GAP fields round trip");

    auto invalid = acknack;
    invalid.reader_state.num_bits = 257;
    check(
        !rtps::encode_acknack_submessage(
            invalid,
            serialization::Endianness::little,
            bytes,
            error),
        "ACKNACK rejects a bitmap larger than 256 bits");
}

void test_history_cache()
{
    using namespace mini_dds;

    rtps::Guid writer_guid;
    writer_guid.entity_id.value = {0x00, 0x00, 0x02, 0x03};
    history::WriterHistory writer(writer_guid, 2);
    const auto first = writer.add({0x01});
    const auto second = writer.add({0x02});
    const auto third = writer.add({0x03});
    check(first.sequence_number.value == 1, "WriterHistory starts at sequence 1");
    check(second.sequence_number.value == 2, "WriterHistory increments sequence");
    check(writer.size() == 2, "WriterHistory enforces depth");
    check(!writer.find(first.sequence_number).has_value(), "WriterHistory evicts oldest");
    check(writer.find(third.sequence_number).has_value(), "WriterHistory finds retained change");

    history::ReaderHistory reader(2);
    check(reader.add(second), "ReaderHistory accepts a new change");
    check(!reader.add(second), "ReaderHistory rejects a queued duplicate");
    check(reader.add(third), "ReaderHistory accepts next sequence");
    check(reader.size() == 2, "ReaderHistory reports size");
    const auto taken = reader.take();
    check(
        taken.has_value() && taken->sequence_number == second.sequence_number,
        "ReaderHistory takes in arrival order");
}

void test_stateless_best_effort_path()
{
    using namespace mini_dds;

    transport::UdpTransport receiver_transport(0, "127.0.0.1");
    transport::UdpTransport sender_transport(0, "127.0.0.1");
    check(receiver_transport.is_open(), "Best-effort receiver transport opens");
    check(sender_transport.is_open(), "Best-effort sender transport opens");
    if (!receiver_transport.is_open() || !sender_transport.is_open()) {
        return;
    }

    rtps::GuidPrefix writer_prefix;
    writer_prefix.value = {
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
        0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b};
    const rtps::EntityId writer_id{{0x00, 0x00, 0x02, 0x03}};
    const rtps::EntityId reader_id{{0x00, 0x00, 0x01, 0x04}};

    rtps::StatelessWriter writer(
        sender_transport,
        writer_prefix,
        writer_id,
        {"127.0.0.1", receiver_transport.local_endpoint().port},
        reader_id,
        4);
    rtps::StatelessReader reader(receiver_transport, reader_id, 4);

    const auto payload = serialization::serialize_string_payload("structured sample");
    check(writer.write(payload), "StatelessWriter sends RTPS DATA: " + writer.last_error());

    const auto receive_result = reader.receive_once(std::chrono::seconds(1));
    check(
        receive_result.status == rtps::ReaderReceiveStatus::sample,
        "StatelessReader receives RTPS DATA: " + receive_result.error);

    const auto change = reader.take();
    check(change.has_value(), "StatelessReader stores CacheChange");
    if (change.has_value()) {
        std::string value;
        std::string error;
        check(
            serialization::deserialize_string_payload(
                change->serialized_payload,
                value,
                error) && value == "structured sample",
            "Best-effort path restores typed value: " + error);
    }
}

void test_stateless_multi_reader_fanout()
{
    using namespace mini_dds;

    auto link = std::make_shared<InMemoryTransportState>();
    InMemoryTransport writer_transport(link, 0);
    InMemoryTransport first_reader_transport(link, 1);
    InMemoryTransport second_reader_transport(link, 2);

    const rtps::EntityId writer_id{{0x00, 0x00, 0x20, 0x03}};
    const rtps::EntityId first_reader_id{{0x00, 0x00, 0x21, 0x04}};
    const rtps::EntityId second_reader_id{{0x00, 0x00, 0x22, 0x04}};
    rtps::StatelessWriter writer(
        writer_transport,
        {},
        writer_id,
        std::vector<rtps::ReaderProxy>{
            {first_reader_transport.local_endpoint(), first_reader_id},
            {second_reader_transport.local_endpoint(), second_reader_id}},
        4);
    rtps::StatelessReader first_reader(
        first_reader_transport,
        first_reader_id,
        4);
    rtps::StatelessReader second_reader(
        second_reader_transport,
        second_reader_id,
        4);

    const auto payload = serialization::serialize_string_payload("fanout sample");
    check(writer.write(payload), "StatelessWriter fans out one change: " + writer.last_error());
    check(writer.matched_reader_count() == 2, "StatelessWriter tracks two readers");
    check(
        first_reader.receive_once(std::chrono::seconds(1)).status ==
            rtps::ReaderReceiveStatus::sample,
        "first Best-Effort reader receives fan-out sample");
    check(
        second_reader.receive_once(std::chrono::seconds(1)).status ==
            rtps::ReaderReceiveStatus::sample,
        "second Best-Effort reader receives fan-out sample");
    check(
        first_reader.take().has_value() && second_reader.take().has_value(),
        "both Best-Effort reader histories contain the sample");
}

void test_reliable_retransmission_path()
{
    using namespace mini_dds;

    auto link = std::make_shared<InMemoryTransportState>();
    link->drop_first_writer_data = true;
    InMemoryTransport writer_transport(link, 0);
    InMemoryTransport reader_transport(link, 1);

    rtps::GuidPrefix writer_prefix;
    writer_prefix.value = {
        0x00, 0x00, 0x21, 0x22, 0x23, 0x24,
        0x25, 0x26, 0x27, 0x28, 0x29, 0x2a};
    rtps::GuidPrefix reader_prefix;
    reader_prefix.value = {
        0x00, 0x00, 0x31, 0x32, 0x33, 0x34,
        0x35, 0x36, 0x37, 0x38, 0x39, 0x3a};
    const rtps::EntityId writer_id{{0x00, 0x00, 0x02, 0x03}};
    const rtps::EntityId reader_id{{0x00, 0x00, 0x01, 0x04}};

    rtps::ReliableWriterConfig config;
    config.acknowledgment_timeout = std::chrono::milliseconds(100);
    config.max_retries = 3;
    config.history_depth = 4;
    rtps::ReliableWriter writer(
        writer_transport,
        writer_prefix,
        writer_id,
        reader_transport.local_endpoint(),
        reader_id,
        config);
    rtps::ReliableReader reader(
        reader_transport,
        reader_prefix,
        reader_id,
        4);

    rtps::ReaderReceiveResult reader_result;
    std::thread reader_thread([&] {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        do {
            reader_result = reader.receive_once(std::chrono::milliseconds(200));
        } while (reader_result.status != rtps::ReaderReceiveStatus::sample &&
                 reader_result.status != rtps::ReaderReceiveStatus::error &&
                 std::chrono::steady_clock::now() < deadline);
    });

    const auto payload = serialization::serialize_string_payload("recovered sample");
    const bool write_ok = writer.write(payload);
    reader_thread.join();
    check(write_ok, "ReliableWriter completes after retransmission: " + writer.last_error());
    check(link->dropped_data_count == 1, "test transport drops the first DATA");
    check(writer.retransmission_count() >= 1, "ReliableWriter retransmits a NACKed DATA");
    check(
        reader_result.status == rtps::ReaderReceiveStatus::sample,
        "ReliableReader recovers the missing sample: " + reader_result.error);

    const auto change = reader.take();
    std::string value;
    std::string error;
    check(
        change && serialization::deserialize_string_payload(
            change->serialized_payload,
            value,
            error) && value == "recovered sample",
        "reliable path restores the retransmitted payload: " + error);
}

void test_reliable_multi_reader_fanout()
{
    using namespace mini_dds;

    auto link = std::make_shared<InMemoryTransportState>();
    link->drop_first_writer_data = true;
    link->drop_destination_port = 31002;
    InMemoryTransport writer_transport(link, 0);
    InMemoryTransport first_reader_transport(link, 1);
    InMemoryTransport second_reader_transport(link, 2);

    const rtps::EntityId writer_id{{0x00, 0x00, 0x30, 0x03}};
    const rtps::EntityId first_reader_id{{0x00, 0x00, 0x31, 0x04}};
    const rtps::EntityId second_reader_id{{0x00, 0x00, 0x32, 0x04}};
    rtps::ReliableWriterConfig config;
    config.acknowledgment_timeout = std::chrono::milliseconds(100);
    config.max_retries = 3;
    config.history_depth = 4;
    rtps::ReliableWriter writer(
        writer_transport,
        {},
        writer_id,
        std::vector<rtps::ReaderProxy>{
            {first_reader_transport.local_endpoint(), first_reader_id},
            {second_reader_transport.local_endpoint(), second_reader_id}},
        config);
    rtps::ReliableReader first_reader(
        first_reader_transport,
        {},
        first_reader_id,
        4);
    rtps::ReliableReader second_reader(
        second_reader_transport,
        {},
        second_reader_id,
        4);

    rtps::ReaderReceiveResult first_result;
    rtps::ReaderReceiveResult second_result;
    const auto receive_sample = [](
                                    rtps::ReliableReader& reader,
                                    rtps::ReaderReceiveResult& result) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        do {
            result = reader.receive_once(std::chrono::milliseconds(200));
        } while (result.status != rtps::ReaderReceiveStatus::sample &&
                 result.status != rtps::ReaderReceiveStatus::error &&
                 std::chrono::steady_clock::now() < deadline);
    };
    std::thread first_thread(receive_sample, std::ref(first_reader), std::ref(first_result));
    std::thread second_thread(
        receive_sample,
        std::ref(second_reader),
        std::ref(second_result));

    const auto payload = serialization::serialize_string_payload("reliable fanout");
    const bool write_ok = writer.write(payload);
    first_thread.join();
    second_thread.join();

    check(write_ok, "ReliableWriter confirms all readers: " + writer.last_error());
    check(writer.matched_reader_count() == 2, "ReliableWriter tracks two readers");
    check(
        first_result.status == rtps::ReaderReceiveStatus::sample &&
            second_result.status == rtps::ReaderReceiveStatus::sample,
        "both ReliableReaders receive the fan-out sample");
    check(
        link->data_send_counts[1] == 1 && link->data_send_counts[2] == 2,
        "only the reader with a dropped DATA is retransmitted");
    check(
        writer.retransmission_count() == 1,
        "ReliableWriter counts the per-reader retransmission");
    check(
        first_reader.take().has_value() && second_reader.take().has_value(),
        "both ReliableReader histories contain the sample");
}

void test_reliable_reader_ordering_and_gap()
{
    using namespace mini_dds;

    const rtps::GuidPrefix writer_prefix{{
        0x00, 0x00, 0x41, 0x42, 0x43, 0x44,
        0x45, 0x46, 0x47, 0x48, 0x49, 0x4a}};
    const rtps::GuidPrefix reader_prefix{{
        0x00, 0x00, 0x51, 0x52, 0x53, 0x54,
        0x55, 0x56, 0x57, 0x58, 0x59, 0x5a}};
    const rtps::EntityId writer_id{{0x00, 0x00, 0x02, 0x03}};
    const rtps::EntityId reader_id{{0x00, 0x00, 0x01, 0x04}};

    const auto run_case = [&](bool close_gap) {
        auto link = std::make_shared<InMemoryTransportState>();
        InMemoryTransport sender_transport(link, 0);
        InMemoryTransport reader_transport(link, 1);
        rtps::ReliableReader reader(
            reader_transport,
            reader_prefix,
            reader_id,
            4);

        const auto send_data = [&](std::int64_t sequence, std::uint8_t payload) {
            rtps::DataMessage message;
            message.header = {{2, 3}, {}, writer_prefix};
            message.data.reader_id = reader_id;
            message.data.writer_id = writer_id;
            message.data.writer_sequence_number = {sequence};
            message.data.serialized_payload = {payload};
            std::vector<std::uint8_t> bytes;
            std::string error;
            return rtps::encode_data_message(
                       message,
                       serialization::Endianness::little,
                       bytes,
                       error) &&
                   sender_transport.send(bytes, reader_transport.local_endpoint());
        };

        check(send_data(2, 0x02), "test sends out-of-order DATA(2)");
        const auto second_first =
            reader.receive_once(std::chrono::milliseconds(100));
        check(
            second_first.status == rtps::ReaderReceiveStatus::ignored,
            "ReliableReader buffers DATA(2) while DATA(1) is missing");

        if (close_gap) {
            rtps::GapSubmessage gap;
            gap.reader_id = reader_id;
            gap.writer_id = writer_id;
            gap.gap_start = {1};
            gap.gap_list.bitmap_base = {2};
            rtps::MessageHeader header{{2, 3}, {}, writer_prefix};
            std::vector<std::uint8_t> bytes;
            std::string error;
            check(
                rtps::encode_gap_message(
                    header,
                    gap,
                    serialization::Endianness::little,
                    bytes,
                    error) &&
                    sender_transport.send(bytes, reader_transport.local_endpoint()),
                "test sends GAP(1): " + error);
        } else {
            check(send_data(1, 0x01), "test sends delayed DATA(1)");
        }

        const auto completed =
            reader.receive_once(std::chrono::milliseconds(100));
        check(
            completed.status == rtps::ReaderReceiveStatus::sample,
            close_gap
                ? "GAP releases the next contiguous DATA"
                : "delayed DATA releases samples in sequence order");

        const auto first = reader.take();
        check(
            first && first->sequence_number.value == (close_gap ? 2 : 1),
            close_gap
                ? "ReaderHistory omits the irrelevant GAP sequence"
                : "ReaderHistory exposes DATA(1) before DATA(2)");
        if (!close_gap) {
            const auto second = reader.take();
            check(
                second && second->sequence_number.value == 2,
                "ReaderHistory exposes buffered DATA(2) second");
        }
    };

    run_case(false);
    run_case(true);
}

void test_reliable_writer_timeout()
{
    using namespace mini_dds;

    auto link = std::make_shared<InMemoryTransportState>();
    InMemoryTransport writer_transport(link, 0);
    rtps::ReliableWriterConfig config;
    config.acknowledgment_timeout = std::chrono::milliseconds(20);
    config.max_retries = 1;
    rtps::ReliableWriter writer(
        writer_transport,
        {},
        {{0x00, 0x00, 0x02, 0x03}},
        {"memory", 31001},
        {{0x00, 0x00, 0x01, 0x04}},
        config);

    check(
        !writer.write({0x00, 0x01, 0x00, 0x00}),
        "ReliableWriter reports an acknowledgment timeout");
    check(
        writer.last_error().find("timed out") != std::string::npos &&
            writer.retransmission_count() == 1,
        "ReliableWriter bounds its retry count");
}

void test_dds_api_fixed_endpoint()
{
    using namespace mini_dds;

    dds::DomainParticipant subscriber_participant({7, 1, "127.0.0.1", 0});
    dds::DomainParticipant publisher_participant({7, 2, "127.0.0.1", 0});
    check(subscriber_participant.is_valid(), "DDS subscriber participant opens");
    check(publisher_participant.is_valid(), "DDS publisher participant opens");
    if (!subscriber_participant.is_valid() || !publisher_participant.is_valid()) {
        return;
    }

    const auto type_support = std::make_shared<dds::StringTypeSupport>();
    const auto writer_topic = publisher_participant.create_topic<std::string>(
        "Greeting",
        type_support);
    const auto reader_topic = subscriber_participant.create_topic<std::string>(
        "Greeting",
        type_support);

    auto subscriber = subscriber_participant.create_subscriber();
    auto reader = subscriber.create_datareader(reader_topic);

    auto publisher = publisher_participant.create_publisher();
    dds::DataWriterConfig writer_config;
    writer_config.remote_endpoint = {
        "127.0.0.1",
        subscriber_participant.local_endpoint().port};
    writer_config.remote_reader_id = reader->entity_id();
    auto writer = publisher.create_datawriter(writer_topic, writer_config);

    check(writer->topic_name() == "Greeting", "DataWriter retains Topic");
    check(reader->topic_name() == "Greeting", "DataReader retains Topic");
    check(writer->write("DDS API sample"), "DataWriter writes typed sample");

    std::string sample;
    const auto result = reader->take(sample, std::chrono::seconds(1));
    check(result.ok(), "DataReader takes typed sample: " + result.error);
    check(sample == "DDS API sample", "DDS API typed value round trip");

    std::string no_sample;
    const auto non_blocking = reader->take(no_sample, std::chrono::milliseconds(0));
    check(
        non_blocking.status == dds::TakeStatus::timeout,
        "DataReader supports a non-blocking take");

}

void test_dds_api_automatic_discovery()
{
    using namespace mini_dds;

    dds::ParticipantConfig subscriber_config;
    subscriber_config.domain_id = 92;
    subscriber_config.participant_id = 32;
    subscriber_config.bind_address = "0.0.0.0";
    subscriber_config.listen_port = 0;
    subscriber_config.enable_discovery = true;
    subscriber_config.advertised_address = "127.0.0.1";
    subscriber_config.participant_name = "dds-auto-subscriber";
    subscriber_config.announcement_period = std::chrono::milliseconds(100);
    subscriber_config.lease_duration = std::chrono::seconds(2);

    auto publisher_config = subscriber_config;
    publisher_config.participant_id = 33;
    publisher_config.participant_name = "dds-auto-publisher";

    dds::DomainParticipant subscriber_participant(subscriber_config);
    dds::DomainParticipant publisher_participant(publisher_config);
    check(
        subscriber_participant.is_valid(),
        "auto-discovery subscriber participant starts: " +
            subscriber_participant.last_error());
    check(
        publisher_participant.is_valid(),
        "auto-discovery publisher participant starts: " +
            publisher_participant.last_error());
    if (!subscriber_participant.is_valid() || !publisher_participant.is_valid()) {
        return;
    }

    const auto type_support = std::make_shared<dds::StringTypeSupport>();
    const auto reader_topic = subscriber_participant.create_topic<std::string>(
        "DiscoveredGreeting",
        type_support);
    const auto writer_topic = publisher_participant.create_topic<std::string>(
        "DiscoveredGreeting",
        type_support);

    auto subscriber = subscriber_participant.create_subscriber();
    dds::DataReaderConfig reader_config;
    reader_config.qos.reliability = dds::ReliabilityKind::reliable;
    reader_config.qos.history_depth = 4;
    auto reader = subscriber.create_datareader(reader_topic, reader_config);
    auto publisher = publisher_participant.create_publisher();
    dds::DataWriterConfig writer_config;
    writer_config.discovery_timeout = std::chrono::seconds(3);
    writer_config.qos.reliability = dds::ReliabilityKind::reliable;
    writer_config.qos.history_depth = 4;
    writer_config.qos.acknowledgment_timeout = std::chrono::milliseconds(200);
    writer_config.qos.max_retries = 5;
    auto writer = publisher.create_datawriter(writer_topic, writer_config);

    std::string sample;
    dds::TakeResult result;
    std::thread reader_thread([&] {
        result = reader->take(sample, std::chrono::seconds(5));
    });
    check(
        writer->write("automatically discovered sample"),
        "reliable DataWriter discovers and writes to a matching DataReader: " +
            writer->last_error());
    reader_thread.join();

    check(result.ok(), "discovered reliable DataReader receives sample: " + result.error);
    check(
        sample == "automatically discovered sample",
        "automatic-discovery typed value round trip");
    const auto discovery_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (publisher_participant.discovered_participant_count() == 0 &&
           std::chrono::steady_clock::now() < discovery_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    check(
        publisher_participant.discovered_participant_count() >= 1,
        "DomainParticipant exposes discovered participant count");
}

void test_dds_api_automatic_discovery_fanout()
{
    using namespace mini_dds;

    dds::ParticipantConfig first_config;
    first_config.domain_id = 93;
    first_config.participant_id = 40;
    first_config.bind_address = "0.0.0.0";
    first_config.enable_discovery = true;
    first_config.advertised_address = "127.0.0.1";
    first_config.participant_name = "fanout-subscriber-a";
    first_config.announcement_period = std::chrono::milliseconds(100);
    first_config.lease_duration = std::chrono::seconds(2);

    auto second_config = first_config;
    second_config.participant_id = 41;
    second_config.participant_name = "fanout-subscriber-b";
    auto publisher_config = first_config;
    publisher_config.participant_id = 42;
    publisher_config.participant_name = "fanout-publisher";

    dds::DomainParticipant first_participant(first_config);
    dds::DomainParticipant second_participant(second_config);
    dds::DomainParticipant publisher_participant(publisher_config);
    check(
        first_participant.is_valid() && second_participant.is_valid() &&
            publisher_participant.is_valid(),
        "three automatic-discovery participants start");
    if (!first_participant.is_valid() || !second_participant.is_valid() ||
        !publisher_participant.is_valid()) {
        return;
    }

    const auto type_support = std::make_shared<dds::StringTypeSupport>();
    const auto first_topic = first_participant.create_topic<std::string>(
        "FanoutGreeting",
        type_support);
    const auto second_topic = second_participant.create_topic<std::string>(
        "FanoutGreeting",
        type_support);
    const auto writer_topic = publisher_participant.create_topic<std::string>(
        "FanoutGreeting",
        type_support);

    dds::DataReaderConfig reader_config;
    reader_config.qos.reliability = dds::ReliabilityKind::reliable;
    reader_config.qos.history_depth = 4;
    auto first_reader = first_participant.create_subscriber().create_datareader(
        first_topic,
        reader_config);
    auto second_reader = second_participant.create_subscriber().create_datareader(
        second_topic,
        reader_config);

    dds::DataWriterConfig writer_config;
    writer_config.discovery_timeout = std::chrono::seconds(3);
    writer_config.qos.reliability = dds::ReliabilityKind::reliable;
    writer_config.qos.history_depth = 4;
    writer_config.qos.acknowledgment_timeout = std::chrono::milliseconds(200);
    writer_config.qos.max_retries = 5;
    auto writer = publisher_participant.create_publisher().create_datawriter(
        writer_topic,
        writer_config);

    const auto discovery_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(4);
    while (publisher_participant.discovered_endpoint_count() < 2 &&
           std::chrono::steady_clock::now() < discovery_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    check(
        publisher_participant.discovered_endpoint_count() >= 2,
        "publisher discovers both matching DataReaders");
    if (publisher_participant.discovered_endpoint_count() < 2) {
        return;
    }

    std::string first_sample;
    std::string second_sample;
    dds::TakeResult first_result;
    dds::TakeResult second_result;
    std::thread first_thread([&] {
        first_result = first_reader->take(first_sample, std::chrono::seconds(5));
    });
    std::thread second_thread([&] {
        second_result = second_reader->take(second_sample, std::chrono::seconds(5));
    });

    const bool write_ok = writer->write("automatic fanout sample");
    first_thread.join();
    second_thread.join();

    check(write_ok, "discovered DataWriter writes to all readers: " + writer->last_error());
    check(writer->matched_reader_count() == 2, "DataWriter exposes two matched readers");
    check(
        first_result.ok() && second_result.ok(),
        "both discovered DataReaders receive the reliable sample");
    check(
        first_sample == "automatic fanout sample" &&
            second_sample == "automatic fanout sample",
        "automatic-discovery fan-out preserves both typed samples");
}

void test_udp_loopback()
{
    using namespace mini_dds::transport;

    UdpTransport receiver(0, "127.0.0.1");
    UdpTransport sender(0, "127.0.0.1");
    check(receiver.is_open(), "UDP receiver opens: " + receiver.last_error());
    check(sender.is_open(), "UDP sender opens: " + sender.last_error());
    if (!receiver.is_open() || !sender.is_open()) {
        return;
    }

    const std::vector<std::uint8_t> payload{'m', 'i', 'n', 'i'};
    const Endpoint destination{"127.0.0.1", receiver.local_endpoint().port};
    check(sender.send(payload, destination), "UDP loopback send: " + sender.last_error());

    const auto result = receiver.receive(std::chrono::seconds(1));
    check(result.ok(), "UDP loopback receive: " + result.error);
    if (result.ok()) {
        check(result.datagram.payload == payload, "UDP loopback payload");
        check(result.datagram.source.address == "127.0.0.1", "UDP source address");
        check(
            result.datagram.source.port == sender.local_endpoint().port,
            "UDP source port");
    }

    const auto timeout = receiver.receive(std::chrono::milliseconds(10));
    check(timeout.status == ReceiveStatus::timeout, "UDP receive timeout is distinct");

    check(
        !sender.send(payload, Endpoint{"not-an-ip", 9000}),
        "UDP rejects invalid destination address");
    check(
        !sender.join_multicast_group("127.0.0.1"),
        "UDP rejects a non-multicast group");
}

void test_rtps_port_mapping()
{
    const mini_dds::discovery::PortMapping ports;
    check(ports.spdp_multicast_port(0) == 7400, "domain 0 SPDP multicast port");
    check(ports.spdp_unicast_port(0, 0) == 7410, "participant 0 SPDP unicast port");
    check(ports.user_multicast_port(0) == 7401, "domain 0 user multicast port");
    check(ports.user_unicast_port(0, 0) == 7411, "participant 0 user port");
    check(ports.spdp_multicast_port(1) == 7650, "domain gain is applied");
    check(ports.spdp_unicast_port(1, 3) == 7666, "participant gain is applied");
    check(!ports.spdp_multicast_port(1000).has_value(), "port overflow is rejected");
}

void test_parameter_list_codec()
{
    using namespace mini_dds;

    const discovery::ParameterList input{
        {discovery::pid_topic_name, {0xaa, 0xbb, 0xcc}},
        {0x1234, {0x10, 0x20, 0x30, 0x40}}};
    std::vector<std::uint8_t> payload;
    std::string error;
    check(
        discovery::encode_parameter_list(
            input,
            serialization::Endianness::little,
            payload,
            error),
        "ParameterList encodes: " + error);
    const std::vector<std::uint8_t> first_parameter{
        0x00, 0x03, 0x00, 0x00,
        0x05, 0x00, 0x04, 0x00,
        0xaa, 0xbb, 0xcc, 0x00};
    check(
        payload.size() >= first_parameter.size() &&
        std::equal(first_parameter.begin(), first_parameter.end(), payload.begin()),
        "ParameterList representation, header, and padding bytes");

    discovery::ParameterList decoded;
    serialization::Endianness endianness;
    check(
        discovery::decode_parameter_list(payload, decoded, endianness, error),
        "ParameterList decodes: " + error);
    check(endianness == serialization::Endianness::little, "ParameterList endianness");
    check(decoded.size() == 2, "ParameterList parameter count");
    const auto* topic = discovery::find_parameter(decoded, discovery::pid_topic_name);
    check(
        topic != nullptr &&
        topic->value == std::vector<std::uint8_t>({0xaa, 0xbb, 0xcc, 0x00}),
        "ParameterList retains padded parameter value");

    payload.resize(payload.size() - 4U);
    check(
        !discovery::decode_parameter_list(payload, decoded, endianness, error),
        "ParameterList requires PID_SENTINEL");
}

void test_discovery_data_codecs()
{
    using namespace mini_dds;

    rtps::GuidPrefix prefix;
    prefix.value = {
        0x00, 0x00, 0x12, 0x13, 0x14, 0x15,
        0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b};

    discovery::ParticipantDiscoveryData participant;
    participant.participant_guid = {prefix, discovery::entity_id_participant};
    participant.metatraffic_unicast_locator = {"127.0.0.1", 7412};
    participant.default_unicast_locator = {"127.0.0.1", 7413};
    participant.lease_duration = std::chrono::milliseconds(2500);
    participant.name = "participant-one";

    std::vector<std::uint8_t> payload;
    std::string error;
    check(
        discovery::encode_participant_discovery_data(
            participant,
            serialization::Endianness::little,
            payload,
            error),
        "participant discovery data encodes: " + error);
    check(
        payload.size() >= 4 && payload[0] == 0x00 && payload[1] == 0x03,
        "participant discovery uses PL_CDR_LE");

    discovery::ParticipantDiscoveryData decoded_participant;
    check(
        discovery::decode_participant_discovery_data(
            payload,
            decoded_participant,
            error),
        "participant discovery data decodes: " + error);
    check(
        decoded_participant.participant_guid == participant.participant_guid,
        "participant discovery GUID round trip");
    check(
        decoded_participant.metatraffic_unicast_locator ==
            participant.metatraffic_unicast_locator,
        "participant metatraffic locator round trip");
    check(
        decoded_participant.default_unicast_locator ==
            participant.default_unicast_locator,
        "participant default locator round trip");
    check(
        decoded_participant.lease_duration == participant.lease_duration,
        "participant lease duration round trip");
    check(decoded_participant.name == participant.name, "participant name round trip");

    discovery::EndpointDiscoveryData endpoint;
    endpoint.kind = discovery::DiscoveredEndpointKind::reader;
    endpoint.endpoint_guid = {
        prefix,
        rtps::EntityId{{0x00, 0x00, 0x01, 0x04}}};
    endpoint.participant_guid = participant.participant_guid;
    endpoint.topic_name = "Greeting";
    endpoint.type_name = "mini_dds::String";
    endpoint.unicast_locator = {"192.168.1.10", 7655};
    endpoint.reliable = true;

    check(
        discovery::encode_endpoint_discovery_data(
            endpoint,
            serialization::Endianness::big,
            payload,
            error),
        "endpoint discovery data encodes: " + error);
    discovery::EndpointDiscoveryData decoded_endpoint;
    check(
        discovery::decode_endpoint_discovery_data(
            payload,
            discovery::DiscoveredEndpointKind::reader,
            decoded_endpoint,
            error),
        "endpoint discovery data decodes: " + error);
    check(decoded_endpoint.endpoint_guid == endpoint.endpoint_guid, "endpoint GUID round trip");
    check(decoded_endpoint.participant_guid == endpoint.participant_guid, "endpoint participant GUID");
    check(decoded_endpoint.topic_name == endpoint.topic_name, "endpoint Topic round trip");
    check(decoded_endpoint.type_name == endpoint.type_name, "endpoint type round trip");
    check(
        decoded_endpoint.unicast_locator == endpoint.unicast_locator,
        "endpoint locator round trip");
    check(decoded_endpoint.reliable, "endpoint reliability round trip");
}

void test_spdp_sedp_discovery_service()
{
    using namespace mini_dds;

    transport::UdpTransport user_transport_a(0, "127.0.0.1");
    transport::UdpTransport user_transport_b(0, "127.0.0.1");
    check(user_transport_a.is_open(), "discovery user transport A opens");
    check(user_transport_b.is_open(), "discovery user transport B opens");
    if (!user_transport_a.is_open() || !user_transport_b.is_open()) {
        return;
    }

    rtps::GuidPrefix prefix_a;
    prefix_a.value = {
        0x00, 0x00, 0xa1, 0xa2, 0xa3, 0xa4,
        0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa};
    rtps::GuidPrefix prefix_b;
    prefix_b.value = {
        0x00, 0x00, 0xb1, 0xb2, 0xb3, 0xb4,
        0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba};

    discovery::DiscoveryConfig config_a;
    config_a.domain_id = 91;
    config_a.participant_id = 30;
    config_a.guid_prefix = prefix_a;
    config_a.user_unicast_locator = {
        "127.0.0.1",
        user_transport_a.local_endpoint().port};
    config_a.bind_address = "0.0.0.0";
    config_a.advertised_address = "127.0.0.1";
    config_a.participant_name = "discovery-a";
    config_a.announcement_period = std::chrono::milliseconds(100);
    config_a.lease_duration = std::chrono::milliseconds(500);

    auto config_b = config_a;
    config_b.participant_id = 31;
    config_b.guid_prefix = prefix_b;
    config_b.user_unicast_locator = {
        "127.0.0.1",
        user_transport_b.local_endpoint().port};
    config_b.participant_name = "discovery-b";

    discovery::DiscoveryService service_a(config_a);
    auto service_b = std::make_unique<discovery::DiscoveryService>(config_b);
    check(service_a.is_valid(), "SPDP service A starts: " + service_a.last_error());
    check(service_b->is_valid(), "SPDP service B starts: " + service_b->last_error());
    if (!service_a.is_valid() || !service_b->is_valid()) {
        return;
    }

    const rtps::EntityId writer_id{{0x00, 0x00, 0x11, 0x03}};
    const rtps::EntityId reader_id{{0x00, 0x00, 0x12, 0x04}};
    discovery::EndpointDiscoveryData writer;
    writer.kind = discovery::DiscoveredEndpointKind::writer;
    writer.endpoint_guid = {prefix_a, writer_id};
    writer.topic_name = "AutoGreeting";
    writer.type_name = "mini_dds::String";
    service_a.announce_local_endpoint(writer);

    discovery::EndpointDiscoveryData reader;
    reader.kind = discovery::DiscoveredEndpointKind::reader;
    reader.endpoint_guid = {prefix_b, reader_id};
    reader.topic_name = "AutoGreeting";
    reader.type_name = "mini_dds::String";
    service_b->announce_local_endpoint(reader);

    check(
        service_a.wait_for_participant(prefix_b, std::chrono::seconds(3)),
        "SPDP A discovers participant B: " + service_a.last_error());
    check(
        service_b->wait_for_participant(prefix_a, std::chrono::seconds(3)),
        "SPDP B discovers participant A: " + service_b->last_error());

    const auto matched_reader = service_a.wait_for_reader(
        "AutoGreeting",
        "mini_dds::String",
        false,
        std::chrono::seconds(3));
    check(matched_reader.has_value(), "SEDP writer finds matching reader");
    if (matched_reader) {
        check(matched_reader->endpoint_guid == reader.endpoint_guid, "SEDP reader GUID");
        check(
            matched_reader->unicast_locator == config_b.user_unicast_locator,
            "SEDP reader user locator");
    }
    check(
        !service_a.wait_for_reader(
             "AutoGreeting",
             "mini_dds::String",
             true,
             std::chrono::milliseconds(20))
             .has_value(),
        "SEDP does not match Reliable Writer to Best-Effort Reader");

    service_b.reset();
    const auto expiry_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (service_a.remote_participant_count() != 0 &&
           std::chrono::steady_clock::now() < expiry_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    check(
        service_a.remote_participant_count() == 0 &&
            service_a.remote_endpoint_count() == 0,
        "SPDP lease expiry removes participant and SEDP endpoints");
}

} // namespace

int main()
{
    test_cdr_little_endian_round_trip();
    test_cdr_big_endian_and_invalid_input();
    test_rtps_message_header();
    test_rtps_submessage_header();
    test_serialized_string_payload();
    test_rtps_data_message();
    test_rtps_reliability_messages();
    test_history_cache();
    test_udp_loopback();
    test_rtps_port_mapping();
    test_parameter_list_codec();
    test_discovery_data_codecs();
    test_spdp_sedp_discovery_service();
    test_stateless_best_effort_path();
    test_stateless_multi_reader_fanout();
    test_reliable_retransmission_path();
    test_reliable_multi_reader_fanout();
    test_reliable_reader_ordering_and_gap();
    test_reliable_writer_timeout();
    test_dds_api_fixed_endpoint();
    test_dds_api_automatic_discovery();
    test_dds_api_automatic_discovery_fanout();

    if (failures != 0) {
        std::cerr << failures << " test(s) failed\n";
        return 1;
    }

    std::cout << "all mini_dds tests passed\n";
    return 0;
}
