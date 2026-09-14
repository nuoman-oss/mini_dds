#include "mini_dds/dds/DomainParticipant.h"
#include "mini_dds/dds/TypeSupport.h"
#include "mini_dds/history/History.h"
#include "mini_dds/rtps/Data.h"
#include "mini_dds/rtps/Message.h"
#include "mini_dds/rtps/StatelessEndpoint.h"
#include "mini_dds/serialization/Cdr.h"
#include "mini_dds/serialization/SerializedPayload.h"
#include "mini_dds/transport/UDPTransport.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
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

    dds::DataWriterConfig reliable_config;
    reliable_config.remote_endpoint = writer_config.remote_endpoint;
    reliable_config.qos.reliability = dds::ReliabilityKind::reliable;
    auto reliable_writer = publisher.create_datawriter(writer_topic, reliable_config);
    check(
        !reliable_writer->write("unsupported"),
        "unsupported reliable QoS fails explicitly");
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
    test_history_cache();
    test_udp_loopback();
    test_stateless_best_effort_path();
    test_dds_api_fixed_endpoint();

    if (failures != 0) {
        std::cerr << failures << " test(s) failed\n";
        return 1;
    }

    std::cout << "all mini_dds tests passed\n";
    return 0;
}
