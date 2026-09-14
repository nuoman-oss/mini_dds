#include "mini_dds/dds/DomainParticipant.h"
#include "mini_dds/dds/TypeSupport.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <string>

int main()
{
    using namespace mini_dds::dds;

    ParticipantConfig participant_config;
    participant_config.domain_id = 0;
    participant_config.participant_id = 1;
    participant_config.enable_discovery = true;
    participant_config.advertised_address = "127.0.0.1";
    participant_config.participant_name = "mini_dds_subscriber";
    DomainParticipant participant(participant_config);
    if (!participant.is_valid()) {
        std::cerr << "participant creation failed: "
                  << participant.last_error() << '\n';
        return 1;
    }

    const auto type_support = std::make_shared<StringTypeSupport>();
    const auto topic = participant.create_topic<std::string>(
        "HelloWorld",
        type_support);
    const auto subscriber = participant.create_subscriber();
    DataReaderConfig reader_config;
    reader_config.qos.reliability = ReliabilityKind::reliable;
    reader_config.qos.history_depth = 8;
    auto reader = subscriber.create_datareader(topic, reader_config);

    std::cout << "waiting for Topic '" << topic.name()
              << "' with automatic discovery\n";
    for (int index = 0; index < 5; ++index) {
        std::string sample;
        const auto result = reader->take(sample, std::chrono::seconds(10));
        if (result.status == TakeStatus::timeout) {
            std::cerr << "timed out waiting for a sample\n";
            return 1;
        }
        if (!result.ok()) {
            std::cerr << "take failed: " << result.error << '\n';
            return 1;
        }

        std::cout << "received: " << sample << '\n';
    }

    return 0;
}
