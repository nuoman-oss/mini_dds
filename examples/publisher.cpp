#include "mini_dds/dds/DomainParticipant.h"
#include "mini_dds/dds/TypeSupport.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

int main()
{
    using namespace mini_dds::dds;

    DomainParticipant participant({0, 0, "0.0.0.0", 8000});
    if (!participant.is_valid()) {
        std::cerr << "participant creation failed: "
                  << participant.last_error() << '\n';
        return 1;
    }

    const auto type_support = std::make_shared<StringTypeSupport>();
    const auto topic = participant.create_topic<std::string>(
        "HelloWorld",
        type_support);
    const auto publisher = participant.create_publisher();

    DataWriterConfig writer_config;
    writer_config.remote_endpoint = {"127.0.0.1", 9000};
    auto writer = publisher.create_datawriter(topic, writer_config);

    for (int index = 1; index <= 5; ++index) {
        const std::string sample = "Hello mini_dds #" + std::to_string(index);
        if (!writer->write(sample)) {
            std::cerr << "write failed: " << writer->last_error() << '\n';
            return 1;
        }

        std::cout << "published: " << sample << '\n';
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    return 0;
}
