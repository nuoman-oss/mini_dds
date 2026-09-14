#include "mini_dds/transport/UDPTransport.h"

#include <chrono>
#include <iostream>
#include <string>

int main()
{
    using mini_dds::transport::ReceiveStatus;

    mini_dds::transport::UdpTransport transport(9000);
    if (!transport.is_open()) {
        std::cerr << "failed to open receiver: " << transport.last_error() << '\n';
        return 1;
    }

    std::cout << "waiting on 0.0.0.0:9000\n";

    while (true) {
        auto result = transport.receive(std::chrono::seconds(1));
        if (result.status == ReceiveStatus::timeout) {
            continue;
        }

        if (!result.ok()) {
            std::cerr << "receive failed: " << result.error << '\n';
            return 1;
        }

        const std::string message(
            result.datagram.payload.begin(),
            result.datagram.payload.end());

        std::cout << "received from "
                  << result.datagram.source.address << ':'
                  << result.datagram.source.port << ": "
                  << message << '\n';
    }
}
