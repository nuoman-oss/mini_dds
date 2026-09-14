#include "mini_dds/transport/UDPTransport.h"

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

int main()
{
    mini_dds::transport::UdpTransport transport(8000);
    if (!transport.is_open()) {
        std::cerr << "failed to open sender: " << transport.last_error() << '\n';
        return 1;
    }

    const std::string message = "Hello mini_dds";
    const std::vector<std::uint8_t> data(message.begin(), message.end());

    if (!transport.send(data, {"127.0.0.1", 9000})) {
        std::cerr << "send failed: " << transport.last_error() << '\n';
        return 1;
    }

    std::cout << "sent " << data.size() << " bytes to 127.0.0.1:9000\n";
    return 0;
}
