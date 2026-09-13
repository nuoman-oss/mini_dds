#pragma once

#include <vector>
#include <string>
#include <cstdint>


class UDPTransport
{

public:

    UDPTransport(uint16_t port);

    ~UDPTransport();


    bool send(
        const std::vector<uint8_t>& data,
        const std::string& ip,
        uint16_t port
    );


    std::vector<uint8_t> receive();


private:

    int socket_fd;

    uint16_t local_port;

};