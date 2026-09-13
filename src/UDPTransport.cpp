#include "UDPTransport.h"

#include <iostream>

#include <cstring>

#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>


UDPTransport::UDPTransport(uint16_t port)
    :
    local_port(port)
{

    /*
        创建 UDP socket

        AF_INET:
            IPv4

        SOCK_DGRAM:
            UDP

        0:
            默认协议
    */

    socket_fd = socket(
        AF_INET,
        SOCK_DGRAM,
        0
    );


    if(socket_fd < 0)
    {
        std::cerr
            << "create socket failed\n";

        return;
    }



    /*
        绑定本地端口

        receiver:

        IP:
            0.0.0.0

        Port:
            local_port

    */


    sockaddr_in addr{};


    addr.sin_family = AF_INET;


    addr.sin_addr.s_addr =
        INADDR_ANY;


    addr.sin_port =
        htons(local_port);



    int ret = bind(
        socket_fd,
        (sockaddr*)&addr,
        sizeof(addr)
    );


    if(ret < 0)
    {
        std::cerr
            << "bind failed\n";


        close(socket_fd);
        socket_fd = -1;
    }

}



UDPTransport::~UDPTransport()
{

    if(socket_fd >= 0)
    {
        close(socket_fd);
    }

}




bool UDPTransport::send(
    const std::vector<uint8_t>& data,
    const std::string& ip,
    uint16_t port
)
{

    if(socket_fd < 0)
        return false;



    sockaddr_in target{};


    target.sin_family =
        AF_INET;


    target.sin_port =
        htons(port);



    /*
        字符串IP

        例如:

        "192.168.1.10"

        转换为二进制
    */


    inet_pton(
        AF_INET,
        ip.c_str(),
        &target.sin_addr
    );



    ssize_t size = sendto(
        socket_fd,

        data.data(),

        data.size(),

        0,

        (sockaddr*)&target,

        sizeof(target)

    );



    return size ==
        static_cast<ssize_t>(data.size());

}






std::vector<uint8_t> UDPTransport::receive()
{

    std::vector<uint8_t> buffer(
        4096
    );


    sockaddr_in sender{};


    socklen_t len =
        sizeof(sender);



    ssize_t size =
        recvfrom(
            socket_fd,

            buffer.data(),

            buffer.size(),

            0,

            (sockaddr*)&sender,

            &len
        );



    if(size <=0)
    {
        return {};
    }



    buffer.resize(size);


    return buffer;

}