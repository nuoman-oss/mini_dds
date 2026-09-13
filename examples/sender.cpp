#include "UDPTransport.h"

#include <iostream>


int main()
{

    UDPTransport udp(8000);



    std::string msg =
        "Hello mini DDS";


    std::vector<uint8_t> data(
        msg.begin(),
        msg.end()
    );



    udp.send(
        data,
        "127.0.0.1",
        9000
    );


}