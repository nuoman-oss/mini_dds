#include "UDPTransport.h"

#include <iostream>


int main()
{

    UDPTransport udp(9000);



    while(true)
    {

        auto data =
            udp.receive();



        if(!data.empty())
        {

            std::string msg(
                data.begin(),
                data.end()
            );


            std::cout
                << "receive: "
                << msg
                << std::endl;
        }

    }


}