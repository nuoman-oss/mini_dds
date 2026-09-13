cmake_minimum_required(VERSION 3.10)


project(miniDDS)



set(CMAKE_CXX_STANDARD 17)



include_directories(include)



add_library(
    udp_transport

    src/UDPTransport.cpp
)



add_executable(
    sender

    examples/sender.cpp
)


target_link_libraries(
    sender

    udp_transport
)



add_executable(
    receiver

    examples/receiver.cpp
)


target_link_libraries(
    receiver

    udp_transport
)