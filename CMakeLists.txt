cmake_minimum_required(VERSION 3.16)

project(mini_dds VERSION 0.1.0 LANGUAGES CXX)

find_package(Threads REQUIRED)

option(MINI_DDS_BUILD_EXAMPLES "Build mini_dds examples" ON)

add_library(mini_dds
    src/dds/DomainParticipant.cpp
    src/discovery/DiscoveryData.cpp
    src/discovery/DiscoveryService.cpp
    src/discovery/ParameterList.cpp
    src/discovery/PortMapping.cpp
    src/history/History.cpp
    src/rtps/Data.cpp
    src/rtps/Message.cpp
    src/rtps/StatelessEndpoint.cpp
    src/serialization/Cdr.cpp
    src/serialization/SerializedPayload.cpp
    src/transport/UDPTransport.cpp
)

add_library(mini_dds::mini_dds ALIAS mini_dds)
add_library(udp_transport ALIAS mini_dds)

target_include_directories(mini_dds
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

target_compile_features(mini_dds PUBLIC cxx_std_17)
target_link_libraries(mini_dds PRIVATE Threads::Threads)
set_target_properties(mini_dds PROPERTIES CXX_EXTENSIONS OFF)

if(MSVC)
    target_compile_options(mini_dds PRIVATE /W4 /permissive-)
else()
    target_compile_options(mini_dds PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(WIN32)
    target_link_libraries(mini_dds PRIVATE ws2_32)
endif()

if(MINI_DDS_BUILD_EXAMPLES)
    add_executable(sender examples/sender.cpp)
    target_link_libraries(sender PRIVATE mini_dds::mini_dds)

    add_executable(receiver examples/receiver.cpp)
    target_link_libraries(receiver PRIVATE mini_dds::mini_dds)

    add_executable(publisher examples/publisher.cpp)
    target_link_libraries(publisher PRIVATE mini_dds::mini_dds)

    add_executable(subscriber examples/subscriber.cpp)
    target_link_libraries(subscriber PRIVATE mini_dds::mini_dds)
endif()

include(CTest)

if(BUILD_TESTING)
    add_executable(mini_dds_tests tests/mini_dds_tests.cpp)
    target_link_libraries(mini_dds_tests PRIVATE mini_dds::mini_dds)
    add_test(NAME mini_dds_tests COMMAND mini_dds_tests)
endif()
