cmake_minimum_required(VERSION 3.20)


project(MiniDDS)


set(CMAKE_CXX_STANDARD 17)


include_directories(include)


add_executable(
    publisher
    examples/publisher.cpp
)


add_executable(
    subscriber
    examples/subscriber.cpp
)