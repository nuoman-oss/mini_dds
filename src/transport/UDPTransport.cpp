#include "mini_dds/transport/UDPTransport.h"

#include <array>
#include <cerrno>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace mini_dds::transport {
namespace {

#ifdef _WIN32
using NativeSocket = SOCKET;
using SocketLength = int;
constexpr NativeSocket invalid_socket = INVALID_SOCKET;

int native_error_code()
{
    return WSAGetLastError();
}

void close_socket(NativeSocket socket)
{
    closesocket(socket);
}
#else
using NativeSocket = int;
using SocketLength = socklen_t;
constexpr NativeSocket invalid_socket = -1;

int native_error_code()
{
    return errno;
}

void close_socket(NativeSocket socket)
{
    close(socket);
}
#endif

std::string socket_error(const std::string& operation)
{
    return operation + " failed (native error " +
           std::to_string(native_error_code()) + ")";
}

constexpr std::size_t max_udp_payload_size = 65507;

} // namespace

class UdpTransport::Impl {
public:
    NativeSocket socket{invalid_socket};
    Endpoint local_endpoint;
    std::string last_error;

#ifdef _WIN32
    bool winsock_started{false};
#endif

    ~Impl()
    {
        if (socket != invalid_socket) {
            close_socket(socket);
        }

#ifdef _WIN32
        if (winsock_started) {
            WSACleanup();
        }
#endif
    }
};

UdpTransport::UdpTransport(
    std::uint16_t local_port,
    std::string bind_address)
    : impl_(std::make_unique<Impl>())
{
#ifdef _WIN32
    WSADATA winsock_data{};
    if (WSAStartup(MAKEWORD(2, 2), &winsock_data) != 0) {
        impl_->last_error = socket_error("WSAStartup");
        return;
    }
    impl_->winsock_started = true;
#endif

    impl_->socket = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (impl_->socket == invalid_socket) {
        impl_->last_error = socket_error("socket");
        return;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(local_port);

    if (bind_address.empty() || bind_address == "0.0.0.0") {
        address.sin_addr.s_addr = htonl(INADDR_ANY);
        bind_address = "0.0.0.0";
    } else if (::inet_pton(AF_INET, bind_address.c_str(), &address.sin_addr) != 1) {
        impl_->last_error = "invalid IPv4 bind address: " + bind_address;
        close_socket(impl_->socket);
        impl_->socket = invalid_socket;
        return;
    }

    if (::bind(
            impl_->socket,
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<SocketLength>(sizeof(address))) != 0) {
        impl_->last_error = socket_error("bind");
        close_socket(impl_->socket);
        impl_->socket = invalid_socket;
        return;
    }

    sockaddr_in bound_address{};
    SocketLength bound_address_size = static_cast<SocketLength>(sizeof(bound_address));
    if (::getsockname(
            impl_->socket,
            reinterpret_cast<sockaddr*>(&bound_address),
            &bound_address_size) != 0) {
        impl_->last_error = socket_error("getsockname");
        close_socket(impl_->socket);
        impl_->socket = invalid_socket;
        return;
    }

    impl_->local_endpoint = Endpoint{std::move(bind_address), ntohs(bound_address.sin_port)};
}

UdpTransport::~UdpTransport() = default;

bool UdpTransport::send(
    const std::vector<std::uint8_t>& data,
    const Endpoint& destination)
{
    impl_->last_error.clear();

    if (!is_open()) {
        impl_->last_error = "transport is not open";
        return false;
    }

    if (data.size() > max_udp_payload_size) {
        impl_->last_error = "UDP payload exceeds 65507 bytes";
        return false;
    }

    sockaddr_in target{};
    target.sin_family = AF_INET;
    target.sin_port = htons(destination.port);

    if (::inet_pton(AF_INET, destination.address.c_str(), &target.sin_addr) != 1) {
        impl_->last_error = "invalid IPv4 destination address: " + destination.address;
        return false;
    }

    const char* bytes = data.empty()
        ? nullptr
        : reinterpret_cast<const char*>(data.data());

#ifdef _WIN32
    const int sent = ::sendto(
        impl_->socket,
        bytes,
        static_cast<int>(data.size()),
        0,
        reinterpret_cast<const sockaddr*>(&target),
        static_cast<int>(sizeof(target)));
#else
    const ssize_t sent = ::sendto(
        impl_->socket,
        bytes,
        data.size(),
        0,
        reinterpret_cast<const sockaddr*>(&target),
        static_cast<SocketLength>(sizeof(target)));
#endif

    if (sent < 0) {
        impl_->last_error = socket_error("sendto");
        return false;
    }

    return static_cast<std::size_t>(sent) == data.size();
}

bool UdpTransport::send(
    const std::vector<std::uint8_t>& data,
    const std::string& address,
    std::uint16_t port)
{
    return send(data, Endpoint{address, port});
}

std::vector<std::uint8_t> UdpTransport::receive()
{
    auto result = receive(std::chrono::milliseconds(-1));
    return result.ok()
        ? std::move(result.datagram.payload)
        : std::vector<std::uint8_t>{};
}

ReceiveResult UdpTransport::receive(std::chrono::milliseconds timeout)
{
    impl_->last_error.clear();

    if (!is_open()) {
        impl_->last_error = "transport is not open";
        return ReceiveResult{ReceiveStatus::error, {}, impl_->last_error};
    }

    fd_set read_set;
    FD_ZERO(&read_set);
    FD_SET(impl_->socket, &read_set);

    timeval timeout_value{};
    timeval* timeout_pointer = nullptr;
    if (timeout.count() >= 0) {
        const auto total_milliseconds = timeout.count();
        timeout_value.tv_sec = static_cast<long>(total_milliseconds / 1000);
        timeout_value.tv_usec = static_cast<long>((total_milliseconds % 1000) * 1000);
        timeout_pointer = &timeout_value;
    }

#ifdef _WIN32
    const int ready = ::select(0, &read_set, nullptr, nullptr, timeout_pointer);
#else
    const int ready = ::select(
        impl_->socket + 1,
        &read_set,
        nullptr,
        nullptr,
        timeout_pointer);
#endif

    if (ready == 0) {
        return ReceiveResult{ReceiveStatus::timeout, {}, {}};
    }

    if (ready < 0) {
        impl_->last_error = socket_error("select");
        return ReceiveResult{ReceiveStatus::error, {}, impl_->last_error};
    }

    std::vector<std::uint8_t> payload(65535);
    sockaddr_in source{};
    SocketLength source_size = static_cast<SocketLength>(sizeof(source));

#ifdef _WIN32
    const int received = ::recvfrom(
        impl_->socket,
        reinterpret_cast<char*>(payload.data()),
        static_cast<int>(payload.size()),
        0,
        reinterpret_cast<sockaddr*>(&source),
        &source_size);
#else
    const ssize_t received = ::recvfrom(
        impl_->socket,
        payload.data(),
        payload.size(),
        0,
        reinterpret_cast<sockaddr*>(&source),
        &source_size);
#endif

    if (received < 0) {
        impl_->last_error = socket_error("recvfrom");
        return ReceiveResult{ReceiveStatus::error, {}, impl_->last_error};
    }

    payload.resize(static_cast<std::size_t>(received));

    std::array<char, INET_ADDRSTRLEN> source_address{};
    if (::inet_ntop(
            AF_INET,
            &source.sin_addr,
            source_address.data(),
            static_cast<SocketLength>(source_address.size())) == nullptr) {
        impl_->last_error = socket_error("inet_ntop");
        return ReceiveResult{ReceiveStatus::error, {}, impl_->last_error};
    }

    Datagram datagram{
        std::move(payload),
        Endpoint{source_address.data(), ntohs(source.sin_port)}};

    return ReceiveResult{ReceiveStatus::ok, std::move(datagram), {}};
}

bool UdpTransport::is_open() const noexcept
{
    return impl_ != nullptr && impl_->socket != invalid_socket;
}

Endpoint UdpTransport::local_endpoint() const
{
    return impl_->local_endpoint;
}

const std::string& UdpTransport::last_error() const noexcept
{
    return impl_->last_error;
}

} // namespace mini_dds::transport
