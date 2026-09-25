#pragma once

#include "mini_dds/rtps/Types.h"
#include "mini_dds/transport/ITransport.h"

#include <cstddef>
#include <memory>
#include <string>

namespace mini_dds::rtps {

enum class RoutedEndpointKind {
    reader,
    writer,
};

class ParticipantMessageRouter {
public:
    explicit ParticipantMessageRouter(
        std::shared_ptr<transport::ITransport> transport);
    ~ParticipantMessageRouter();

    ParticipantMessageRouter(const ParticipantMessageRouter&) = delete;
    ParticipantMessageRouter& operator=(const ParticipantMessageRouter&) = delete;
    ParticipantMessageRouter(ParticipantMessageRouter&&) = delete;
    ParticipantMessageRouter& operator=(ParticipantMessageRouter&&) = delete;

    std::shared_ptr<transport::ITransport> create_endpoint(
        EntityId entity_id,
        RoutedEndpointKind kind);

    [[nodiscard]] bool is_open() const noexcept;
    [[nodiscard]] transport::Endpoint local_endpoint() const;
    [[nodiscard]] std::size_t endpoint_count() const;
    [[nodiscard]] std::string last_error() const;

private:
    struct Impl;
    class RoutedTransport;

    std::shared_ptr<Impl> impl_;
};

} // namespace mini_dds::rtps
