#include "mini_dds/rtps/ParticipantMessageRouter.h"

#include "mini_dds/rtps/Data.h"
#include "mini_dds/rtps/Message.h"
#include "mini_dds/rtps/Reliability.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace mini_dds::rtps {
namespace {

struct RouteTarget {
    RoutedEndpointKind kind{RoutedEndpointKind::reader};
    EntityId entity_id;
};

bool is_unknown_entity(const EntityId& entity_id)
{
    return entity_id == EntityId{};
}

std::optional<RouteTarget> route_target(
    const std::vector<std::uint8_t>& payload)
{
    MessageHeader message_header;
    if (!decode_message_header(payload, message_header) ||
        message_header.version.major != 2 ||
        payload.size() <= message_header_size) {
        return std::nullopt;
    }

    const auto* submessage = payload.data() + message_header_size;
    const auto submessage_size = payload.size() - message_header_size;
    SubmessageHeader submessage_header;
    if (!decode_submessage_header(
            submessage,
            submessage_size,
            submessage_header)) {
        return std::nullopt;
    }

    std::string error;
    switch (submessage_header.kind) {
    case SubmessageKind::data: {
        DataSubmessage data;
        if (!decode_data_submessage(
                submessage,
                submessage_size,
                data,
                error)) {
            return std::nullopt;
        }
        return RouteTarget{RoutedEndpointKind::reader, data.reader_id};
    }
    case SubmessageKind::heartbeat: {
        HeartbeatSubmessage heartbeat;
        if (!decode_heartbeat_submessage(
                submessage,
                submessage_size,
                heartbeat,
                error)) {
            return std::nullopt;
        }
        return RouteTarget{RoutedEndpointKind::reader, heartbeat.reader_id};
    }
    case SubmessageKind::gap: {
        GapSubmessage gap;
        if (!decode_gap_submessage(
                submessage,
                submessage_size,
                gap,
                error)) {
            return std::nullopt;
        }
        return RouteTarget{RoutedEndpointKind::reader, gap.reader_id};
    }
    case SubmessageKind::acknack: {
        AckNackSubmessage acknack;
        if (!decode_acknack_submessage(
                submessage,
                submessage_size,
                acknack,
                error)) {
            return std::nullopt;
        }
        return RouteTarget{RoutedEndpointKind::writer, acknack.writer_id};
    }
    default:
        return std::nullopt;
    }
}

} // namespace

struct ParticipantMessageRouter::Impl {
    struct Channel {
        EntityId entity_id;
        RoutedEndpointKind kind{RoutedEndpointKind::reader};
        std::mutex mutex;
        std::condition_variable ready;
        std::deque<transport::Datagram> datagrams;
        bool active{true};
        std::string error;
    };

    explicit Impl(std::shared_ptr<transport::ITransport> source)
        : transport(std::move(source))
    {
    }

    ~Impl()
    {
        stop();
    }

    void start()
    {
        receiver = std::thread([this] {
            receive_loop();
        });
    }

    void stop()
    {
        stopping.store(true);
        if (receiver.joinable() &&
            receiver.get_id() != std::this_thread::get_id()) {
            receiver.join();
        }

        std::vector<std::shared_ptr<Channel>> current_channels;
        {
            std::lock_guard<std::mutex> lock(channels_mutex);
            current_channels = channels;
            channels.clear();
        }
        for (const auto& channel : current_channels) {
            {
                std::lock_guard<std::mutex> lock(channel->mutex);
                channel->active = false;
            }
            channel->ready.notify_all();
        }
    }

    std::shared_ptr<Channel> add_channel(
        EntityId entity_id,
        RoutedEndpointKind kind)
    {
        std::shared_ptr<Channel> channel;
        std::string registration_error;
        {
            std::lock_guard<std::mutex> lock(channels_mutex);
            if (stopping.load() || failed.load()) {
                registration_error = "participant message router is not running";
            } else {
                const auto duplicate = std::find_if(
                    channels.begin(),
                    channels.end(),
                    [&](const std::shared_ptr<Channel>& current) {
                        return current->active && current->kind == kind &&
                               current->entity_id == entity_id;
                    });
                if (duplicate != channels.end()) {
                    registration_error =
                        "an endpoint with the same EntityId is already registered";
                } else {
                    channel = std::make_shared<Channel>();
                    channel->entity_id = entity_id;
                    channel->kind = kind;
                    channels.push_back(channel);
                }
            }
        }
        if (!registration_error.empty()) {
            set_last_error(std::move(registration_error));
        }
        return channel;
    }

    void remove_channel(const std::shared_ptr<Channel>& channel)
    {
        if (!channel) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(channel->mutex);
            channel->active = false;
            channel->datagrams.clear();
        }
        channel->ready.notify_all();

        std::lock_guard<std::mutex> lock(channels_mutex);
        channels.erase(
            std::remove(channels.begin(), channels.end(), channel),
            channels.end());
    }

    bool send(
        const std::vector<std::uint8_t>& data,
        const transport::Endpoint& destination,
        std::string& error)
    {
        std::lock_guard<std::mutex> lock(send_mutex);
        if (stopping.load() || failed.load()) {
            error = "participant message router is not running";
            return false;
        }
        if (!transport || !transport->is_open()) {
            error = "participant transport is not open";
            return false;
        }
        if (!transport->send(data, destination)) {
            error = transport->last_error();
            return false;
        }
        error.clear();
        return true;
    }

    bool is_open() const noexcept
    {
        return transport && transport->is_open() &&
               !stopping.load() && !failed.load();
    }

    transport::Endpoint local_endpoint() const
    {
        return transport ? transport->local_endpoint() : transport::Endpoint{};
    }

    std::size_t endpoint_count() const
    {
        std::lock_guard<std::mutex> lock(channels_mutex);
        return channels.size();
    }

    std::string last_error_copy() const
    {
        std::lock_guard<std::mutex> lock(error_mutex);
        return last_error;
    }

    void set_last_error(std::string error)
    {
        std::lock_guard<std::mutex> lock(error_mutex);
        last_error = std::move(error);
    }

    void receive_loop()
    {
        while (!stopping.load()) {
            const auto received = transport->receive(std::chrono::milliseconds(20));
            if (received.status == transport::ReceiveStatus::timeout) {
                continue;
            }
            if (!received.ok()) {
                if (!stopping.load()) {
                    fail(received.error.empty()
                        ? "participant transport receive failed"
                        : received.error);
                }
                return;
            }

            const auto target = route_target(received.datagram.payload);
            if (!target) {
                continue;
            }

            std::vector<std::shared_ptr<Channel>> targets;
            {
                std::lock_guard<std::mutex> lock(channels_mutex);
                for (const auto& channel : channels) {
                    if (channel->kind == target->kind &&
                        (is_unknown_entity(target->entity_id) ||
                         channel->entity_id == target->entity_id)) {
                        targets.push_back(channel);
                    }
                }
            }

            for (const auto& channel : targets) {
                {
                    std::lock_guard<std::mutex> lock(channel->mutex);
                    if (!channel->active) {
                        continue;
                    }
                    channel->datagrams.push_back(received.datagram);
                }
                channel->ready.notify_one();
            }
        }
    }

    void fail(std::string error)
    {
        failed.store(true);
        set_last_error(error);

        std::vector<std::shared_ptr<Channel>> current_channels;
        {
            std::lock_guard<std::mutex> lock(channels_mutex);
            current_channels = channels;
        }
        for (const auto& channel : current_channels) {
            {
                std::lock_guard<std::mutex> lock(channel->mutex);
                channel->error = error;
            }
            channel->ready.notify_all();
        }
    }

    std::shared_ptr<transport::ITransport> transport;
    mutable std::mutex channels_mutex;
    std::vector<std::shared_ptr<Channel>> channels;
    std::mutex send_mutex;
    mutable std::mutex error_mutex;
    std::string last_error;
    std::atomic<bool> stopping{false};
    std::atomic<bool> failed{false};
    std::thread receiver;
};

class ParticipantMessageRouter::RoutedTransport final
    : public transport::ITransport {
public:
    RoutedTransport(
        std::shared_ptr<Impl> impl,
        std::shared_ptr<Impl::Channel> channel)
        : impl_(std::move(impl)),
          channel_(std::move(channel))
    {
    }

    ~RoutedTransport() override
    {
        if (impl_) {
            impl_->remove_channel(channel_);
        }
    }

    bool send(
        const std::vector<std::uint8_t>& data,
        const transport::Endpoint& destination) override
    {
        if (!impl_) {
            last_error_ = "participant message router is unavailable";
            return false;
        }
        return impl_->send(data, destination, last_error_);
    }

    transport::ReceiveResult receive(std::chrono::milliseconds timeout) override
    {
        std::unique_lock<std::mutex> lock(channel_->mutex);
        const auto available = [this] {
            return !channel_->datagrams.empty() || !channel_->error.empty() ||
                   !channel_->active;
        };
        bool ready = true;
        if (timeout.count() < 0) {
            channel_->ready.wait(lock, available);
        } else {
            ready = channel_->ready.wait_for(lock, timeout, available);
        }
        if (!ready) {
            return {transport::ReceiveStatus::timeout, {}, {}};
        }
        if (!channel_->datagrams.empty()) {
            auto datagram = std::move(channel_->datagrams.front());
            channel_->datagrams.pop_front();
            return {
                transport::ReceiveStatus::ok,
                std::move(datagram),
                {}};
        }
        if (!channel_->error.empty()) {
            return {
                transport::ReceiveStatus::error,
                {},
                channel_->error};
        }
        return {
            transport::ReceiveStatus::error,
            {},
            "routed endpoint is closed"};
    }

    [[nodiscard]] bool is_open() const noexcept override
    {
        std::lock_guard<std::mutex> lock(channel_->mutex);
        return channel_->active && impl_ && impl_->is_open();
    }

    [[nodiscard]] transport::Endpoint local_endpoint() const override
    {
        return impl_ ? impl_->local_endpoint() : transport::Endpoint{};
    }

    [[nodiscard]] std::string last_error() const override
    {
        return last_error_;
    }

private:
    std::shared_ptr<Impl> impl_;
    std::shared_ptr<Impl::Channel> channel_;
    std::string last_error_;
};

ParticipantMessageRouter::ParticipantMessageRouter(
    std::shared_ptr<transport::ITransport> transport)
    : impl_(std::make_shared<Impl>(std::move(transport)))
{
    if (!impl_->transport || !impl_->transport->is_open()) {
        impl_->failed.store(true);
        impl_->set_last_error("participant transport is not open");
        return;
    }
    impl_->start();
}

ParticipantMessageRouter::~ParticipantMessageRouter()
{
    if (impl_) {
        impl_->stop();
    }
}

std::shared_ptr<transport::ITransport> ParticipantMessageRouter::create_endpoint(
    EntityId entity_id,
    RoutedEndpointKind kind)
{
    auto channel = impl_->add_channel(entity_id, kind);
    return channel
        ? std::static_pointer_cast<transport::ITransport>(
              std::make_shared<RoutedTransport>(impl_, std::move(channel)))
        : nullptr;
}

bool ParticipantMessageRouter::is_open() const noexcept
{
    return impl_ && impl_->is_open();
}

transport::Endpoint ParticipantMessageRouter::local_endpoint() const
{
    return impl_ ? impl_->local_endpoint() : transport::Endpoint{};
}

std::size_t ParticipantMessageRouter::endpoint_count() const
{
    return impl_ ? impl_->endpoint_count() : 0;
}

std::string ParticipantMessageRouter::last_error() const
{
    return impl_ ? impl_->last_error_copy() :
        "participant message router is unavailable";
}

} // namespace mini_dds::rtps
