#include "mini_dds/dds/DomainParticipant.h"

#include <cstdint>
#include <random>
#include <utility>

namespace mini_dds::dds {
namespace {

rtps::GuidPrefix make_guid_prefix()
{
    rtps::GuidPrefix prefix;
    std::random_device random;
    // The unassigned experimental VendorId is {0x00, 0x00}; RTPS requires
    // the first two GuidPrefix bytes to match the VendorId.
    prefix.value[0] = 0;
    prefix.value[1] = 0;
    for (std::size_t offset = 2; offset < prefix.value.size(); ++offset) {
        prefix.value[offset] = static_cast<std::uint8_t>(random());
    }
    return prefix;
}

rtps::EntityId make_user_entity_id(std::uint32_t key, std::uint8_t kind)
{
    key &= 0x00ffffffU;
    if (key == 0) {
        key = 1;
    }
    return rtps::EntityId{{
        static_cast<std::uint8_t>(key >> 16U),
        static_cast<std::uint8_t>(key >> 8U),
        static_cast<std::uint8_t>(key),
        kind}};
}

} // namespace

rtps::EntityId detail::ParticipantState::allocate_writer_id()
{
    return make_user_entity_id(next_writer_key.fetch_add(1), 0x03);
}

rtps::EntityId detail::ParticipantState::allocate_reader_id()
{
    return make_user_entity_id(next_reader_key.fetch_add(1), 0x04);
}

Publisher::Publisher(std::shared_ptr<detail::ParticipantState> state)
    : state_(std::move(state))
{
}

Subscriber::Subscriber(std::shared_ptr<detail::ParticipantState> state)
    : state_(std::move(state))
{
}

DomainParticipant::DomainParticipant(ParticipantConfig config)
    : state_(std::make_shared<detail::ParticipantState>())
{
    state_->config = std::move(config);
    state_->guid_prefix = make_guid_prefix();
    state_->transport = std::make_shared<transport::UdpTransport>(
        state_->config.listen_port,
        state_->config.bind_address);

    if (!state_->transport->is_open()) {
        last_error_ = state_->transport->last_error();
        return;
    }

    if (state_->config.enable_discovery) {
        discovery::DiscoveryConfig discovery_config;
        discovery_config.domain_id = state_->config.domain_id;
        discovery_config.participant_id = state_->config.participant_id;
        discovery_config.guid_prefix = state_->guid_prefix;
        discovery_config.user_unicast_locator = {
            state_->config.advertised_address,
            state_->transport->local_endpoint().port};
        discovery_config.bind_address = state_->config.bind_address;
        discovery_config.advertised_address = state_->config.advertised_address;
        discovery_config.participant_name = state_->config.participant_name;
        discovery_config.announcement_period = state_->config.announcement_period;
        discovery_config.lease_duration = state_->config.lease_duration;
        state_->discovery = std::make_shared<discovery::DiscoveryService>(
            std::move(discovery_config));
        if (!state_->discovery->is_valid()) {
            last_error_ = state_->discovery->last_error();
        }
    }
}

bool DomainParticipant::is_valid() const noexcept
{
    return state_ != nullptr && state_->transport != nullptr &&
           state_->transport->is_open() &&
           (!state_->config.enable_discovery ||
            (state_->discovery && state_->discovery->is_valid()));
}

const std::string& DomainParticipant::last_error() const noexcept
{
    return last_error_;
}

std::uint32_t DomainParticipant::domain_id() const noexcept
{
    return state_->config.domain_id;
}

rtps::GuidPrefix DomainParticipant::guid_prefix() const noexcept
{
    return state_->guid_prefix;
}

transport::Endpoint DomainParticipant::local_endpoint() const
{
    return state_->transport->local_endpoint();
}

bool DomainParticipant::discovery_enabled() const noexcept
{
    return state_->discovery && state_->discovery->is_valid();
}

std::size_t DomainParticipant::discovered_participant_count() const
{
    return state_->discovery
        ? state_->discovery->remote_participant_count()
        : 0;
}

std::size_t DomainParticipant::discovered_endpoint_count() const
{
    return state_->discovery
        ? state_->discovery->remote_endpoint_count()
        : 0;
}

Publisher DomainParticipant::create_publisher() const
{
    return Publisher(state_);
}

Subscriber DomainParticipant::create_subscriber() const
{
    return Subscriber(state_);
}

} // namespace mini_dds::dds
