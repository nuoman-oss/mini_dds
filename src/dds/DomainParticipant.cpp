#include "mini_dds/dds/DomainParticipant.h"

#include <cstdint>
#include <random>
#include <utility>

namespace mini_dds::dds {
namespace {

void write_uint32_big_endian(
    std::uint32_t value,
    std::uint8_t* destination)
{
    destination[0] = static_cast<std::uint8_t>(value >> 24U);
    destination[1] = static_cast<std::uint8_t>(value >> 16U);
    destination[2] = static_cast<std::uint8_t>(value >> 8U);
    destination[3] = static_cast<std::uint8_t>(value);
}

rtps::GuidPrefix make_guid_prefix()
{
    rtps::GuidPrefix prefix;
    std::random_device random;
    for (std::size_t offset = 0; offset < prefix.value.size(); offset += 4) {
        const std::uint32_t random_word =
            (static_cast<std::uint32_t>(random()) << 16U) ^
            static_cast<std::uint32_t>(random());
        write_uint32_big_endian(random_word, prefix.value.data() + offset);
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
    }
}

bool DomainParticipant::is_valid() const noexcept
{
    return state_ != nullptr && state_->transport != nullptr &&
           state_->transport->is_open();
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

Publisher DomainParticipant::create_publisher() const
{
    return Publisher(state_);
}

Subscriber DomainParticipant::create_subscriber() const
{
    return Subscriber(state_);
}

} // namespace mini_dds::dds
