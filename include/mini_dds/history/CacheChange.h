#pragma once

#include "mini_dds/rtps/Types.h"

#include <cstdint>
#include <vector>

namespace mini_dds::history {

enum class ChangeKind {
    alive,
    not_alive_disposed,
};

struct CacheChange {
    ChangeKind kind{ChangeKind::alive};
    rtps::Guid writer_guid;
    rtps::SequenceNumber sequence_number;
    std::vector<std::uint8_t> serialized_payload;
};

} // namespace mini_dds::history
