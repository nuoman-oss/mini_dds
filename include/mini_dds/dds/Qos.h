#pragma once

#include <chrono>
#include <cstddef>

namespace mini_dds::dds {

enum class ReliabilityKind {
    best_effort,
    reliable,
};

enum class HistoryKind {
    keep_last,
};

enum class DurabilityKind {
    volatile_data,
};

struct EndpointQos {
    ReliabilityKind reliability{ReliabilityKind::best_effort};
    std::size_t history_depth{1};
    HistoryKind history{HistoryKind::keep_last};
    DurabilityKind durability{DurabilityKind::volatile_data};
    std::chrono::milliseconds acknowledgment_timeout{500};
    std::size_t max_retries{3};
};

} // namespace mini_dds::dds
