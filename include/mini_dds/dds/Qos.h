#pragma once

#include <cstddef>

namespace mini_dds::dds {

enum class ReliabilityKind {
    best_effort,
    reliable,
};

struct EndpointQos {
    ReliabilityKind reliability{ReliabilityKind::best_effort};
    std::size_t history_depth{1};
};

} // namespace mini_dds::dds
