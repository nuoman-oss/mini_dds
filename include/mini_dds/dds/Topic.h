#pragma once

#include "mini_dds/dds/TypeSupport.h"

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

namespace mini_dds::dds {

template<typename T>
class Topic {
public:
    Topic(
        std::string name,
        std::shared_ptr<const TypeSupport<T>> type_support)
        : name_(std::move(name)),
          type_support_(std::move(type_support))
    {
        if (name_.empty()) {
            throw std::invalid_argument("topic name must not be empty");
        }
        if (!type_support_) {
            throw std::invalid_argument("topic type support must not be null");
        }
    }

    [[nodiscard]] const std::string& name() const noexcept
    {
        return name_;
    }

    [[nodiscard]] std::string type_name() const
    {
        return type_support_->type_name();
    }

    [[nodiscard]] const TypeSupport<T>& type_support() const noexcept
    {
        return *type_support_;
    }

private:
    std::string name_;
    std::shared_ptr<const TypeSupport<T>> type_support_;
};

} // namespace mini_dds::dds
