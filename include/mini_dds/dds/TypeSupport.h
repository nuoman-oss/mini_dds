#pragma once

#include "mini_dds/serialization/SerializedPayload.h"

#include <cstdint>
#include <exception>
#include <string>
#include <vector>

namespace mini_dds::dds {

template<typename T>
class TypeSupport {
public:
    virtual ~TypeSupport() = default;

    [[nodiscard]] virtual std::string type_name() const = 0;

    virtual bool serialize(
        const T& value,
        std::vector<std::uint8_t>& payload,
        std::string& error) const = 0;

    virtual bool deserialize(
        const std::vector<std::uint8_t>& payload,
        T& value,
        std::string& error) const = 0;
};

class StringTypeSupport final : public TypeSupport<std::string> {
public:
    [[nodiscard]] std::string type_name() const override
    {
        return "mini_dds::String";
    }

    bool serialize(
        const std::string& value,
        std::vector<std::uint8_t>& payload,
        std::string& error) const override
    {
        error.clear();
        try {
            payload = serialization::serialize_string_payload(value);
            return true;
        } catch (const std::exception& exception) {
            error = exception.what();
            return false;
        }
    }

    bool deserialize(
        const std::vector<std::uint8_t>& payload,
        std::string& value,
        std::string& error) const override
    {
        return serialization::deserialize_string_payload(payload, value, error);
    }
};

} // namespace mini_dds::dds
