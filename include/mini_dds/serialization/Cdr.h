#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace mini_dds::serialization {

enum class Endianness {
    big,
    little,
};

class CdrWriter {
public:
    explicit CdrWriter(Endianness endianness = Endianness::little);

    void write_uint8(std::uint8_t value);
    void write_uint16(std::uint16_t value);
    void write_uint32(std::uint32_t value);
    void write_uint64(std::uint64_t value);
    void write_int32(std::int32_t value);
    void write_bytes(const std::vector<std::uint8_t>& value);
    void write_string(const std::string& value);

    [[nodiscard]] Endianness endianness() const noexcept;
    [[nodiscard]] const std::vector<std::uint8_t>& data() const noexcept;
    [[nodiscard]] std::vector<std::uint8_t> take_data();

private:
    void align(std::size_t alignment);
    void write_unsigned(std::uint64_t value, std::size_t byte_count);

    Endianness endianness_;
    std::vector<std::uint8_t> data_;
};

class CdrReader {
public:
    CdrReader(
        const std::uint8_t* data,
        std::size_t size,
        Endianness endianness = Endianness::little);

    explicit CdrReader(
        const std::vector<std::uint8_t>& data,
        Endianness endianness = Endianness::little);

    bool read_uint8(std::uint8_t& value);
    bool read_uint16(std::uint16_t& value);
    bool read_uint32(std::uint32_t& value);
    bool read_uint64(std::uint64_t& value);
    bool read_int32(std::int32_t& value);
    bool read_bytes(std::size_t count, std::vector<std::uint8_t>& value);
    bool read_string(std::string& value);

    [[nodiscard]] bool valid() const noexcept;
    [[nodiscard]] std::size_t remaining() const noexcept;

private:
    bool align(std::size_t alignment);
    bool read_unsigned(std::uint64_t& value, std::size_t byte_count);
    bool require(std::size_t count);

    const std::uint8_t* data_;
    std::size_t size_;
    std::size_t offset_{0};
    Endianness endianness_;
    bool valid_{true};
};

} // namespace mini_dds::serialization
