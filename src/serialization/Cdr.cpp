#include "mini_dds/serialization/Cdr.h"

#include <limits>
#include <stdexcept>
#include <utility>

namespace mini_dds::serialization {

CdrWriter::CdrWriter(Endianness endianness)
    : endianness_(endianness)
{
}

void CdrWriter::align(std::size_t alignment)
{
    const std::size_t padding = (alignment - (data_.size() % alignment)) % alignment;
    data_.insert(data_.end(), padding, 0);
}

void CdrWriter::write_unsigned(std::uint64_t value, std::size_t byte_count)
{
    align(byte_count);

    if (endianness_ == Endianness::little) {
        for (std::size_t index = 0; index < byte_count; ++index) {
            data_.push_back(static_cast<std::uint8_t>(value >> (index * 8U)));
        }
    } else {
        for (std::size_t index = 0; index < byte_count; ++index) {
            const std::size_t shift = (byte_count - index - 1U) * 8U;
            data_.push_back(static_cast<std::uint8_t>(value >> shift));
        }
    }
}

void CdrWriter::write_uint8(std::uint8_t value)
{
    data_.push_back(value);
}

void CdrWriter::write_uint16(std::uint16_t value)
{
    write_unsigned(value, sizeof(value));
}

void CdrWriter::write_uint32(std::uint32_t value)
{
    write_unsigned(value, sizeof(value));
}

void CdrWriter::write_uint64(std::uint64_t value)
{
    write_unsigned(value, sizeof(value));
}

void CdrWriter::write_int32(std::int32_t value)
{
    write_uint32(static_cast<std::uint32_t>(value));
}

void CdrWriter::write_bytes(const std::vector<std::uint8_t>& value)
{
    data_.insert(data_.end(), value.begin(), value.end());
}

void CdrWriter::write_string(const std::string& value)
{
    if (value.size() >= std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("CDR string is too large");
    }

    write_uint32(static_cast<std::uint32_t>(value.size() + 1U));
    data_.insert(data_.end(), value.begin(), value.end());
    data_.push_back(0);
}

Endianness CdrWriter::endianness() const noexcept
{
    return endianness_;
}

const std::vector<std::uint8_t>& CdrWriter::data() const noexcept
{
    return data_;
}

std::vector<std::uint8_t> CdrWriter::take_data()
{
    return std::move(data_);
}

CdrReader::CdrReader(
    const std::uint8_t* data,
    std::size_t size,
    Endianness endianness)
    : data_(data),
      size_(size),
      endianness_(endianness)
{
    if (data_ == nullptr && size_ != 0) {
        valid_ = false;
    }
}

CdrReader::CdrReader(
    const std::vector<std::uint8_t>& data,
    Endianness endianness)
    : CdrReader(data.data(), data.size(), endianness)
{
}

bool CdrReader::require(std::size_t count)
{
    if (!valid_ || count > size_ - offset_) {
        valid_ = false;
        return false;
    }
    return true;
}

bool CdrReader::align(std::size_t alignment)
{
    const std::size_t padding = (alignment - (offset_ % alignment)) % alignment;
    if (!require(padding)) {
        return false;
    }
    offset_ += padding;
    return true;
}

bool CdrReader::read_unsigned(std::uint64_t& value, std::size_t byte_count)
{
    if (!align(byte_count) || !require(byte_count)) {
        return false;
    }

    value = 0;
    if (endianness_ == Endianness::little) {
        for (std::size_t index = 0; index < byte_count; ++index) {
            value |= static_cast<std::uint64_t>(data_[offset_ + index]) << (index * 8U);
        }
    } else {
        for (std::size_t index = 0; index < byte_count; ++index) {
            value = (value << 8U) | data_[offset_ + index];
        }
    }

    offset_ += byte_count;
    return true;
}

bool CdrReader::read_uint8(std::uint8_t& value)
{
    if (!require(1)) {
        return false;
    }
    value = data_[offset_++];
    return true;
}

bool CdrReader::read_uint16(std::uint16_t& value)
{
    std::uint64_t decoded = 0;
    if (!read_unsigned(decoded, sizeof(value))) {
        return false;
    }
    value = static_cast<std::uint16_t>(decoded);
    return true;
}

bool CdrReader::read_uint32(std::uint32_t& value)
{
    std::uint64_t decoded = 0;
    if (!read_unsigned(decoded, sizeof(value))) {
        return false;
    }
    value = static_cast<std::uint32_t>(decoded);
    return true;
}

bool CdrReader::read_uint64(std::uint64_t& value)
{
    return read_unsigned(value, sizeof(value));
}

bool CdrReader::read_int32(std::int32_t& value)
{
    std::uint32_t decoded = 0;
    if (!read_uint32(decoded)) {
        return false;
    }
    value = static_cast<std::int32_t>(decoded);
    return true;
}

bool CdrReader::read_bytes(
    std::size_t count,
    std::vector<std::uint8_t>& value)
{
    if (!require(count)) {
        return false;
    }

    value.assign(data_ + offset_, data_ + offset_ + count);
    offset_ += count;
    return true;
}

bool CdrReader::read_string(std::string& value)
{
    std::uint32_t encoded_length = 0;
    if (!read_uint32(encoded_length) || encoded_length == 0 || !require(encoded_length)) {
        valid_ = false;
        return false;
    }

    if (data_[offset_ + encoded_length - 1U] != 0) {
        valid_ = false;
        return false;
    }

    value.assign(
        reinterpret_cast<const char*>(data_ + offset_),
        encoded_length - 1U);
    offset_ += encoded_length;
    return true;
}

bool CdrReader::valid() const noexcept
{
    return valid_;
}

std::size_t CdrReader::remaining() const noexcept
{
    return valid_ ? size_ - offset_ : 0;
}

} // namespace mini_dds::serialization
