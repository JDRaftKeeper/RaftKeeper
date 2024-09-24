#pragma once

#include <string>
#include <fmt/format.h>


namespace RK
{

class WriteBuffer;

}

/// Displays the passed size in bytes as 123.45 GiB.
void formatReadableSizeWithBinarySuffix(double value, RK::WriteBuffer & out, int precision = 2);
std::string formatReadableSizeWithBinarySuffix(double value, int precision = 2);

/// Displays the passed size in bytes as 132.55 GB.
void formatReadableSizeWithDecimalSuffix(double value, RK::WriteBuffer & out, int precision = 2);
std::string formatReadableSizeWithDecimalSuffix(double value, int precision = 2);

/// Prints the number as 123.45 billion.
void formatReadableQuantity(double value, RK::WriteBuffer & out, int precision = 2);
std::string formatReadableQuantity(double value, int precision = 2);


/// Wrapper around value. If used with fmt library (e.g. for log messages),
///  value is automatically formatted as size with binary suffix.
struct ReadableSize
{
    double value;
    explicit ReadableSize(double value_) : value(value_) {}
};

/// See https://fmt.dev/latest/api.html#formatting-user-defined-types
template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of_v<ReadableSize, T>, char>> :
    fmt::formatter<std::string> {
    auto format(const ReadableSize& size, format_context& ctx) const {
        return formatter<std::string>::format(formatReadableSizeWithBinarySuffix(size.value), ctx);
    }
};
