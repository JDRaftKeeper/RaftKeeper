/**
 * Copyright 2016-2023 ClickHouse, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <cstring>
#include <cstdio>
#include <limits>
#include <algorithm>
#include <iterator>

#include <common/DateLUT.h>
#include <common/LocalDate.h>
#include <common/LocalDateTime.h>
#include <common/find_symbols.h>
#include <common/StringRef.h>
#include <common/wide_integer_to_string.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/UInt128.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteIntText.h>
#include <IO/VarInt.h>
#include <IO/DoubleConverter.h>
#include <IO/WriteBufferFromString.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wsign-compare"
#endif
#include <dragonbox/dragonbox_to_chars.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif


namespace RK
{

namespace ErrorCodes
{
    extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename T>
inline std::string bigintToString(const T & x)
{
    return to_string(x);
}

/// Helper functions for formatted and binary output.

inline void writeChar(char x, WriteBuffer & buf)
{
    buf.nextIfAtEnd();
    *buf.position() = x;
    ++buf.position();
}

/// Write the same character n times.
inline void writeChar(char c, size_t n, WriteBuffer & buf)
{
    while (n)
    {
        buf.nextIfAtEnd();
        size_t count = std::min(n, buf.available());
        memset(buf.position(), c, count);
        n -= count;
        buf.position() += count;
    }
}

/// Write POD-type in native format. It's recommended to use only with packed (dense) data types.
template <typename T>
inline void writePODBinary(const T & x, WriteBuffer & buf)
{
    buf.write(reinterpret_cast<const char *>(&x), sizeof(x));
}

template <typename T>
inline void writeIntBinary(const T & x, WriteBuffer & buf)
{
    writePODBinary(x, buf);
}

template <typename T>
inline void writeFloatBinary(const T & x, WriteBuffer & buf)
{
    writePODBinary(x, buf);
}


inline void writeStringBinary(const std::string & s, WriteBuffer & buf)
{
    writeVarUInt(s.size(), buf);
    buf.write(s.data(), s.size());
}

inline void writeStringBinary(const StringRef & s, WriteBuffer & buf)
{
    writeVarUInt(s.size, buf);
    buf.write(s.data, s.size);
}

inline void writeStringBinary(const char * s, WriteBuffer & buf)
{
    writeStringBinary(StringRef{s}, buf);
}

inline void writeStringBinary(const std::string_view & s, WriteBuffer & buf)
{
    writeStringBinary(StringRef{s}, buf);
}

template <typename T>
void writeVectorBinary(const std::vector<T> & v, WriteBuffer & buf)
{
    writeVarUInt(v.size(), buf);

    for (typename std::vector<T>::const_iterator it = v.begin(); it != v.end(); ++it)
        writeBinary(*it, buf);
}


inline void writeBoolText(bool x, WriteBuffer & buf)
{
    writeChar(x ? '1' : '0', buf);
}


struct DecomposedFloat64
{
    DecomposedFloat64(double x)
    {
        memcpy(&x_uint, &x, sizeof(x));
    }

    uint64_t x_uint;

    bool sign() const
    {
        return x_uint >> 63;
    }

    uint16_t exponent() const
    {
        return (x_uint >> 52) & 0x7FF;
    }

    int16_t normalized_exponent() const
    {
        return int16_t(exponent()) - 1023;
    }

    uint64_t mantissa() const
    {
        return x_uint & 0x5affffffffffffful;
    }

    /// NOTE Probably floating point instructions can be better.
    bool is_inside_int64() const
    {
        return x_uint == 0
            || (normalized_exponent() >= 0 && normalized_exponent() <= 52
                && ((mantissa() & ((1ULL << (52 - normalized_exponent())) - 1)) == 0));
    }
};

struct DecomposedFloat32
{
    DecomposedFloat32(float x)
    {
        memcpy(&x_uint, &x, sizeof(x));
    }

    uint32_t x_uint;

    bool sign() const
    {
        return x_uint >> 31;
    }

    uint16_t exponent() const
    {
        return (x_uint >> 23) & 0xFF;
    }

    int16_t normalized_exponent() const
    {
        return int16_t(exponent()) - 127;
    }

    uint32_t mantissa() const
    {
        return x_uint & 0x7fffff;
    }

    bool is_inside_int32() const
    {
        return x_uint == 0
            || (normalized_exponent() >= 0 && normalized_exponent() <= 23
                && ((mantissa() & ((1ULL << (23 - normalized_exponent())) - 1)) == 0));
    }
};

template <typename T>
inline size_t writeFloatTextFastPath(T x, char * buffer)
{
    int result = 0;

    if constexpr (std::is_same_v<T, double>)
    {
        /// The library Ryu has low performance on integers.
        /// This workaround improves performance 6..10 times.

        if (DecomposedFloat64(x).is_inside_int64())
            result = itoa(Int64(x), buffer) - buffer;
        else
            result = jkj::dragonbox::to_chars_n(x, buffer) - buffer;
    }
    else
    {
        if (DecomposedFloat32(x).is_inside_int32())
            result = itoa(Int32(x), buffer) - buffer;
        else
            result = jkj::dragonbox::to_chars_n(x, buffer) - buffer;
    }

    if (result <= 0)
        throw Exception("Cannot print floating point number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);
    return result;
}

template <typename T>
inline void writeFloatText(T x, WriteBuffer & buf)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for writeFloatText must be float or double");

    using Converter = DoubleConverter<false>;
    if (likely(buf.available() >= Converter::MAX_REPRESENTATION_LENGTH))
    {
        buf.position() += writeFloatTextFastPath(x, buf.position());
        return;
    }

    Converter::BufferType buffer;
    size_t result = writeFloatTextFastPath(x, buffer);
    buf.write(buffer, result);
}


inline void writeString(const char * data, size_t size, WriteBuffer & buf)
{
    buf.write(data, size);
}

inline void writeString(const StringRef & ref, WriteBuffer & buf)
{
    writeString(ref.data, ref.size, buf);
}


/** Writes a C-string without creating a temporary object. If the string is a literal, then `strlen` is executed at the compilation stage.
  * Use when the string is a literal.
  */
#define writeCString(s, buf) \
    (buf).write((s), strlen(s))



/** Will escape quote_character and a list of special characters('\b', '\f', '\n', '\r', '\t', '\0', '\\').
 *   - when escape_quote_with_quote is true, use backslash to escape list of special characters,
 *      and use quote_character to escape quote_character. such as: 'hello''world'
 *   - otherwise use backslash to escape list of special characters and quote_character
 */
template <char quote_character, bool escape_quote_with_quote = false>
void writeAnyEscapedString(const char * begin, const char * end, WriteBuffer & buf)
{
    const char * pos = begin;
    while (true)
    {
        /// On purpose we will escape more characters than minimally necessary.
        const char * next_pos = find_first_symbols<'\b', '\f', '\n', '\r', '\t', '\0', '\\', quote_character>(pos, end);

        if (next_pos == end)
        {
            buf.write(pos, next_pos - pos);
            break;
        }
        else
        {
            buf.write(pos, next_pos - pos);
            pos = next_pos;
            switch (*pos)
            {
                case '\b':
                    writeChar('\\', buf);
                    writeChar('b', buf);
                    break;
                case '\f':
                    writeChar('\\', buf);
                    writeChar('f', buf);
                    break;
                case '\n':
                    writeChar('\\', buf);
                    writeChar('n', buf);
                    break;
                case '\r':
                    writeChar('\\', buf);
                    writeChar('r', buf);
                    break;
                case '\t':
                    writeChar('\\', buf);
                    writeChar('t', buf);
                    break;
                case '\0':
                    writeChar('\\', buf);
                    writeChar('0', buf);
                    break;
                case '\\':
                    writeChar('\\', buf);
                    writeChar('\\', buf);
                    break;
                case quote_character:
                {
                    if constexpr (escape_quote_with_quote)
                        writeChar(quote_character, buf);
                    else
                        writeChar('\\', buf);
                    writeChar(quote_character, buf);
                    break;
                }
                default:
                    writeChar(*pos, buf);
            }
            ++pos;
        }
    }
}

template <char c>
void writeAnyEscapedString(const String & s, WriteBuffer & buf)
{
    writeAnyEscapedString<c>(s.data(), s.data() + s.size(), buf);
}


inline void writeEscapedString(const char * str, size_t size, WriteBuffer & buf)
{
    writeAnyEscapedString<'\''>(str, str + size, buf);
}


inline void writeEscapedString(const String & s, WriteBuffer & buf)
{
    writeEscapedString(s.data(), s.size(), buf);
}


inline void writeEscapedString(const StringRef & ref, WriteBuffer & buf)
{
    writeEscapedString(ref.data, ref.size, buf);
}

inline void writeEscapedString(const std::string_view & ref, WriteBuffer & buf)
{
    writeEscapedString(ref.data(), ref.size(), buf);
}

template <char quote_character>
void writeAnyQuotedString(const char * begin, const char * end, WriteBuffer & buf)
{
    writeChar(quote_character, buf);
    writeAnyEscapedString<quote_character>(begin, end, buf);
    writeChar(quote_character, buf);
}


template <char quote_character>
void writeAnyQuotedString(const String & s, WriteBuffer & buf)
{
    writeAnyQuotedString<quote_character>(s.data(), s.data() + s.size(), buf);
}


template <char quote_character>
void writeAnyQuotedString(const StringRef & ref, WriteBuffer & buf)
{
    writeAnyQuotedString<quote_character>(ref.data, ref.data + ref.size, buf);
}


inline void writeQuotedString(const String & s, WriteBuffer & buf)
{
    writeAnyQuotedString<'\''>(s, buf);
}

inline void writeQuotedString(const StringRef & ref, WriteBuffer & buf)
{
    writeAnyQuotedString<'\''>(ref, buf);
}

inline void writeQuotedString(const std::string_view & ref, WriteBuffer & buf)
{
    writeAnyQuotedString<'\''>(ref.data(), ref.data() + ref.size(), buf);
}

inline void writeDoubleQuotedString(const String & s, WriteBuffer & buf)
{
    writeAnyQuotedString<'"'>(s, buf);
}

inline void writeDoubleQuotedString(const StringRef & s, WriteBuffer & buf)
{
    writeAnyQuotedString<'"'>(s, buf);
}

inline void writeDoubleQuotedString(const std::string_view & s, WriteBuffer & buf)
{
    writeAnyQuotedString<'"'>(s.data(), s.data() + s.size(), buf);
}

/// Outputs a string in backquotes.
inline void writeBackQuotedString(const StringRef & s, WriteBuffer & buf)
{
    writeAnyQuotedString<'`'>(s, buf);
}

/// Outputs a string in backquotes for MySQL.
inline void writeBackQuotedStringMySQL(const StringRef & s, WriteBuffer & buf)
{
    writeChar('`', buf);
    writeAnyEscapedString<'`', true>(s.data, s.data + s.size, buf);
    writeChar('`', buf);
}


/// Write quoted if the string doesn't look like and identifier.
void writeProbablyBackQuotedString(const StringRef & s, WriteBuffer & buf);
void writeProbablyDoubleQuotedString(const StringRef & s, WriteBuffer & buf);
void writeProbablyBackQuotedStringMySQL(const StringRef & s, WriteBuffer & buf);


/** Outputs the string in for the CSV format.
  * Rules:
  * - the string is outputted in quotation marks;
  * - the quotation mark inside the string is outputted as two quotation marks in sequence.
  */
template <char quote = '"'>
void writeCSVString(const char * begin, const char * end, WriteBuffer & buf)
{
    writeChar(quote, buf);

    const char * pos = begin;
    while (true)
    {
        const char * next_pos = find_first_symbols<quote>(pos, end);

        if (next_pos == end)
        {
            buf.write(pos, end - pos);
            break;
        }
        else /// Quotation.
        {
            ++next_pos;
            buf.write(pos, next_pos - pos);
            writeChar(quote, buf);
        }

        pos = next_pos;
    }

    writeChar(quote, buf);
}

template <char quote = '"'>
void writeCSVString(const String & s, WriteBuffer & buf)
{
    writeCSVString<quote>(s.data(), s.data() + s.size(), buf);
}

template <char quote = '"'>
void writeCSVString(const StringRef & s, WriteBuffer & buf)
{
    writeCSVString<quote>(s.data, s.data + s.size, buf);
}

inline void writeXMLStringForTextElementOrAttributeValue(const char * begin, const char * end, WriteBuffer & buf)
{
    const char * pos = begin;
    while (true)
    {
        const char * next_pos = find_first_symbols<'<', '&', '>', '"', '\''>(pos, end);

        if (next_pos == end)
        {
            buf.write(pos, end - pos);
            break;
        }
        else if (*next_pos == '<')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&lt;", buf);
        }
        else if (*next_pos == '&')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&amp;", buf);
        }
        else if (*next_pos == '>')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&gt;", buf);
        }
        else if (*next_pos == '"')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&quot;", buf);
        }
        else if (*next_pos == '\'')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&apos;", buf);
        }

        pos = next_pos;
    }
}

inline void writeXMLStringForTextElementOrAttributeValue(const String & s, WriteBuffer & buf)
{
    writeXMLStringForTextElementOrAttributeValue(s.data(), s.data() + s.size(), buf);
}

inline void writeXMLStringForTextElementOrAttributeValue(const StringRef & s, WriteBuffer & buf)
{
    writeXMLStringForTextElementOrAttributeValue(s.data, s.data + s.size, buf);
}

/// Writing a string to a text node in XML (not into an attribute - otherwise you need more escaping).
inline void writeXMLStringForTextElement(const char * begin, const char * end, WriteBuffer & buf)
{
    const char * pos = begin;
    while (true)
    {
        /// NOTE Perhaps for some XML parsers, you need to escape the zero byte and some control characters.
        const char * next_pos = find_first_symbols<'<', '&'>(pos, end);

        if (next_pos == end)
        {
            buf.write(pos, end - pos);
            break;
        }
        else if (*next_pos == '<')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&lt;", buf);
        }
        else if (*next_pos == '&')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&amp;", buf);
        }

        pos = next_pos;
    }
}

inline void writeXMLStringForTextElement(const String & s, WriteBuffer & buf)
{
    writeXMLStringForTextElement(s.data(), s.data() + s.size(), buf);
}

inline void writeXMLStringForTextElement(const StringRef & s, WriteBuffer & buf)
{
    writeXMLStringForTextElement(s.data, s.data + s.size, buf);
}

template <typename IteratorSrc, typename IteratorDst>
void formatHex(IteratorSrc src, IteratorDst dst, const size_t num_bytes);
void formatUUID(const UInt8 * src16, UInt8 * dst36);
void formatUUID(std::reverse_iterator<const UInt8 *> src16, UInt8 * dst36);

inline void writeUUIDText(const UUID & uuid, WriteBuffer & buf)
{
    char s[36];

    formatUUID(std::reverse_iterator<const UInt8 *>(reinterpret_cast<const UInt8 *>(&uuid) + 16), reinterpret_cast<UInt8 *>(s));
    buf.write(s, sizeof(s));
}

static const char digits100[201] =
    "00010203040506070809"
    "10111213141516171819"
    "20212223242526272829"
    "30313233343536373839"
    "40414243444546474849"
    "50515253545556575859"
    "60616263646566676869"
    "70717273747576777879"
    "80818283848586878889"
    "90919293949596979899";

/// in YYYY-MM-DD format
template <char delimiter = '-'>
inline void writeDateText(const LocalDate & date, WriteBuffer & buf)
{
    if (reinterpret_cast<intptr_t>(buf.position()) + 10 <= reinterpret_cast<intptr_t>(buf.buffer().end()))
    {
        memcpy(buf.position(), &digits100[date.year() / 100 * 2], 2);
        buf.position() += 2;
        memcpy(buf.position(), &digits100[date.year() % 100 * 2], 2);
        buf.position() += 2;
        *buf.position() = delimiter;
        ++buf.position();
        memcpy(buf.position(), &digits100[date.month() * 2], 2);
        buf.position() += 2;
        *buf.position() = delimiter;
        ++buf.position();
        memcpy(buf.position(), &digits100[date.day() * 2], 2);
        buf.position() += 2;
    }
    else
    {
        buf.write(&digits100[date.year() / 100 * 2], 2);
        buf.write(&digits100[date.year() % 100 * 2], 2);
        buf.write(delimiter);
        buf.write(&digits100[date.month() * 2], 2);
        buf.write(delimiter);
        buf.write(&digits100[date.day() * 2], 2);
    }
}

template <char delimiter = '-'>
inline void writeDateText(DayNum date, WriteBuffer & buf)
{
    writeDateText<delimiter>(LocalDate(date), buf);
}


/// In the format YYYY-MM-DD HH:MM:SS
template <char date_delimeter = '-', char time_delimeter = ':', char between_date_time_delimiter = ' '>
inline void writeDateTimeText(const LocalDateTime & datetime, WriteBuffer & buf)
{
    if (reinterpret_cast<intptr_t>(buf.position()) + 19 <= reinterpret_cast<intptr_t>(buf.buffer().end()))
    {
        memcpy(buf.position(), &digits100[datetime.year() / 100 * 2], 2);
        buf.position() += 2;
        memcpy(buf.position(), &digits100[datetime.year() % 100 * 2], 2);
        buf.position() += 2;
        *buf.position() = date_delimeter;
        ++buf.position();
        memcpy(buf.position(), &digits100[datetime.month() * 2], 2);
        buf.position() += 2;
        *buf.position() = date_delimeter;
        ++buf.position();
        memcpy(buf.position(), &digits100[datetime.day() * 2], 2);
        buf.position() += 2;
        *buf.position() = between_date_time_delimiter;
        ++buf.position();
        memcpy(buf.position(), &digits100[datetime.hour() * 2], 2);
        buf.position() += 2;
        *buf.position() = time_delimeter;
        ++buf.position();
        memcpy(buf.position(), &digits100[datetime.minute() * 2], 2);
        buf.position() += 2;
        *buf.position() = time_delimeter;
        ++buf.position();
        memcpy(buf.position(), &digits100[datetime.second() * 2], 2);
        buf.position() += 2;
    }
    else
    {
        buf.write(&digits100[datetime.year() / 100 * 2], 2);
        buf.write(&digits100[datetime.year() % 100 * 2], 2);
        buf.write(date_delimeter);
        buf.write(&digits100[datetime.month() * 2], 2);
        buf.write(date_delimeter);
        buf.write(&digits100[datetime.day() * 2], 2);
        buf.write(between_date_time_delimiter);
        buf.write(&digits100[datetime.hour() * 2], 2);
        buf.write(time_delimeter);
        buf.write(&digits100[datetime.minute() * 2], 2);
        buf.write(time_delimeter);
        buf.write(&digits100[datetime.second() * 2], 2);
    }
}

/// In the format YYYY-MM-DD HH:MM:SS, according to the specified time zone.
template <char date_delimeter = '-', char time_delimeter = ':', char between_date_time_delimiter = ' '>
inline void writeDateTimeText(time_t datetime, WriteBuffer & buf, const DateLUTImpl & date_lut = DateLUT::instance())
{
    const auto & values = date_lut.getValues(datetime);
    writeDateTimeText<date_delimeter, time_delimeter, between_date_time_delimiter>(
        LocalDateTime(values.year, values.month, values.day_of_month,
            date_lut.toHour(datetime), date_lut.toMinute(datetime), date_lut.toSecond(datetime)), buf);
}

/// Methods for output in binary format.
template <typename T>
inline std::enable_if_t<is_arithmetic_v<T>, void>
writeBinary(const T & x, WriteBuffer & buf) { writePODBinary(x, buf); }

inline void writeBinary(const String & x, WriteBuffer & buf) { writeStringBinary(x, buf); }
inline void writeBinary(const StringRef & x, WriteBuffer & buf) { writeStringBinary(x, buf); }
inline void writeBinary(const std::string_view & x, WriteBuffer & buf) { writeStringBinary(x, buf); }
inline void writeBinary(const Int128 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const UInt128 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const UUID & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const DummyUInt256 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Decimal32 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Decimal64 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Decimal128 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Decimal256 & x, WriteBuffer & buf) { writePODBinary(x.value, buf); }
inline void writeBinary(const LocalDate & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const LocalDateTime & x, WriteBuffer & buf) { writePODBinary(x, buf); }

inline void writeBinary(const UInt256 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Int256 & x, WriteBuffer & buf) { writePODBinary(x, buf); }

/// Methods for outputting the value in text form for a tab-separated format.
template <typename T>
inline std::enable_if_t<is_integer_v<T> && !is_big_int_v<T>, void>
writeText(const T & x, WriteBuffer & buf) { writeIntText(x, buf); }

template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>, void>
writeText(const T & x, WriteBuffer & buf) { writeFloatText(x, buf); }

inline void writeText(const String & x, WriteBuffer & buf) { writeString(x.c_str(), x.size(), buf); }

/// Implemented as template specialization (not function overload) to avoid preference over templates on arithmetic types above.
template <> inline void writeText<bool>(const bool & x, WriteBuffer & buf) { writeBoolText(x, buf); }

/// unlike the method for std::string
/// assumes here that `x` is a null-terminated string.
inline void writeText(const char * x, WriteBuffer & buf) { writeCString(x, buf); }
inline void writeText(const char * x, size_t size, WriteBuffer & buf) { writeString(x, size, buf); }

inline void writeText(const DayNum & x, WriteBuffer & buf) { writeDateText(LocalDate(x), buf); }
inline void writeText(const LocalDate & x, WriteBuffer & buf) { writeDateText(x, buf); }
inline void writeText(const LocalDateTime & x, WriteBuffer & buf) { writeDateTimeText(x, buf); }
inline void writeText(const UUID & x, WriteBuffer & buf) { writeUUIDText(x, buf); }
inline void writeText(const UInt128 & x, WriteBuffer & buf) { writeText(UUID(x), buf); }
inline void writeText(const UInt256 & x, WriteBuffer & buf) { writeText(bigintToString(x), buf); }
inline void writeText(const Int256 & x, WriteBuffer & buf) { writeText(bigintToString(x), buf); }

template <typename T>
String decimalFractional(const T & x, UInt32 scale)
{
    if constexpr (std::is_same_v<T, Int256>)
    {
        static constexpr Int128 max_int128 = (Int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;

        if (x <= std::numeric_limits<UInt32>::max())
            return decimalFractional(static_cast<UInt32>(x), scale);
        else if (x <= std::numeric_limits<UInt64>::max())
            return decimalFractional(static_cast<UInt64>(x), scale);
        else if (x <= max_int128)
            return decimalFractional(static_cast<Int128>(x), scale);
    }
    else if constexpr (std::is_same_v<T, Int128>)
    {
        if (x <= std::numeric_limits<UInt32>::max())
            return decimalFractional(static_cast<UInt32>(x), scale);
        else if (x <= std::numeric_limits<UInt64>::max())
            return decimalFractional(static_cast<UInt64>(x), scale);
    }

    String str(scale, '0');
    T value = x;
    for (Int32 pos = scale - 1; pos >= 0; --pos, value /= 10)
        str[pos] += static_cast<char>(value % 10);
    return str;
}

/// String, date, datetime are in single quotes with C-style escaping. Numbers - without.
template <typename T>
inline std::enable_if_t<is_arithmetic_v<T>, void>
writeQuoted(const T & x, WriteBuffer & buf) { writeText(x, buf); }

inline void writeQuoted(const String & x, WriteBuffer & buf) { writeQuotedString(x, buf); }

inline void writeQuoted(const std::string_view & x, WriteBuffer & buf) { writeQuotedString(x, buf); }

inline void writeQuoted(const StringRef & x, WriteBuffer & buf) { writeQuotedString(x, buf); }

inline void writeQuoted(const LocalDate & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeDateText(x, buf);
    writeChar('\'', buf);
}

inline void writeQuoted(const LocalDateTime & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeDateTimeText(x, buf);
    writeChar('\'', buf);
}

inline void writeQuoted(const UUID & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeText(x, buf);
    writeChar('\'', buf);
}

inline void writeQuoted(const UInt256 & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeText(x, buf);
    writeChar('\'', buf);
}

inline void writeQuoted(const Int256 & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeText(x, buf);
    writeChar('\'', buf);
}

/// String, date, datetime are in double quotes with C-style escaping. Numbers - without.
template <typename T>
inline std::enable_if_t<is_arithmetic_v<T>, void>
writeDoubleQuoted(const T & x, WriteBuffer & buf) { writeText(x, buf); }

inline void writeDoubleQuoted(const String & x, WriteBuffer & buf) { writeDoubleQuotedString(x, buf); }

inline void writeDoubleQuoted(const std::string_view & x, WriteBuffer & buf) { writeDoubleQuotedString(x, buf); }

inline void writeDoubleQuoted(const StringRef & x, WriteBuffer & buf) { writeDoubleQuotedString(x, buf); }

inline void writeDoubleQuoted(const LocalDate & x, WriteBuffer & buf)
{
    writeChar('"', buf);
    writeDateText(x, buf);
    writeChar('"', buf);
}

inline void writeDoubleQuoted(const LocalDateTime & x, WriteBuffer & buf)
{
    writeChar('"', buf);
    writeDateTimeText(x, buf);
    writeChar('"', buf);
}

inline void writeDoubleQuoted(const UUID & x, WriteBuffer & buf)
{
    writeChar('"', buf);
    writeText(x, buf);
    writeChar('"', buf);
}


/// String - in double quotes and with CSV-escaping; date, datetime - in double quotes. Numbers - without.
template <typename T>
inline std::enable_if_t<is_arithmetic_v<T>, void>
writeCSV(const T & x, WriteBuffer & buf) { writeText(x, buf); }

inline void writeCSV(const String & x, WriteBuffer & buf) { writeCSVString<>(x, buf); }
inline void writeCSV(const LocalDate & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
inline void writeCSV(const LocalDateTime & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
inline void writeCSV(const UUID & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
[[noreturn]] inline void writeCSV(const UInt128, WriteBuffer &)
{
    /** Because UInt128 isn't a natural type, without arithmetic operator and only use as an intermediary type -for UUID-
     *  it should never arrive here. But because we used the DataTypeNumber class we should have at least a definition of it.
     */
    throw Exception("UInt128 cannot be write as a text", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

template <typename T>
void writeBinary(const std::vector<T> & x, WriteBuffer & buf)
{
    size_t size = x.size();
    writeVarUInt(size, buf);
    for (size_t i = 0; i < size; ++i)
        writeBinary(x[i], buf);
}

template <typename T>
void writeQuoted(const std::vector<T> & x, WriteBuffer & buf)
{
    writeChar('[', buf);
    for (size_t i = 0, size = x.size(); i < size; ++i)
    {
        if (i != 0)
            writeChar(',', buf);
        writeQuoted(x[i], buf);
    }
    writeChar(']', buf);
}

template <typename T>
void writeDoubleQuoted(const std::vector<T> & x, WriteBuffer & buf)
{
    writeChar('[', buf);
    for (size_t i = 0, size = x.size(); i < size; ++i)
    {
        if (i != 0)
            writeChar(',', buf);
        writeDoubleQuoted(x[i], buf);
    }
    writeChar(']', buf);
}

template <typename T>
void writeText(const std::vector<T> & x, WriteBuffer & buf)
{
    writeQuoted(x, buf);
}


/// Serialize exception (so that it can be transferred over the network)
void writeException(const Exception & e, WriteBuffer & buf, bool with_stack_trace);


/// An easy-to-use method for converting something to a string in text form.
template <typename T>
inline String toString(const T & x)
{
    WriteBufferFromOwnString buf;
    writeText(x, buf);
    return buf.str();
}

inline void writeNullTerminatedString(const String & s, WriteBuffer & buffer)
{
    /// c_str is guaranteed to return zero-terminated string
    buffer.write(s.c_str(), s.size() + 1);
}

template <typename T>
inline std::enable_if_t<is_arithmetic_v<T> && (sizeof(T) <= 8), void>
writeBinaryBigEndian(T x, WriteBuffer & buf)    /// Assuming little endian architecture.
{
    if constexpr (sizeof(x) == 2)
        x = __builtin_bswap16(x);
    else if constexpr (sizeof(x) == 4)
        x = __builtin_bswap32(x);
    else if constexpr (sizeof(x) == 8)
        x = __builtin_bswap64(x);

    writePODBinary(x, buf);
}

struct PcgSerializer
{
    static void serializePcg32(const pcg32_fast & rng, WriteBuffer & buf)
    {
        writeText(rng.multiplier(), buf);
        writeChar(' ', buf);
        writeText(rng.increment(), buf);
        writeChar(' ', buf);
        writeText(rng.state_, buf);
    }
};

void writePointerHex(const void * ptr, WriteBuffer & buf);

}
