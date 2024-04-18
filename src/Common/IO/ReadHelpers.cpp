#include <stdlib.h>
#include <common/defines.h>
#include "Common/PODArray.h"
#include "Common/StringUtils.h"
#include "Common/hex.h"
#include "Common/memcpySmall.h"
#include "common/find_symbols.h"
#include "Operators.h"
#include "WriteBufferFromString.h"
#include "WriteHelpers.h"
#include "readFloatText.h"

#ifdef __SSE2__
    #include <emmintrin.h>
#endif

namespace RK
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_DATE;
}

template <typename IteratorSrc, typename IteratorDst>
void parseHex(IteratorSrc src, IteratorDst dst, const size_t num_bytes)
{
    size_t src_pos = 0;
    size_t dst_pos = 0;
    for (; dst_pos < num_bytes; ++dst_pos, src_pos += 2)
        dst[dst_pos] = unhex2(reinterpret_cast<const char *>(&src[src_pos]));
}

void NO_INLINE throwAtAssertionFailed(const char * s, ReadBuffer & buf)
{
    WriteBufferFromOwnString out;
    out << "Cannot parse input: expected " << quote << s;

    if (buf.eof())
        out << " at end of stream.";
    else
        out << " before: " << quote << String(buf.position(), std::min(SHOW_CHARS_ON_SYNTAX_ERROR, buf.buffer().end() - buf.position()));

    throw ParsingException(out.str(), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
}


bool checkString(const char * s, ReadBuffer & buf)
{
    for (; *s; ++s)
    {
        if (buf.eof() || *buf.position() != *s)
            return false;
        ++buf.position();
    }
    return true;
}


bool checkStringCaseInsensitive(const char * s, ReadBuffer & buf)
{
    for (; *s; ++s)
    {
        if (buf.eof())
            return false;

        char c = *buf.position();
        if (!equalsCaseInsensitive(*s, c))
            return false;

        ++buf.position();
    }
    return true;
}


void assertString(const char * s, ReadBuffer & buf)
{
    if (!checkString(s, buf))
        throwAtAssertionFailed(s, buf);
}


void assertEOF(ReadBuffer & buf)
{
    if (!buf.eof())
        throwAtAssertionFailed("eof", buf);
}


void assertStringCaseInsensitive(const char * s, ReadBuffer & buf)
{
    if (!checkStringCaseInsensitive(s, buf))
        throwAtAssertionFailed(s, buf);
}


bool checkStringByFirstCharacterAndAssertTheRest(const char * s, ReadBuffer & buf)
{
    if (buf.eof() || *buf.position() != *s)
        return false;

    assertString(s, buf);
    return true;
}

bool checkStringByFirstCharacterAndAssertTheRestCaseInsensitive(const char * s, ReadBuffer & buf)
{
    if (buf.eof())
        return false;

    char c = *buf.position();
    if (!equalsCaseInsensitive(*s, c))
        return false;

    assertStringCaseInsensitive(s, buf);
    return true;
}


template <typename T>
static void appendToStringOrVector(T & s, ReadBuffer & rb, const char * end)
{
    s.append(rb.position(), end - rb.position());
}

template <>
inline void appendToStringOrVector(PaddedPODArray<UInt8> & s, ReadBuffer & rb, const char * end)
{
    if (rb.isPadded())
        s.insertSmallAllowReadWriteOverflow15(rb.position(), end);
    else
        s.insert(rb.position(), end);
}

template <>
inline void appendToStringOrVector(PODArray<char> & s, ReadBuffer & rb, const char * end)
{
    s.insert(rb.position(), end);
}

template <char... chars, typename Vector>
void readStringUntilCharsInto(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<chars...>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (buf.hasPendingData())
            return;
    }
}

template <typename Vector>
void readStringInto(Vector & s, ReadBuffer & buf)
{
    readStringUntilCharsInto<'\t', '\n'>(s, buf);
}

template <typename Vector>
void readStringUntilWhitespaceInto(Vector & s, ReadBuffer & buf)
{
    readStringUntilCharsInto<' '>(s, buf);
}

template <typename Vector>
void readNullTerminated(Vector & s, ReadBuffer & buf)
{
    readStringUntilCharsInto<'\0'>(s, buf);
    buf.ignore();
}

void readStringUntilWhitespace(String & s, ReadBuffer & buf)
{
    s.clear();
    readStringUntilWhitespaceInto(s, buf);
}

template void readNullTerminated<PODArray<char>>(PODArray<char> & s, ReadBuffer & buf);
template void readNullTerminated<String>(String & s, ReadBuffer & buf);

void readString(String & s, ReadBuffer & buf)
{
    s.clear();
    readStringInto(s, buf);
}

template void readStringInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);


template <typename Vector>
void readStringUntilEOFInto(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        appendToStringOrVector(s, buf, buf.buffer().end());
        buf.position() = buf.buffer().end();
    }
}


void readStringUntilEOF(String & s, ReadBuffer & buf)
{
    s.clear();
    readStringUntilEOFInto(s, buf);
}

template <typename Vector>
void readEscapedStringUntilEOLInto(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\n', '\\'>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
            return;

        if (*buf.position() == '\\')
            parseComplexEscapeSequence(s, buf);
    }
}


void readEscapedStringUntilEOL(String & s, ReadBuffer & buf)
{
    s.clear();
    readEscapedStringUntilEOLInto(s, buf);
}

template void readStringUntilEOFInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);


/** Parse the escape sequence, which can be simple (one character after backslash) or more complex (multiple characters).
  * It is assumed that the cursor is located on the `\` symbol
  */
template <typename Vector>
static void parseComplexEscapeSequence(Vector & s, ReadBuffer & buf)
{
    ++buf.position();
    if (buf.eof())
        throw Exception("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

    char char_after_backslash = *buf.position();

    if (char_after_backslash == 'x')
    {
        ++buf.position();
        /// escape sequence of the form \xAA
        char hex_code[2];
        readPODBinary(hex_code, buf);
        s.push_back(unhex2(hex_code));
    }
    else if (char_after_backslash == 'N')
    {
        /// Support for NULLs: \N sequence must be parsed as empty string.
        ++buf.position();
    }
    else
    {
        /// The usual escape sequence of a single character.
        char decoded_char = parseEscapeSequence(char_after_backslash);

        /// For convenience using LIKE and regular expressions,
        /// we leave backslash when user write something like 'Hello 100\%':
        /// it is parsed like Hello 100\% instead of Hello 100%
        if (decoded_char != '\\'
            && decoded_char != '\''
            && decoded_char != '"'
            && decoded_char != '`'  /// MySQL style identifiers
            && decoded_char != '/'  /// JavaScript in HTML
            && !isControlASCII(decoded_char))
        {
            s.push_back('\\');
        }

        s.push_back(decoded_char);
        ++buf.position();
    }
}

template <typename Vector>
void readEscapedStringInto(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\t', '\n', '\\'>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\t' || *buf.position() == '\n')
            return;

        if (*buf.position() == '\\')
            parseComplexEscapeSequence(s, buf);
    }
}

void readEscapedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readEscapedStringInto(s, buf);
}

template void readEscapedStringInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readEscapedStringInto<NullOutput>(NullOutput & s, ReadBuffer & buf);


/** If enable_sql_style_quoting == true,
  *  strings like 'abc''def' will be parsed as abc'def.
  * Please note, that even with SQL style quoting enabled,
  *  backslash escape sequences are also parsed,
  *  that could be slightly confusing.
  */
template <char quote, bool enable_sql_style_quoting, typename Vector>
static void readAnyQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    if (buf.eof() || *buf.position() != quote)
    {
        throw ParsingException(ErrorCodes::CANNOT_PARSE_QUOTED_STRING,
            "Cannot parse quoted string: expected opening quote '{}', got '{}'",
            std::string{quote}, buf.eof() ? "EOF" : std::string{*buf.position()});
    }

    ++buf.position();

    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\\', quote>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == quote)
        {
            ++buf.position();

            if (enable_sql_style_quoting && !buf.eof() && *buf.position() == quote)
            {
                s.push_back(quote);
                ++buf.position();
                continue;
            }

            return;
        }

        if (*buf.position() == '\\')
            parseComplexEscapeSequence(s, buf);
    }

    throw ParsingException("Cannot parse quoted string: expected closing quote",
        ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
}

template <bool enable_sql_style_quoting, typename Vector>
void readQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    readAnyQuotedStringInto<'\'', enable_sql_style_quoting>(s, buf);
}

template <bool enable_sql_style_quoting, typename Vector>
void readDoubleQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    readAnyQuotedStringInto<'"', enable_sql_style_quoting>(s, buf);
}

template <bool enable_sql_style_quoting, typename Vector>
void readBackQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    readAnyQuotedStringInto<'`', enable_sql_style_quoting>(s, buf);
}


void readQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readQuotedStringInto<false>(s, buf);
}

void readQuotedStringWithSQLStyle(String & s, ReadBuffer & buf)
{
    s.clear();
    readQuotedStringInto<true>(s, buf);
}


template void readQuotedStringInto<true>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readDoubleQuotedStringInto<false>(NullOutput & s, ReadBuffer & buf);

void readDoubleQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readDoubleQuotedStringInto<false>(s, buf);
}

void readDoubleQuotedStringWithSQLStyle(String & s, ReadBuffer & buf)
{
    s.clear();
    readDoubleQuotedStringInto<true>(s, buf);
}

void readBackQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readBackQuotedStringInto<false>(s, buf);
}

void readBackQuotedStringWithSQLStyle(String & s, ReadBuffer & buf)
{
    s.clear();
    readBackQuotedStringInto<true>(s, buf);
}

template <typename ReturnType>
ReturnType readDateTextFallback(LocalDate & date, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = []
    {
        if constexpr (throw_exception)
            throw Exception("Cannot parse date: value is too short", ErrorCodes::CANNOT_PARSE_DATE);
        return ReturnType(false);
    };

    auto ignore_delimiter = [&]
    {
        if (!buf.eof())
        {
            ++buf.position();
            return true;
        }
        else
            return false;
    };

    auto append_digit = [&](auto & x)
    {
        if (!buf.eof() && isNumericASCII(*buf.position()))
        {
            x = x * 10 + (*buf.position() - '0');
            ++buf.position();
            return true;
        }
        else
            return false;
    };

    UInt16 year = 0;
    if (!append_digit(year)
        || !append_digit(year) // NOLINT
        || !append_digit(year) // NOLINT
        || !append_digit(year)) // NOLINT
        return error();

    if (!ignore_delimiter())
        return error();

    UInt8 month = 0;
    if (!append_digit(month))
        return error();
    append_digit(month);

    if (!ignore_delimiter())
        return error();

    UInt8 day = 0;
    if (!append_digit(day))
        return error();
    append_digit(day);

    date = LocalDate(year, month, day);
    return ReturnType(true);
}

template void readDateTextFallback<void>(LocalDate &, ReadBuffer &);
template bool readDateTextFallback<bool>(LocalDate &, ReadBuffer &);


template <typename ReturnType>
ReturnType readDateTimeTextFallback(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// YYYY-MM-DD hh:mm:ss
    static constexpr auto date_time_broken_down_length = 19;
    /// YYYY-MM-DD
    static constexpr auto date_broken_down_length = 10;

    char s[date_time_broken_down_length];
    char * s_pos = s;

    /** Read characters, that could represent unix timestamp.
      * Only unix timestamp of at least 5 characters is supported.
      * Then look at 5th character. If it is a number - treat whole as unix timestamp.
      * If it is not a number - then parse datetime in YYYY-MM-DD hh:mm:ss or YYYY-MM-DD format.
      */

    /// A piece similar to unix timestamp, maybe scaled to subsecond precision.
    while (s_pos < s + date_time_broken_down_length && !buf.eof() && isNumericASCII(*buf.position()))
    {
        *s_pos = *buf.position();
        ++s_pos;
        ++buf.position();
    }

    /// 2015-01-01 01:02:03 or 2015-01-01
    if (s_pos == s + 4 && !buf.eof() && !isNumericASCII(*buf.position()))
    {
        const auto already_read_length = s_pos - s;
        const size_t remaining_date_time_size = date_time_broken_down_length - already_read_length;
        const size_t remaining_date_size = date_broken_down_length - already_read_length;

        size_t size = buf.read(s_pos, remaining_date_time_size);
        if (size != remaining_date_time_size && size != remaining_date_size)
        {
            s_pos[size] = 0;

            if constexpr (throw_exception)
                throw ParsingException(std::string("Cannot parse datetime ") + s, ErrorCodes::CANNOT_PARSE_DATETIME);
            else
                return false;
        }

        UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
        UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
        UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');

        UInt8 hour = 0;
        UInt8 minute = 0;
        UInt8 second = 0;

        if (size == remaining_date_time_size)
        {
            hour = (s[11] - '0') * 10 + (s[12] - '0');
            minute = (s[14] - '0') * 10 + (s[15] - '0');
            second = (s[17] - '0') * 10 + (s[18] - '0');
        }

        if (unlikely(year == 0))
            datetime = 0;
        else
            datetime = date_lut.makeDateTime(year, month, day, hour, minute, second);
    }
    else
    {
        if (s_pos - s >= 5)
        {
            /// Not very efficient.
            datetime = 0;
            for (const char * digit_pos = s; digit_pos < s_pos; ++digit_pos)
                datetime = datetime * 10 + *digit_pos - '0';
        }
        else
        {
            if constexpr (throw_exception)
                throw ParsingException("Cannot parse datetime", ErrorCodes::CANNOT_PARSE_DATETIME);
            else
                return false;
        }
    }

    return ReturnType(true);
}

template void readDateTimeTextFallback<void>(time_t &, ReadBuffer &, const DateLUTImpl &);
template bool readDateTimeTextFallback<bool>(time_t &, ReadBuffer &, const DateLUTImpl &);


Exception readException(ReadBuffer & buf, const String & additional_message, bool remote_exception)
{
    int code = 0;
    String name;
    String message;
    String stack_trace;
    bool has_nested = false;    /// Obsolete

    readBinary(code, buf);
    readBinary(name, buf);
    readBinary(message, buf);
    readBinary(stack_trace, buf);
    readBinary(has_nested, buf);

    WriteBufferFromOwnString out;

    if (!additional_message.empty())
        out << additional_message << ". ";

    if (name != "RK::Exception")
        out << name << ". ";

    out << message << ".";

    if (!stack_trace.empty())
        out << " Stack trace:\n\n" << stack_trace;

    return Exception(out.str(), code, remote_exception);
}

void readAndThrowException(ReadBuffer & buf, const String & additional_message)
{
    readException(buf, additional_message).rethrow();
}


void skipToCarriageReturnOrEOF(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\r'>(buf.position(), buf.buffer().end());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\r')
        {
            ++buf.position();
            return;
        }
    }
}


void skipToNextLineOrEOF(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\n'>(buf.position(), buf.buffer().end());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
        {
            ++buf.position();
            return;
        }
    }
}


void skipToUnescapedNextLineOrEOF(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\n', '\\'>(buf.position(), buf.buffer().end());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
        {
            ++buf.position();
            return;
        }

        if (*buf.position() == '\\')
        {
            ++buf.position();
            if (buf.eof())
                return;

            /// Skip escaped character. We do not consider escape sequences with more than one character after backslash (\x01).
            /// It's ok for the purpose of this function, because we are interested only in \n and \\.
            ++buf.position();
            continue;
        }
    }
}

void saveUpToPosition(ReadBuffer & in, RK::Memory<> & memory, char * current)
{
    assert(current >= in.position());
    assert(current <= in.buffer().end());

    const size_t old_bytes = memory.size();
    const size_t additional_bytes = current - in.position();
    const size_t new_bytes = old_bytes + additional_bytes;
    /// There are no new bytes to add to memory.
    /// No need to do extra stuff.
    if (new_bytes == 0)
        return;
    memory.resize(new_bytes);
    memcpy(memory.data() + old_bytes, in.position(), additional_bytes);
    in.position() = current;
}

bool loadAtPosition(ReadBuffer & in, RK::Memory<> & memory, char * & current)
{
    assert(current <= in.buffer().end());

    if (current < in.buffer().end())
        return true;

    saveUpToPosition(in, memory, current);

    bool loaded_more = !in.eof();
    // A sanity check. Buffer position may be in the beginning of the buffer
    // (normal case), or have some offset from it (AIO).
    assert(in.position() >= in.buffer().begin());
    assert(in.position() <= in.buffer().end());
    current = in.position();

    return loaded_more;
}

}
