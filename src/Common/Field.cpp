#include "Field.h"
#include "Common/IO/ReadBuffer.h"
#include "Common/IO/ReadBufferFromString.h"
#include "Common/IO/ReadHelpers.h"
#include "Common/IO/WriteBuffer.h"
#include "Common/IO/WriteHelpers.h"
#include "Common/IO/readDecimalText.h"
//#include <Core/DecimalComparison.h>
#include "FieldVisitors.h"


namespace RK
{
namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_FROM_FIELD_DUMP;
    extern const int DECIMAL_OVERFLOW;
}

inline Field getBinaryValue(UInt8 type, ReadBuffer & buf)
{
    switch (type)
    {
        case Field::Types::Null: {
            return RK::Field();
        }
        case Field::Types::UInt64: {
            UInt64 value;
            RK::readVarUInt(value, buf);
            return value;
        }
        case Field::Types::UInt128: {
            UInt128 value;
            RK::readBinary(value, buf);
            return value;
        }
        case Field::Types::Int64: {
            Int64 value;
            RK::readVarInt(value, buf);
            return value;
        }
        case Field::Types::Float64: {
            Float64 value;
            RK::readFloatBinary(value, buf);
            return value;
        }
        case Field::Types::String: {
            std::string value;
            RK::readStringBinary(value, buf);
            return value;
        }
        case Field::Types::Array: {
            Array value;
            RK::readBinary(value, buf);
            return value;
        }
        case Field::Types::Tuple: {
            Tuple value;
            RK::readBinary(value, buf);
            return value;
        }
        case Field::Types::Map: {
            Map value;
            RK::readBinary(value, buf);
            return value;
        }
        case Field::Types::AggregateFunctionState: {
            AggregateFunctionStateData value;
            RK::readStringBinary(value.name, buf);
            RK::readStringBinary(value.data, buf);
            return value;
        }
    }
    return RK::Field();
}

void readBinary(Array & x, ReadBuffer & buf)
{
    size_t size;
    UInt8 type;
    RK::readBinary(type, buf);
    RK::readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
        x.push_back(getBinaryValue(type, buf));
}

void writeBinary(const Array & x, WriteBuffer & buf)
{
    UInt8 type = Field::Types::Null;
    size_t size = x.size();
    if (size)
        type = x.front().getType();
    RK::writeBinary(type, buf);
    RK::writeBinary(size, buf);

    for (const auto & elem : x)
        Field::dispatch([&buf] (const auto & value) { RK::FieldVisitorWriteBinary()(value, buf); }, elem);
}

void writeText(const Array & x, WriteBuffer & buf)
{
    RK::String res = applyVisitor(RK::FieldVisitorToString(), RK::Field(x));
    buf.write(res.data(), res.size());
}

void readBinary(Tuple & x, ReadBuffer & buf)
{
    size_t size;
    RK::readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        RK::readBinary(type, buf);
        x.push_back(getBinaryValue(type, buf));
    }
}

void writeBinary(const Tuple & x, WriteBuffer & buf)
{
    const size_t size = x.size();
    RK::writeBinary(size, buf);

    for (const auto & elem : x)
    {
        const UInt8 type = elem.getType();
        RK::writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { RK::FieldVisitorWriteBinary()(value, buf); }, elem);
    }
}

void writeText(const Tuple & x, WriteBuffer & buf)
{
    writeFieldText(RK::Field(x), buf);
}

void readBinary(Map & x, ReadBuffer & buf)
{
    size_t size;
    RK::readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        RK::readBinary(type, buf);
        x.push_back(getBinaryValue(type, buf));
    }
}

void writeBinary(const Map & x, WriteBuffer & buf)
{
    const size_t size = x.size();
    RK::writeBinary(size, buf);

    for (const auto & elem : x)
    {
        const UInt8 type = elem.getType();
        RK::writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { RK::FieldVisitorWriteBinary()(value, buf); }, elem);
    }
}

void writeText(const Map & x, WriteBuffer & buf)
{
    writeFieldText(RK::Field(x), buf);
}

void writeFieldText(const Field & x, WriteBuffer & buf)
{
    RK::String res = Field::dispatch(RK::FieldVisitorToString(), x);
    buf.write(res.data(), res.size());
}


String Field::dump() const
{
    return applyVisitor(RK::FieldVisitorDump(), *this);
}

Field Field::restoreFromDump(const std::string_view & dump_)
{
    auto show_error = [&dump_]
    {
        throw Exception("Couldn't restore Field from dump: " + String{dump_}, ErrorCodes::CANNOT_RESTORE_FROM_FIELD_DUMP);
    };

    std::string_view dump = dump_;
    trim(dump);

    if (dump == "NULL")
        return {};

    std::string_view prefix = std::string_view{"Int64_"};
    if (dump.starts_with(prefix))
    {
        Int64 value = parseFromString<Int64>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"UInt64_"};
    if (dump.starts_with(prefix))
    {
        UInt64 value = parseFromString<UInt64>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"Int128_"};
    if (dump.starts_with(prefix))
    {
        Int128 value = parseFromString<Int128>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"Int256_"};
    if (dump.starts_with(prefix))
    {
        Int256 value = parseFromString<Int256>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"UInt256_"};
    if (dump.starts_with(prefix))
    {
        UInt256 value = parseFromString<UInt256>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"Float64_"};
    if (dump.starts_with(prefix))
    {
        Float64 value = parseFromString<Float64>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"UUID_"};
    if (dump.starts_with(prefix))
    {
        UUID uuid;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(uuid, buf);
        return uuid;
    }

    if (dump.starts_with("\'"))
    {
        String str;
        ReadBufferFromString buf{dump};
        readQuoted(str, buf);
        return str;
    }

    prefix = std::string_view{"Array_["};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        Array array;
        while (tail != "]")
        {
            size_t separator = tail.find_first_of(",]");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != "]")
                show_error();
            array.push_back(Field::restoreFromDump(element));
        }
        return array;
    }

    prefix = std::string_view{"Tuple_("};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        Tuple tuple;
        while (tail != ")")
        {
            size_t separator = tail.find_first_of(",)");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != ")")
                show_error();
            tuple.push_back(Field::restoreFromDump(element));
        }
        return tuple;
    }

    prefix = std::string_view{"Map_("};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        Map map;
        while (tail != ")")
        {
            size_t separator = tail.find_first_of(",)");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != ")")
                show_error();
            map.push_back(Field::restoreFromDump(element));
        }
        return map;
    }

    prefix = std::string_view{"AggregateFunctionState_("};
    if (dump.starts_with(prefix))
    {
        std::string_view after_prefix = dump.substr(prefix.length());
        size_t comma = after_prefix.find(',');
        size_t end = after_prefix.find(')', comma + 1);
        if ((comma == std::string_view::npos) || (end != after_prefix.length() - 1))
            show_error();
        std::string_view name_view = after_prefix.substr(0, comma);
        std::string_view data_view = after_prefix.substr(comma + 1, end - comma - 1);
        trim(name_view);
        trim(data_view);
        ReadBufferFromString name_buf{name_view};
        ReadBufferFromString data_buf{data_view};
        AggregateFunctionStateData res;
        readQuotedString(res.name, name_buf);
        readQuotedString(res.data, data_buf);
        return res;
    }

    show_error();
    __builtin_unreachable();
}

inline void writeText(const Null &, WriteBuffer & buf)
{
    writeText(std::string("Null"), buf);
}

String toString(const Field & x)
{
    return Field::dispatch(
        [] (const auto & value)
        {
            // Use explicit type to prevent implicit construction of Field and
            // infinite recursion into toString<Field>.
            return toString<decltype(value)>(value);
        },
        x);
}

}
