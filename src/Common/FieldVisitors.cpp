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
#include <Core/UUID.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/FieldVisitors.h>
#include <Common/SipHash.h>


namespace RK
{
namespace ErrorCodes
{
    extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
}

template <typename T>
static inline String formatQuoted(T x)
{
    WriteBufferFromOwnString wb;
    writeQuoted(x, wb);
    return wb.str();
}

template <typename T>
static inline String formatQuotedWithPrefix(T x, const char * prefix)
{
    WriteBufferFromOwnString wb;
    wb.write(prefix, strlen(prefix));
    writeQuoted(x, wb);
    return wb.str();
}

String FieldVisitorDump::operator() (const Null &) const { return "NULL"; }
String FieldVisitorDump::operator() (const UInt64 & x) const { return formatQuotedWithPrefix(x, "UInt64_"); }
String FieldVisitorDump::operator() (const Int64 & x) const { return formatQuotedWithPrefix(x, "Int64_"); }
String FieldVisitorDump::operator() (const Float64 & x) const { return formatQuotedWithPrefix(x, "Float64_"); }
String FieldVisitorDump::operator() (const UInt256 & x) const { return formatQuotedWithPrefix(x, "UInt256_"); }
String FieldVisitorDump::operator() (const Int256 & x) const { return formatQuotedWithPrefix(x, "Int256_"); }
String FieldVisitorDump::operator() (const Int128 & x) const { return formatQuotedWithPrefix(x, "Int128_"); }
String FieldVisitorDump::operator() (const UInt128 & x) const { return formatQuotedWithPrefix(UUID(x), "UUID_"); }


String FieldVisitorDump::operator() (const String & x) const
{
    WriteBufferFromOwnString wb;
    writeQuoted(x, wb);
    return wb.str();
}

String FieldVisitorDump::operator() (const Array & x) const
{
    WriteBufferFromOwnString wb;

    wb << "Array_[";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorDump::operator() (const Tuple & x) const
{
    WriteBufferFromOwnString wb;

    wb << "Tuple_(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorDump::operator() (const Map & x) const
{
    WriteBufferFromOwnString wb;

    wb << "Map_(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorDump::operator() (const AggregateFunctionStateData & x) const
{
    WriteBufferFromOwnString wb;
    wb << "AggregateFunctionState_(";
    writeQuoted(x.name, wb);
    wb << ", ";
    writeQuoted(x.data, wb);
    wb << ')';
    return wb.str();
}

/** In contrast to writeFloatText (and writeQuoted),
  *  even if number looks like integer after formatting, prints decimal point nevertheless (for example, Float64(1) is printed as 1.).
  * - because resulting text must be able to be parsed back as Float64 by query parser (otherwise it will be parsed as integer).
  *
  * Trailing zeros after decimal point are omitted.
  *
  * NOTE: Roundtrip may lead to loss of precision.
  */
static String formatFloat(const Float64 x)
{
    DoubleConverter<true>::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    const auto result = DoubleConverter<true>::instance().ToShortest(x, &builder);

    if (!result)
        throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

    return { buffer, buffer + builder.position() };
}


String FieldVisitorToString::operator() (const Null &) const { return "NULL"; }
String FieldVisitorToString::operator() (const UInt64 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Int64 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Float64 & x) const { return formatFloat(x); }
String FieldVisitorToString::operator() (const String & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Int128 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const UInt128 & x) const { return formatQuoted(UUID(x)); }
String FieldVisitorToString::operator() (const AggregateFunctionStateData & x) const
{
    return formatQuoted(x.data);
}
String FieldVisitorToString::operator() (const UInt256 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Int256 & x) const { return formatQuoted(x); }

String FieldVisitorToString::operator() (const Array & x) const
{
    WriteBufferFromOwnString wb;

    wb << '[';
    for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb.write(", ", 2);
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorToString::operator() (const Tuple & x) const
{
    WriteBufferFromOwnString wb;

    wb << '(';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorToString::operator() (const Map & x) const
{
    WriteBufferFromOwnString wb;

    wb << '(';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}


void FieldVisitorWriteBinary::operator() (const Null &, WriteBuffer &) const { }
void FieldVisitorWriteBinary::operator() (const UInt64 & x, WriteBuffer & buf) const { RK::writeVarUInt(x, buf); }
void FieldVisitorWriteBinary::operator() (const Int64 & x, WriteBuffer & buf) const { RK::writeVarInt(x, buf); }
void FieldVisitorWriteBinary::operator() (const Float64 & x, WriteBuffer & buf) const { RK::writeFloatBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const String & x, WriteBuffer & buf) const { RK::writeStringBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const UInt128 & x, WriteBuffer & buf) const { RK::writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const Int128 & x, WriteBuffer & buf) const { RK::writeVarInt(x, buf); }
void FieldVisitorWriteBinary::operator() (const UInt256 & x, WriteBuffer & buf) const { RK::writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const Int256 & x, WriteBuffer & buf) const { RK::writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const AggregateFunctionStateData & x, WriteBuffer & buf) const
{
    RK::writeStringBinary(x.name, buf);
    RK::writeStringBinary(x.data, buf);
}

void FieldVisitorWriteBinary::operator() (const Array & x, WriteBuffer & buf) const
{
    const size_t size = x.size();
    RK::writeBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        const UInt8 type = x[i].getType();
        RK::writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { RK::FieldVisitorWriteBinary()(value, buf); }, x[i]);
    }
}

void FieldVisitorWriteBinary::operator() (const Tuple & x, WriteBuffer & buf) const
{
    const size_t size = x.size();
    RK::writeBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        const UInt8 type = x[i].getType();
        RK::writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { RK::FieldVisitorWriteBinary()(value, buf); }, x[i]);
    }
}


void FieldVisitorWriteBinary::operator() (const Map & x, WriteBuffer & buf) const
{
    const size_t size = x.size();
    RK::writeBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        const UInt8 type = x[i].getType();
        writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { RK::FieldVisitorWriteBinary()(value, buf); }, x[i]);
    }
}


FieldVisitorHash::FieldVisitorHash(SipHash & hash_) : hash(hash_) {}

void FieldVisitorHash::operator() (const Null &) const
{
    UInt8 type = Field::Types::Null;
    hash.update(type);
}

void FieldVisitorHash::operator() (const UInt64 & x) const
{
    UInt8 type = Field::Types::UInt64;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const UInt128 & x) const
{
    UInt8 type = Field::Types::UInt128;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const Int64 & x) const
{
    UInt8 type = Field::Types::Int64;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const Int128 & x) const
{
    UInt8 type = Field::Types::Int128;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const Float64 & x) const
{
    UInt8 type = Field::Types::Float64;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const String & x) const
{
    UInt8 type = Field::Types::String;
    hash.update(type);
    hash.update(x.size());
    hash.update(x.data(), x.size());
}

void FieldVisitorHash::operator() (const Tuple & x) const
{
    UInt8 type = Field::Types::Tuple;
    hash.update(type);
    hash.update(x.size());

    for (const auto & elem : x)
        applyVisitor(*this, elem);
}

void FieldVisitorHash::operator() (const Map & x) const
{
    UInt8 type = Field::Types::Map;
    hash.update(type);
    hash.update(x.size());

    for (const auto & elem : x)
        applyVisitor(*this, elem);
}

void FieldVisitorHash::operator() (const Array & x) const
{
    UInt8 type = Field::Types::Array;
    hash.update(type);
    hash.update(x.size());

    for (const auto & elem : x)
        applyVisitor(*this, elem);
}

void FieldVisitorHash::operator() (const AggregateFunctionStateData & x) const
{
    UInt8 type = Field::Types::AggregateFunctionState;
    hash.update(type);
    hash.update(x.name.size());
    hash.update(x.name.data(), x.name.size());
    hash.update(x.data.size());
    hash.update(x.data.data(), x.data.size());
}

void FieldVisitorHash::operator() (const UInt256 & x) const
{
    UInt8 type = Field::Types::UInt256;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const Int256 & x) const
{
    UInt8 type = Field::Types::Int256;
    hash.update(type);
    hash.update(x);
}

}
