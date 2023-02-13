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
#include <cmath>

#include <Common/formatReadable.h>
#include <IO/DoubleConverter.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace RK
{
    namespace ErrorCodes
    {
        extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
    }
}

static void formatReadable(double size, RK::WriteBuffer & out, int precision, const char ** units, size_t units_size, double delimiter)
{
    size_t i = 0;
    for (; i + 1 < units_size && fabs(size) >= delimiter; ++i)
        size /= delimiter;

    RK::DoubleConverter<false>::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    const auto result = RK::DoubleConverter<false>::instance().ToFixed(size, precision, &builder);

    if (!result)
        throw RK::Exception("Cannot print float or double number", RK::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

    out.write(buffer, builder.position());
    writeCString(units[i], out);
}


void formatReadableSizeWithBinarySuffix(double value, RK::WriteBuffer & out, int precision)
{
    const char * units[] = {" B", " KiB", " MiB", " GiB", " TiB", " PiB", " EiB", " ZiB", " YiB"};
    formatReadable(value, out, precision, units, sizeof(units) / sizeof(units[0]), 1024);
}

std::string formatReadableSizeWithBinarySuffix(double value, int precision)
{
    RK::WriteBufferFromOwnString out;
    formatReadableSizeWithBinarySuffix(value, out, precision);
    return out.str();
}


void formatReadableSizeWithDecimalSuffix(double value, RK::WriteBuffer & out, int precision)
{
    const char * units[] = {" B", " KB", " MB", " GB", " TB", " PB", " EB", " ZB", " YB"};
    formatReadable(value, out, precision, units, sizeof(units) / sizeof(units[0]), 1000);
}

std::string formatReadableSizeWithDecimalSuffix(double value, int precision)
{
    RK::WriteBufferFromOwnString out;
    formatReadableSizeWithDecimalSuffix(value, out, precision);
    return out.str();
}


void formatReadableQuantity(double value, RK::WriteBuffer & out, int precision)
{
    const char * units[] = {"", " thousand", " million", " billion", " trillion", " quadrillion"};
    formatReadable(value, out, precision, units, sizeof(units) / sizeof(units[0]), 1000);
}

std::string formatReadableQuantity(double value, int precision)
{
    RK::WriteBufferFromOwnString out;
    formatReadableQuantity(value, out, precision);
    return out.str();
}
