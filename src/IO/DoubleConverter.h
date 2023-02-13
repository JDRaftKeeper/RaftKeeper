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

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdouble-promotion"
#endif

#include <double-conversion/double-conversion.h>
#include <boost/noncopyable.hpp>

#ifdef __clang__
#pragma clang diagnostic pop
#endif


namespace RK
{

template <bool emit_decimal_point> struct DoubleToStringConverterFlags
{
    static constexpr auto flags = double_conversion::DoubleToStringConverter::NO_FLAGS;
};

template <> struct DoubleToStringConverterFlags<true>
{
    static constexpr auto flags = double_conversion::DoubleToStringConverter::EMIT_TRAILING_DECIMAL_POINT;
};

template <bool emit_decimal_point>
class DoubleConverter : private boost::noncopyable
{
    DoubleConverter(const DoubleConverter &) = delete;
    DoubleConverter & operator=(const DoubleConverter &) = delete;

    DoubleConverter() = default;

public:
    /// Sign (1 byte) + DigitsBeforePoint + point (1 byte) + DigitsAfterPoint + zero byte.
    /// See comment to DoubleToStringConverter::ToFixed method for explanation.
    static constexpr auto MAX_REPRESENTATION_LENGTH =
            1 + double_conversion::DoubleToStringConverter::kMaxFixedDigitsBeforePoint +
            1 + double_conversion::DoubleToStringConverter::kMaxFixedDigitsAfterPoint + 1;
    using BufferType = char[MAX_REPRESENTATION_LENGTH];

    static const double_conversion::DoubleToStringConverter & instance();
};

}
