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
#include <IO/DoubleConverter.h>

namespace RK
{
template <bool emit_decimal_point>
const double_conversion::DoubleToStringConverter & DoubleConverter<emit_decimal_point>::instance()
{
    static const double_conversion::DoubleToStringConverter instance{
        DoubleToStringConverterFlags<emit_decimal_point>::flags, "inf", "nan", 'e', -6, 21, 6, 1};

    return instance;
}

template class DoubleConverter<true>;
template class DoubleConverter<false>;
}
