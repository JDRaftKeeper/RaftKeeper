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

#include <cmath>
#include <limits>
#include <type_traits>

#include <common/extended_types.h>


/// To be sure, that this function is zero-cost for non-floating point types.
template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>, bool> isNaN(T x)
{
    return std::isnan(x);
}

template <typename T>
inline std::enable_if_t<!std::is_floating_point_v<T>, bool> isNaN(T)
{
    return false;
}

template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>, bool> isFinite(T x)
{
    return std::isfinite(x);
}

template <typename T>
inline std::enable_if_t<!std::is_floating_point_v<T>, bool> isFinite(T)
{
    return true;
}

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, T> NaNOrZero()
{
    return std::numeric_limits<T>::quiet_NaN();
}

template <typename T>
std::enable_if_t<is_integer_v<T>, T> NaNOrZero()
{
    return T{0};
}

template <typename T>
std::enable_if_t<std::is_class_v<T> && !is_integer_v<T>, T> NaNOrZero()
{
    return T{};
}
