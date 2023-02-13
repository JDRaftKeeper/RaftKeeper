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

#include <type_traits>
#include <utility>

template <auto Val, decltype(Val)... List>
inline constexpr bool static_in_v = std::disjunction_v<std::bool_constant<Val == List>...>;

template <typename Func, typename Arg>
bool func_wrapper(Func && func, Arg && arg)
{
    if constexpr (std::is_void_v<std::invoke_result_t<Func, Arg>>)
    {
        func(arg);
        return false;
    }
    else
        return func(arg);
}

template <typename T, T Begin, typename Func, T... Is>
constexpr bool static_for_impl(Func && f, std::integer_sequence<T, Is...>)
{
    return (func_wrapper(std::forward<Func>(f), std::integral_constant<T, Begin + Is>{}) || ...);
}

template <auto Begin, decltype(Begin) End, typename Func>
constexpr bool static_for(Func && f)
{
    using T = decltype(Begin);
    return static_for_impl<T, Begin>(std::forward<Func>(f), std::make_integer_sequence<T, End - Begin>{});
}
