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

#include <vector>

namespace ext
{

/// Moves all arguments starting from the second to the end of the vector.
/// For example, `push_back(vec, a1, a2, a3)` is a more compact way to write
/// `vec.push_back(a1); vec.push_back(a2); vec.push_back(a3);`
/// This function is like boost::range::push_back() but works for noncopyable types too.
template <typename T>
void push_back(std::vector<T> &)
{
}

template <typename T, typename FirstArg, typename... OtherArgs>
void push_back(std::vector<T> & vec, FirstArg && first, OtherArgs &&... other)
{
    vec.reserve(vec.size() + sizeof...(other) + 1);
    vec.emplace_back(std::move(first));
    push_back(vec, std::move(other)...);
}

}
