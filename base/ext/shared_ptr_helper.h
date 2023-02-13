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

#include <memory>

namespace ext
{

/** Allows to make std::shared_ptr from T with protected constructor.
  *
  * Derive your T class from shared_ptr_helper<T> and add shared_ptr_helper<T> as a friend
  *  and you will have static 'create' method in your class.
  */
template <typename T>
struct shared_ptr_helper
{
    template <typename... TArgs>
    static std::shared_ptr<T> create(TArgs &&... args)
    {
        return std::shared_ptr<T>(new T(std::forward<TArgs>(args)...));
    }
};


template <typename T>
struct is_shared_ptr
{
    static constexpr bool value = false;
};


template <typename T>
struct is_shared_ptr<std::shared_ptr<T>>
{
    static constexpr bool value = true;
};

template <typename T>
inline constexpr bool is_shared_ptr_v = is_shared_ptr<T>::value;
}
