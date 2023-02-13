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

#include <map>
#include <tuple>
#include <mutex>
#include <ext/function_traits.h>


/** The simplest cache for a free function.
  * You can also pass a static class method or lambda without captures.
  * The size is unlimited. Values are stored permanently and never evicted.
  * But single record or all cache can be manually dropped.
  * Mutex is used for synchronization.
  * Suitable only for the simplest cases.
  *
  * Usage
  *
  * SimpleCache<decltype(func), &func> func_cached;
  * std::cerr << func_cached(args...);
  */
template <typename F, F* f>
class SimpleCache
{
private:
    using Key = typename function_traits<F>::arguments_decay;
    using Result = typename function_traits<F>::result;

    std::map<Key, Result> cache;
    mutable std::mutex mutex;

public:
    template <typename... Args>
    Result operator() (Args &&... args)
    {
        {
            std::lock_guard lock(mutex);

            Key key{std::forward<Args>(args)...};
            auto it = cache.find(key);

            if (cache.end() != it)
                return it->second;
        }

        /// The calculations themselves are not done under mutex.
        Result res = f(std::forward<Args>(args)...);

        {
            std::lock_guard lock(mutex);

            cache.emplace(std::forward_as_tuple(args...), res);
        }

        return res;
    }

    template <typename... Args>
    void update(Args &&... args)
    {
        Result res = f(std::forward<Args>(args)...);
        {
            std::lock_guard lock(mutex);

            Key key{std::forward<Args>(args)...};
            cache[key] = std::move(res);
        }
    }

    size_t size() const
    {
        std::lock_guard lock(mutex);
        return cache.size();
    }

    void drop()
    {
        std::lock_guard lock(mutex);
        cache.clear();
    }
};
