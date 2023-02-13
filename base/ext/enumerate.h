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

#include <ext/size.h>
#include <type_traits>
#include <utility>
#include <iterator>


/** \brief Provides a wrapper view around a container, allowing to iterate over it's elements and indices.
  *    Allow writing code like shown below:
  *
  *        std::vector<T> v = getVector();
  *        for (const std::pair<const std::size_t, T &> index_and_value : ext::enumerate(v))
  *            std::cout << "element " << index_and_value.first << " is " << index_and_value.second << std::endl;
  */
namespace ext
{
    template <typename It> struct enumerate_iterator
    {
        using traits = typename std::iterator_traits<It>;
        using iterator_category = typename traits::iterator_category;
        using value_type = std::pair<const std::size_t, typename traits::value_type>;
        using difference_type = typename traits::difference_type;
        using reference = std::pair<const std::size_t, typename traits::reference>;

        std::size_t idx;
        It it;

        enumerate_iterator(const std::size_t idx_, It it_) : idx{idx_}, it{it_} {}

        auto operator*() const { return reference(idx, *it); }

        bool operator!=(const enumerate_iterator & other) const { return it != other.it; }

        enumerate_iterator & operator++() { return ++idx, ++it, *this; }
    };

    template <typename Collection> struct enumerate_wrapper
    {
        using underlying_iterator = decltype(std::begin(std::declval<Collection &>()));
        using iterator = enumerate_iterator<underlying_iterator>;

        Collection & collection;

        enumerate_wrapper(Collection & collection_) : collection(collection_) {}

        auto begin() { return iterator(0, std::begin(collection)); }
        auto end() { return iterator(ext::size(collection), std::end(collection)); }
    };

    template <typename Collection> auto enumerate(Collection & collection)
    {
        return enumerate_wrapper<Collection>{collection};
    }

    template <typename Collection> auto enumerate(const Collection & collection)
    {
        return enumerate_wrapper<const Collection>{collection};
    }
}
