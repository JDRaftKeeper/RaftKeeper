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

#include <Common/typeid_cast.h>

namespace RK
{

/* This base class adds public methods:
 *  -       Derived * as<Derived>()
 *  - const Derived * as<Derived>() const
 *  -       Derived & as<Derived &>()
 *  - const Derived & as<Derived &>() const
 */

template <class Base>
class TypePromotion
{
private:
    /// Need a helper-struct to fight the lack of the function-template partial specialization.
    template <class T, bool is_const, bool is_ref = std::is_reference_v<T>>
    struct CastHelper;

    template <class T>
    struct CastHelper<T, false, true>
    {
        auto & value(Base * ptr) { return typeid_cast<T>(*ptr); }
    };

    template <class T>
    struct CastHelper<T, true, true>
    {
        auto & value(const Base * ptr) { return typeid_cast<std::add_lvalue_reference_t<std::add_const_t<std::remove_reference_t<T>>>>(*ptr); }
    };

    template <class T>
    struct CastHelper<T, false, false>
    {
        auto * value(Base * ptr) { return typeid_cast<T *>(ptr); }
    };

    template <class T>
    struct CastHelper<T, true, false>
    {
        auto * value(const Base * ptr) { return typeid_cast<std::add_const_t<T> *>(ptr); }
    };

public:
    template <class Derived>
    auto as() -> std::invoke_result_t<decltype(&CastHelper<Derived, false>::value), CastHelper<Derived, false>, Base *>
    {
        // TODO: if we do downcast to base type, then just return |this|.
        return CastHelper<Derived, false>().value(static_cast<Base *>(this));
    }

    template <class Derived>
    auto as() const -> std::invoke_result_t<decltype(&CastHelper<Derived, true>::value), CastHelper<Derived, true>, const Base *>
    {
        // TODO: if we do downcast to base type, then just return |this|.
        return CastHelper<Derived, true>().value(static_cast<const Base *>(this));
    }
};

}
