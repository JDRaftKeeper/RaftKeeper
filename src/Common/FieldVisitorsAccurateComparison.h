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

#include <Core/Field.h>
#include <Core/AccurateComparison.h>
#include <common/demangle.h>
#include <Common/FieldVisitors.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>


namespace RK
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
}

/** More precise comparison, used for index.
  * Differs from Field::operator< and Field::operator== in that it also compares values of different types.
  * Comparison rules are same as in FunctionsComparison (to be consistent with expression evaluation in query).
  */
class FieldVisitorAccurateEquals : public StaticVisitor<bool>
{
public:
    template <typename T, typename U>
    bool operator() (const T & l, const U & r) const
    {
        if constexpr (std::is_same_v<T, Null> || std::is_same_v<U, Null>)
            return std::is_same_v<T, U>;
        else
        {
            if constexpr (std::is_same_v<T, U>)
                return l == r;

            if constexpr (std::is_arithmetic_v<T> && std::is_arithmetic_v<U>)
                return accurate::equalsOp(l, r);

            if constexpr (isDecimalField<T>() && isDecimalField<U>())
                return l == r;

            if constexpr (isDecimalField<T>() && std::is_arithmetic_v<U>)
                return l == DecimalField<Decimal128>(r, 0);

            if constexpr (std::is_arithmetic_v<T> && isDecimalField<U>())
                return DecimalField<Decimal128>(l, 0) == r;

            if constexpr (std::is_same_v<T, String>)
            {
                if constexpr (std::is_same_v<U, UInt128>)
                    return stringToUUID(l) == r;

                if constexpr (std::is_arithmetic_v<U>)
                {
                    ReadBufferFromString in(l);
                    U parsed;
                    readText(parsed, in);
                    return operator()(parsed, r);
                }
            }

            if constexpr (std::is_same_v<U, String>)
            {
                if constexpr (std::is_same_v<T, UInt128>)
                    return l == stringToUUID(r);

                if constexpr (std::is_arithmetic_v<T>)
                {
                    ReadBufferFromString in(r);
                    T parsed;
                    readText(parsed, in);
                    return operator()(l, parsed);
                }
            }
        }

        throw Exception("Cannot compare " + demangle(typeid(T).name()) + " with " + demangle(typeid(U).name()),
            ErrorCodes::BAD_TYPE_OF_FIELD);
    }
};


class FieldVisitorAccurateLess : public StaticVisitor<bool>
{
public:
    template <typename T, typename U>
    bool operator() (const T & l, const U & r) const
    {
        if constexpr (std::is_same_v<T, Null> || std::is_same_v<U, Null>)
            return false;
        else
        {
            if constexpr (std::is_same_v<T, U>)
                return l < r;

            if constexpr (std::is_arithmetic_v<T> && std::is_arithmetic_v<U>)
                return accurate::lessOp(l, r);

            if constexpr (isDecimalField<T>() && isDecimalField<U>())
                return l < r;

            if constexpr (isDecimalField<T>() && std::is_arithmetic_v<U>)
                return l < DecimalField<Decimal128>(r, 0);

            if constexpr (std::is_arithmetic_v<T> && isDecimalField<U>())
                return DecimalField<Decimal128>(l, 0) < r;

            if constexpr (std::is_same_v<T, String>)
            {
                if constexpr (std::is_same_v<U, UInt128>)
                    return stringToUUID(l) < r;

                if constexpr (std::is_arithmetic_v<U>)
                {
                    ReadBufferFromString in(l);
                    U parsed;
                    readText(parsed, in);
                    return operator()(parsed, r);
                }
            }

            if constexpr (std::is_same_v<U, String>)
            {
                if constexpr (std::is_same_v<T, UInt128>)
                    return l < stringToUUID(r);

                if constexpr (std::is_arithmetic_v<T>)
                {
                    ReadBufferFromString in(r);
                    T parsed;
                    readText(parsed, in);
                    return operator()(l, parsed);
                }
            }
        }

        throw Exception("Cannot compare " + demangle(typeid(T).name()) + " with " + demangle(typeid(U).name()),
            ErrorCodes::BAD_TYPE_OF_FIELD);
    }
};

}
