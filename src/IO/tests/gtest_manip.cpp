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
#include <gtest/gtest.h>

#include <string>
#include <type_traits>
#include <common/StringRef.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

using namespace RK;

template <typename T, typename U>
void checkString(const T & str, U manip, const std::string & expected)
{
    WriteBufferFromOwnString buf;

    buf << manip << str;
    EXPECT_EQ(expected, buf.str()) << "str type:" << typeid(str).name();
}

TEST(OperatorsManipTest, EscapingTest)
{
    checkString("Hello 'world'", escape, "Hello \\'world\\'");
    checkString("Hello \\world\\", escape, "Hello \\\\world\\\\"); // NOLINT

    std::string s1 = "Hello 'world'";
    checkString(s1, escape, "Hello \\'world\\'");
    std::string s2 = "Hello \\world\\";
    checkString(s2, escape, "Hello \\\\world\\\\"); // NOLINT

    std::string_view sv1 = s1;
    checkString(sv1, escape, "Hello \\'world\\'");
    std::string_view sv2 = s2;
    checkString(sv2, escape, "Hello \\\\world\\\\"); // NOLINT

    StringRef sr1 = s1;
    checkString(sr1, escape, "Hello \\'world\\'");
    StringRef sr2 = s2;
    checkString(sr2, escape, "Hello \\\\world\\\\"); // NOLINT
}

TEST(OperatorsManipTest, QuouteTest)
{
    checkString("Hello 'world'", quote, "'Hello \\'world\\''");

    std::string s1 = "Hello 'world'";
    checkString(s1, quote, "'Hello \\'world\\''");

    std::string_view sv1 = s1;
    checkString(sv1, quote, "'Hello \\'world\\''");

    StringRef sr1 = s1;
    checkString(sr1, quote, "'Hello \\'world\\''");
}

TEST(OperatorsManipTest, DoubleQuouteTest)
{
    checkString("Hello 'world'", double_quote, "\"Hello 'world'\"");

    std::string s1 = "Hello 'world'";
    checkString(s1, double_quote, "\"Hello 'world'\"");

    std::string_view sv1 = s1;
    checkString(sv1, double_quote, "\"Hello 'world'\"");

    StringRef sr1 = s1;
    checkString(sr1, double_quote, "\"Hello 'world'\"");
}

TEST(OperatorsManipTest, binary)
{
    checkString("Hello", binary, "\x5Hello");

    std::string s1 = "Hello";
    checkString(s1, binary, "\x5Hello");

    std::string_view sv1 = s1;
    checkString(sv1, binary, "\x5Hello");

    StringRef sr1 = s1;
    checkString(sr1, binary, "\x5Hello");
}
