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
#include <string>
#include <vector>
#include <common/find_symbols.h>
#include <gtest/gtest.h>


TEST(FindSymbols, SimpleTest)
{
    std::string s = "Hello, world! Goodbye...";
    const char * begin = s.data();
    const char * end = s.data() + s.size();

    ASSERT_EQ(find_first_symbols<'a'>(begin, end), end);
    ASSERT_EQ(find_first_symbols<'e'>(begin, end), begin + 1);
    ASSERT_EQ(find_first_symbols<'.'>(begin, end), begin + 21);
    ASSERT_EQ(find_first_symbols<' '>(begin, end), begin + 6);
    ASSERT_EQ(find_first_symbols<'H'>(begin, end), begin);
    ASSERT_EQ((find_first_symbols<'a', 'e'>(begin, end)), begin + 1);

    ASSERT_EQ(find_last_symbols_or_null<'a'>(begin, end), nullptr);
    ASSERT_EQ(find_last_symbols_or_null<'e'>(begin, end), end - 4);
    ASSERT_EQ(find_last_symbols_or_null<'.'>(begin, end), end - 1);
    ASSERT_EQ(find_last_symbols_or_null<' '>(begin, end), end - 11);
    ASSERT_EQ(find_last_symbols_or_null<'H'>(begin, end), begin);
    ASSERT_EQ((find_last_symbols_or_null<'a', 'e'>(begin, end)), end - 4);

    {
        std::vector<std::string> vals;
        splitInto<' ', ','>(vals, "hello, world", true);
        ASSERT_EQ(vals, (std::vector<std::string>{"hello", "world"}));
    }

    {
        std::vector<std::string> vals;
        splitInto<' ', ','>(vals, "s String", true);
        ASSERT_EQ(vals, (std::vector<std::string>{"s", "String"}));
    }
}
