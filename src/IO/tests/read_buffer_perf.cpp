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

#include <iostream>

#include <common/types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>


int main(int, char **)
{
    try
    {
        RK::ReadBufferFromFile in("test");

        RK::Int64 a = 0;
        RK::Float64 b = 0;
        RK::String c, d;

        size_t i = 0;
        while (!in.eof())
        {
            RK::readIntText(a, in);
            in.ignore();

            RK::readFloatText(b, in);
            in.ignore();

            RK::readEscapedString(c, in);
            in.ignore();

            RK::readQuotedString(d, in);
            in.ignore();

            ++i;
        }

        std::cout << a << ' ' << b << ' ' << c << '\t' << '\'' << d << '\'' << std::endl;
        std::cout << i << std::endl;
    }
    catch (const RK::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
