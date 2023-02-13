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
#include <IO/ReadBufferFromString.h>


int main(int, char **)
{
    try
    {
        std::string s = "-123456 123.456 вася пе\\tтя\t'\\'xyz\\\\'";
        RK::ReadBufferFromString in(s);

        RK::Int64 a;
        RK::Float64 b;
        RK::String c, d;

        RK::readIntText(a, in);
        in.ignore();

        RK::readFloatText(b, in);
        in.ignore();

        RK::readEscapedString(c, in);
        in.ignore();

        RK::readQuotedString(d, in);

        std::cout << a << ' ' << b << ' ' << c << '\t' << '\'' << d << '\'' << std::endl;
        std::cout << in.count() << std::endl;
    }
    catch (const RK::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
