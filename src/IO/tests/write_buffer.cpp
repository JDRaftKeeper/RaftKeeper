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
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromOStream.h>


int main(int, char **)
{
    try
    {
        RK::Int64 a = -123456;
        RK::Float64 b = 123.456;
        RK::String c = "вася пе\tтя";
        RK::String d = "'xyz\\";

        std::stringstream s;    // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        s.exceptions(std::ios::failbit);

        {
            RK::WriteBufferFromOStream out(s);

            RK::writeIntText(a, out);
            RK::writeChar(' ', out);

            RK::writeFloatText(b, out);
            RK::writeChar(' ', out);

            RK::writeEscapedString(c, out);
            RK::writeChar('\t', out);

            RK::writeQuotedString(d, out);
            RK::writeChar('\n', out);
        }

        std::cout << s.str();
    }
    catch (const RK::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
