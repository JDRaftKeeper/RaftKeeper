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
#include <IO/WriteBufferValidUTF8.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <string>
#include <iostream>

int main(int, char **)
{
    try
    {
        std::string test1 = R"(kjhsgdfkjhg2378rtzgvxkz877%^&^*%&^*&*)";
        std::string test2 = R"({"asd" = "qw1","qwe24" = "3asd"})";
        test2[test2.find('1')] = char(127 + 64);
        test2[test2.find('2')] = char(127 + 64 + 32);
        test2[test2.find('3')] = char(127 + 64 + 32 + 16);
        test2[test2.find('4')] = char(127 + 64 + 32 + 16 + 8);

        std::string str;
        {
            RK::WriteBufferFromString str_buf(str);
            {
                RK::WriteBufferValidUTF8 utf_buf(str_buf, true, "-");
                RK::writeEscapedString(test1, utf_buf);
            }
        }
        std::cout << str << std::endl;

        str = "";
        {
            RK::WriteBufferFromString str_buf(str);
            {
                RK::WriteBufferValidUTF8 utf_buf(str_buf, true, "-");
                RK::writeEscapedString(test2, utf_buf);
            }
        }
        std::cout << str << std::endl;
    }
    catch (const RK::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
