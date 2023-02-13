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
#include <iostream>
#include <stdexcept>

#include <common/LocalDateTime.h>


void fillStackWithGarbage()
{
    volatile uint64_t a = 0xAABBCCDDEEFF0011ULL;
    volatile uint64_t b = 0x2233445566778899ULL;
    std::cout << a + b << '\n';
}

void checkComparison()
{
    LocalDateTime a("2018-07-18 01:02:03");
    LocalDateTime b("2018-07-18 01:02:03");

    if (a != b)
        throw std::runtime_error("Test failed");
}


int main(int, char **)
{
    fillStackWithGarbage();
    checkComparison();
    return 0;
}
