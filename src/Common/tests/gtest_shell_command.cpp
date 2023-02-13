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
#include <common/types.h>
#include <Common/ShellCommand.h>
#include <IO/copyData.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <chrono>
#include <thread>

#include <gtest/gtest.h>


using namespace RK;


TEST(ShellCommand, Execute)
{
    auto command = ShellCommand::execute("echo 'Hello, world!'");

    std::string res;
    readStringUntilEOF(res, command->out);
    command->wait();

    EXPECT_EQ(res, "Hello, world!\n");
}

TEST(ShellCommand, ExecuteDirect)
{
    auto command = ShellCommand::executeDirect("/bin/echo", {"Hello, world!"});

    std::string res;
    readStringUntilEOF(res, command->out);
    command->wait();

    EXPECT_EQ(res, "Hello, world!\n");
}

TEST(ShellCommand, ExecuteWithInput)
{
    auto command = ShellCommand::execute("cat");

    String in_str = "Hello, world!\n";
    ReadBufferFromString in(in_str);
    copyData(in, command->in);
    command->in.close();

    std::string res;
    readStringUntilEOF(res, command->out);
    command->wait();

    EXPECT_EQ(res, "Hello, world!\n");
}

TEST(ShellCommand, AutoWait)
{
    // <defunct> hunting:
    for (int i = 0; i < 1000; ++i)
    {
        auto command = ShellCommand::execute("echo " + std::to_string(i));
        //command->wait(); // now automatic
    }

    // std::cerr << "inspect me: ps auxwwf\n";
    // std::this_thread::sleep_for(std::chrono::seconds(100));
}
