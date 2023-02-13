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
#include <thread>

#include <Common/ShellCommand.h>
#include <Common/Exception.h>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/copyData.h>

/** This example shows how we can proxy stdin to ShellCommand and obtain stdout in streaming fashion. */

int main(int argc, char ** argv)
try
{
    using namespace RK;

    if (argc < 2)
    {
        std::cerr << "Usage: shell_command_inout 'command...' < in > out\n";
        return 1;
    }

    auto command = ShellCommand::execute(argv[1]);

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);
    WriteBufferFromFileDescriptor err(STDERR_FILENO);

    /// Background thread sends data and foreground thread receives result.

    std::thread thread([&]
    {
        copyData(in, command->in);
        command->in.close();
    });

    copyData(command->out, out);
    copyData(command->err, err);

    thread.join();
    return 0;
}
catch (...)
{
    std::cerr << RK::getCurrentExceptionMessage(true) << '\n';
    throw;
}
