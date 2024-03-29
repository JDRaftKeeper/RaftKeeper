#include <iostream>
#include <common/types.h>
#include <Common/ShellCommand.h>
#include <Common/IO/copyData.h>
#include <Common/IO/WriteBufferFromFileDescriptor.h>
#include <Common/IO/ReadBufferFromString.h>
#include <Common/IO/ReadHelpers.h>

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
