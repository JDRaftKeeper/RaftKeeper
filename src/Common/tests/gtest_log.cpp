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
#include <common/logger_useful.h>
#include <gtest/gtest.h>

#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/NullChannel.h>


TEST(Logger, Log)
{
    Poco::Logger::root().setLevel("none");
    Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::NullChannel>(new Poco::NullChannel()));
    Poco::Logger * log = &Poco::Logger::get("Log");

    /// This test checks that we don't pass this string to fmtlib, because it is the only argument.
    EXPECT_NO_THROW(LOG_INFO(log, "Hello {} World"));
}
