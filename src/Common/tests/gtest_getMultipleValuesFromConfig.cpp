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
#include <Common/getMultipleKeysFromConfig.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>


using namespace RK;

TEST(Common, getMultipleValuesFromConfig)
{
    std::istringstream      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        xml_isteam(R"END(<?xml version="1.0"?>
<raftkeeper>
    <first_level>
        <second_level>0</second_level>
        <second_level>1</second_level>
        <second_level>2</second_level>
        <second_level>3</second_level>
    </first_level>
</raftkeeper>)END");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(xml_isteam);
    std::vector<std::string> answer = getMultipleValuesFromConfig(*config, "first_level", "second_level");
    std::vector<std::string> right_answer = {"0", "1", "2", "3"};
    EXPECT_EQ(answer, right_answer);
}
