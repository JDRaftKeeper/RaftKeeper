/**
 * Copyright 2021-2023 JD.com, Inc.
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
//
// Created by JackyWoo on 2020/4/23.
//

#include <Service/NuRaftStateManager.h>
#include<cstdio>
#include <gtest/gtest.h>

namespace RK
{
using namespace nuraft;

class MockedRaftStateManager : public NuRaftStateManager
{
public:
    MockedRaftStateManager() : NuRaftStateManager() {
        log = &(Poco::Logger::get("RaftStateManager"));
        srv_state_file = "./srv_state_test";
    }

    ~MockedRaftStateManager() override
    {
        remove(srv_state_file.c_str());
    }
};


TEST(RaftStateManager, load_srv_state)
{
    ptr<MockedRaftStateManager> state_manager = cs_new<MockedRaftStateManager>();
    ptr<srv_state> state = cs_new<srv_state>();
    state->set_term(1);
    state->set_voted_for(1);

    state_manager->save_state(*state);
    ptr<srv_state> real = state_manager->read_state();

    ASSERT_EQ(real->get_term(), state->get_term());
    ASSERT_EQ(real->get_voted_for(), state->get_voted_for());

    state_manager->save_state(*state);
    ptr<srv_state> real2 = state_manager->read_state();

    ASSERT_EQ(real2->get_term(), state->get_term());
    ASSERT_EQ(real2->get_voted_for(), state->get_voted_for());
}

}
