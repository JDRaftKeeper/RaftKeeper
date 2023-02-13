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
#include <Common/PipeFDs.h>
#include <poll.h>
#include <iostream>
#include <thread>
#include <unistd.h>


using namespace RK;

int main(int, char**)
{
    PipeFDs pipe;
    pipe.setNonBlockingReadWrite();
    pollfd poll_buf[1];
    poll_buf[0].fd = pipe.fds_rw[0];
    poll_buf[0].events = POLLIN;

    std::thread t([&pipe]()
    {
        size_t i = 0;
//        while (true)
        {
            sleep(10);
            int response_fd = pipe.fds_rw[1];
            int8_t single_byte = 1;
            [[maybe_unused]] int result = write(response_fd, &single_byte, sizeof(single_byte));
            if (i > 999999)
//                break;
            i++;
        }
    });

    size_t i = 0;
//    while (true)
//    {
        int rc = 0;
        do
        {
            rc = ::poll(poll_buf, 1, -1);
            if (rc < 0)
            {

            }
        } while (rc < 0);

        if (rc >= 1 && poll_buf[0].revents & POLLIN)
            std::cout << "true" << std::endl;
        else
            std::cout << "false" << std::endl;

        if (i > 999999)
//            break;
        i++;
//    }

    t.join();
    return 0;
}
