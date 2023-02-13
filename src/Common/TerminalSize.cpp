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
#include <unistd.h>
#include <sys/ioctl.h>
#include <Common/Exception.h>
#include <Common/TerminalSize.h>
#include <boost/program_options.hpp>


namespace RK::ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

uint16_t getTerminalWidth()
{
    if (isatty(STDIN_FILENO))
    {
        winsize terminal_size {};

        if (ioctl(STDIN_FILENO, TIOCGWINSZ, &terminal_size))
            RK::throwFromErrno("Cannot obtain terminal window size (ioctl TIOCGWINSZ)", RK::ErrorCodes::SYSTEM_ERROR);

        return terminal_size.ws_col;
    }
    return 0;
}

po::options_description createOptionsDescription(const std::string & caption, uint16_t terminal_width)
{
    unsigned line_length = po::options_description::m_default_line_length;
    unsigned min_description_length = line_length / 2;
    std::string longest_option_desc = "--http_native_compression_disable_checksumming_on_decompress";

    line_length = std::max(static_cast<uint16_t>(longest_option_desc.size()), terminal_width);
    min_description_length = std::min(min_description_length, line_length - 2);

    return po::options_description(caption, line_length, min_description_length);
}
