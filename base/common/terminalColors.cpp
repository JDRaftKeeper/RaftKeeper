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
#include <common/terminalColors.h>


std::string setColor(UInt64 hash)
{
    /// Make a random RGB color that has constant brightness.
    /// https://en.wikipedia.org/wiki/YCbCr

    /// Note that this is darker than the middle relative luminance, see "Gamma_correction" and "Luma_(video)".
    /// It still looks awesome.
    UInt8 y = 128;

    UInt8 cb = hash % 256;
    UInt8 cr = hash / 256 % 256;

    UInt8 r = std::max(0.0, std::min(255.0, y + 1.402 * (cr - 128)));
    UInt8 g = std::max(0.0, std::min(255.0, y - 0.344136 * (cb - 128) - 0.714136 * (cr - 128)));
    UInt8 b = std::max(0.0, std::min(255.0, y + 1.772 * (cb - 128)));

    /// ANSI escape sequence to set 24-bit foreground font color in terminal.
    return "\033[38;2;" + std::to_string(r) + ";" + std::to_string(g) + ";" + std::to_string(b) + "m";
}

const char * setColorForLogPriority(int priority)
{
    if (priority < 1 || priority > 8)
        return "";

    static const char * colors[] =
    {
        "",
        "\033[1;41m",   /// Fatal
        "\033[7;31m",   /// Critical
        "\033[1;31m",   /// Error
        "\033[0;31m",   /// Warning
        "\033[0;33m",   /// Notice
        "\033[1m",      /// Information
        "",             /// Debug
        "\033[2m",      /// Trace
    };

    return colors[priority];
}

const char * resetColor()
{
    return "\033[0m";
}
