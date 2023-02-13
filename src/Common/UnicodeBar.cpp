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
#include <cstring>
#include <cmath>
#include <string>
#include <common/types.h>
#include <common/arithmeticOverflow.h>
#include <Common/Exception.h>
#include <Common/UnicodeBar.h>
#include <Common/NaNUtils.h>

#include <iostream>


namespace UnicodeBar
{
    double getWidth(double x, double min, double max, double max_width)
    {
        if (isNaN(x) || isNaN(min) || isNaN(max))
            return 0;

        if (x <= min)
            return 0;

        if (x >= max)
            return max_width;

        return (x - min) / (max - min) * max_width;
    }

    size_t getWidthInBytes(double width)
    {
        return ceil(width - 1.0 / 8) * UNICODE_BAR_CHAR_SIZE;
    }

    void render(double width, char * dst)
    {
        size_t floor_width = floor(width);

        for (size_t i = 0; i < floor_width; ++i)
        {
            memcpy(dst, "█", UNICODE_BAR_CHAR_SIZE);
            dst += UNICODE_BAR_CHAR_SIZE;
        }

        size_t remainder = floor((width - floor_width) * 8);

        if (remainder)
        {
            memcpy(dst, &"▏▎▍▌▋▋▊▉"[(remainder - 1) * UNICODE_BAR_CHAR_SIZE], UNICODE_BAR_CHAR_SIZE);
            dst += UNICODE_BAR_CHAR_SIZE;
        }

        *dst = 0;
    }

    std::string render(double width)
    {
        std::string res(getWidthInBytes(width), '\0');
        render(width, res.data());
        return res;
    }
}

