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
#pragma once

#include <string>
#include <common/types.h>

#define UNICODE_BAR_CHAR_SIZE (strlen("█"))


/** Allows you to draw a unicode-art bar whose width is displayed with a resolution of 1/8 character.
  */
namespace UnicodeBar
{
    double getWidth(double x, double min, double max, double max_width);
    size_t getWidthInBytes(double width);

    /// In `dst` there must be a space for barWidthInBytes(width) characters and a trailing zero.
    void render(double width, char * dst);
    std::string render(double width);
}
