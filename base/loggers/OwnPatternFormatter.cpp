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
#include "OwnPatternFormatter.h"

#include <functional>
#include <optional>
#include <sys/time.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/HashTable/Hash.h>
#include <Common/CurrentThread.h>
#include <common/getThreadId.h>
#include <common/terminalColors.h>
#include "Loggers.h"


OwnPatternFormatter::OwnPatternFormatter(const Loggers * loggers_, OwnPatternFormatter::Options options_, bool color_)
    : Poco::PatternFormatter(""), loggers(loggers_), options(options_), color(color_)
{
}


void OwnPatternFormatter::formatExtended(const RK::ExtendedLogMessage & msg_ext, std::string & text)
{
    RK::WriteBufferFromString wb(text);

    const Poco::Message & msg = msg_ext.base;

    /// For syslog: tag must be before message and first whitespace.
    if ((options & ADD_LAYER_TAG) && loggers)
    {
        auto layer = loggers->getLayer();
        if (layer)
        {
            writeCString("layer[", wb);
            RK::writeIntText(*layer, wb);
            writeCString("]: ", wb);
        }
    }

    /// Change delimiters in date for compatibility with old logs.
    RK::writeDateTimeText<'.', ':'>(msg_ext.time_seconds, wb);

    RK::writeChar('.', wb);
    RK::writeChar('0' + ((msg_ext.time_microseconds / 100000) % 10), wb);
    RK::writeChar('0' + ((msg_ext.time_microseconds / 10000) % 10), wb);
    RK::writeChar('0' + ((msg_ext.time_microseconds / 1000) % 10), wb);
    RK::writeChar('0' + ((msg_ext.time_microseconds / 100) % 10), wb);
    RK::writeChar('0' + ((msg_ext.time_microseconds / 10) % 10), wb);
    RK::writeChar('0' + ((msg_ext.time_microseconds / 1) % 10), wb);

    writeCString(" [ ", wb);
    if (color)
        writeString(setColor(intHash64(msg_ext.thread_id)), wb);
    RK::writeIntText(msg_ext.thread_id, wb);
    if (color)
        writeCString(resetColor(), wb);
    writeCString(" ] ", wb);

    /// We write query_id even in case when it is empty (no query context)
    /// just to be convenient for various log parsers.
    writeCString("{", wb);
    if (color)
        writeString(setColor(std::hash<std::string>()(msg_ext.query_id)), wb);
    RK::writeString(msg_ext.query_id, wb);
    if (color)
        writeCString(resetColor(), wb);
    writeCString("} ", wb);

    writeCString("<", wb);
    int priority = static_cast<int>(msg.getPriority());
    if (color)
        writeCString(setColorForLogPriority(priority), wb);
    RK::writeString(getPriorityName(priority), wb);
    if (color)
        writeCString(resetColor(), wb);
    writeCString("> ", wb);
    if (color)
        writeString(setColor(std::hash<std::string>()(msg.getSource())), wb);
    RK::writeString(msg.getSource(), wb);
    if (color)
        writeCString(resetColor(), wb);
    writeCString(": ", wb);
    RK::writeString(msg.getText(), wb);
}

void OwnPatternFormatter::format(const Poco::Message & msg, std::string & text)
{
    formatExtended(RK::ExtendedLogMessage::getFrom(msg), text);
}
