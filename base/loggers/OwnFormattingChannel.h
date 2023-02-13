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
#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/FormattingChannel.h>
#include "ExtendedLogChannel.h"
#include "OwnPatternFormatter.h"


namespace RK
{
// Like Poco::FormattingChannel but supports the extended logging interface and log level filter
class OwnFormattingChannel : public Poco::Channel, public ExtendedLogChannel
{
public:
    explicit OwnFormattingChannel(
        Poco::AutoPtr<OwnPatternFormatter> pFormatter_ = nullptr, Poco::AutoPtr<Poco::Channel> pChannel_ = nullptr)
        : pFormatter(std::move(pFormatter_)), pChannel(std::move(pChannel_))
    {
    }

    void setChannel(Poco::AutoPtr<Poco::Channel> pChannel_) { pChannel = std::move(pChannel_); }

    void setLevel(Poco::Message::Priority priority_) { priority = priority_; }

    void open() override
    {
        if (pChannel)
            pChannel->open();
    }

    void close() override
    {
        if (pChannel)
            pChannel->close();
    }

    void log(const Poco::Message & msg) override;
    void logExtended(const ExtendedLogMessage & msg) override;

    ~OwnFormattingChannel() override;

private:
    Poco::AutoPtr<OwnPatternFormatter> pFormatter;
    Poco::AutoPtr<Poco::Channel> pChannel;
    Poco::Message::Priority priority = Poco::Message::PRIO_TRACE;
};

}
