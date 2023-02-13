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
#include "configReadClient.h"

#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/File.h>
#include "ConfigProcessor.h"

namespace RK
{
bool configReadClient(Poco::Util::LayeredConfiguration & config, const std::string & home_path)
{
    std::string config_path;
    if (config.has("config-file"))
        config_path = config.getString("config-file");
    else if (Poco::File("./clickhouse-client.xml").exists())
        config_path = "./clickhouse-client.xml";
    else if (!home_path.empty() && Poco::File(home_path + "/.clickhouse-client/config.xml").exists())
        config_path = home_path + "/.clickhouse-client/config.xml";
    else if (Poco::File("/etc/clickhouse-client/config.xml").exists())
        config_path = "/etc/clickhouse-client/config.xml";

    if (!config_path.empty())
    {
        ConfigProcessor config_processor(config_path);
        auto loaded_config = config_processor.loadConfig();
        config.add(loaded_config.configuration);
        return true;
    }
    return false;
}
}
