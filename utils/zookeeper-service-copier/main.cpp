#include <list>
#include <iostream>
#include <boost/program_options.hpp>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <regex>
#include <Common/ThreadPool.h>


void filterAndSortQueueNodes(std::vector<std::string> & all_nodes)
{
    std::sort(all_nodes.begin(), all_nodes.end());
}

static std::string getBaseName(const String & path)
{
    size_t basename_start = path.rfind('/');
    return std::string{&path[basename_start + 1], path.length() - basename_start - 1};
}

int main(int argc, char ** argv)
{
    try
    {
        boost::program_options::options_description desc("Allowed options");
        desc.add_options()
            ("help,h", "produce help message")
            ("address,a", boost::program_options::value<std::string>()->required(),
                "addresses of src ZooKeeper instances, comma separated. Example: example01e.yandex.ru:2181")
            ("dest,d", boost::program_options::value<std::string>()->required(),
                "addresses of dest ZooKeeper instances, comma separated. Example: example01e.yandex.ru:2181")
            ("path,p", boost::program_options::value<std::string>()->default_value("/"),
                "where to start")
        ;

        boost::program_options::variables_map options;
        boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

        if (options.count("help"))
        {
            std::cout << "Dump paths of all nodes in ZooKeeper." << std::endl;
            std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
            std::cout << desc << std::endl;
            return 1;
        }

        zkutil::ZooKeeperPtr zookeeper = std::make_shared<zkutil::ZooKeeper>(options.at("address").as<std::string>());
        zkutil::ZooKeeperPtr dest_zookeeper = std::make_shared<zkutil::ZooKeeper>(options.at("dest").as<std::string>());

        std::string initial_path = options.at("path").as<std::string>();

        std::list<std::pair<std::string, std::future<Coordination::ListResponse>>> list_futures;
        list_futures.emplace_back(initial_path, zookeeper->asyncGetChildren(initial_path));

        size_t num_reconnects = 0;
        constexpr size_t max_reconnects = 100;

        auto ensure_session = [&]
        {
            if (zookeeper->expired())
            {
                if (num_reconnects == max_reconnects)
                    return false;
                ++num_reconnects;
                std::cerr << "num_reconnects: " << num_reconnects << "\n";
                zookeeper = zookeeper->startNewSession();
            }
            if (dest_zookeeper->expired())
            {
                if (num_reconnects == max_reconnects)
                    return false;
                ++num_reconnects;
                std::cerr << "num_reconnects: " << num_reconnects << "\n";
                dest_zookeeper = dest_zookeeper->startNewSession();
            }
            return true;
        };

        auto is_sequential = [&] (const String & path) -> bool
        {
            const auto & baseName = getBaseName(path);
            if (baseName.size() > 10)
            {
                std::string pattern{ "\\d{10}"};
                std::regex re(pattern);

                return std::regex_match(baseName.substr(baseName.size() - 10, 10), re) && (baseName.substr(baseName.size() - 11, 1) == "-");
            }

            return false;
        };

        auto thread_pool = std::make_shared<ThreadPool>(96);

        for (auto it = list_futures.begin(); it != list_futures.end(); ++it)
        {
            Coordination::ListResponse response;

            try
            {
                response = it->second.get();
            }
            catch (const Coordination::Exception & e)
            {
                if (e.code == Coordination::Error::ZNONODE)
                {
                    continue;
                }
                else if (Coordination::isHardwareError(e.code))
                {
                    /// Reinitialize the session and move the node to the end of the queue for later retry.
                    if (!ensure_session())
                        throw;
                    list_futures.emplace_back(it->first, zookeeper->asyncGetChildren(it->first));
                    continue;
                }
                else
                    throw;
            }

            auto create = [&]()
            {
                try
                {
                    Coordination::Stat stat;
                    const auto & data = zookeeper->get(it->first, &stat);
                    if (!stat.ephemeralOwner)
                    {
                        std::future<Coordination::CreateResponse> future;
                        if (is_sequential(it->first))
                        {
                            future = dest_zookeeper->asyncCreate(it->first.substr(0, it->first.size() - 10), data, zkutil::CreateMode::PersistentSequential);
                            //const String & path_created = dest_zookeeper->create(
                            //    it->first.substr(0, it->first.size() - 10), data, zkutil::CreateMode::PersistentSequential);
                            //if (path_created != it->first)
                            //    throw; // TODO Error
                            //                        std::cout << it->first << '\t' << "create: " << it->first.substr(0, it->first.size() - 10) << ", path_created: " << path_created << '\n';
                        }
                        else
                        {
                            future = dest_zookeeper->asyncCreate(it->first, data, zkutil::CreateMode::Persistent);
                        }

                        if (!response.names.empty())
                        {
                            filterAndSortQueueNodes(response.names);

                            if (is_sequential(response.names[0]))
                            {
                                future.get();
                                std::cout << "setSeqNum, " << it->first << '\t' << response.names[0].substr(response.names[0].size() - 10, 10)
                                          << '\n';
                                dest_zookeeper->setSeqNum(
                                    it->first,
                                    std::stoi(
                                        response.names[0].substr(response.names[0].size() - 10, 10))); // TODO set seq_num for it->first znode
                            }
                        }
                    }
                }
                catch (const Coordination::Exception & e)
                {
                    if (e.code == Coordination::Error::ZNONODE)
                    {
//                        continue;
                    }
                }
                catch (...)
                {
                    std::cout << it->first << '\t' << response.stat.numChildren << '\t' << response.stat.dataLength << '\n';
                    throw;
                }
            };

            if (response.names.empty())
                thread_pool->trySchedule(create);
            else
                create();

            for (const auto & name : response.names)
            {
                std::string child_path = it->first == "/" ? it->first + name : it->first + '/' + name;

                ensure_session();
                list_futures.emplace_back(child_path, zookeeper->asyncGetChildren(child_path));
            }
        }

        return 0;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
        return 1;
    }
}
