#include <list>
#include <iostream>
#include <boost/program_options.hpp>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <regex>
#include <Common/ThreadPool.h>
#include <random>


void filterAndSortQueueNodes(std::vector<std::string> & all_nodes)
{
    std::sort(all_nodes.begin(), all_nodes.end());
}

static std::string getBaseName(const String & path)
{
    size_t basename_start = path.rfind('/');
    return std::string{&path[basename_start + 1], path.length() - basename_start - 1};
}

static std::string src_address;

static std::string dest_address;

static std::vector<zkutil::ZooKeeperPtr> src_zookeeper_pool;

static std::vector<zkutil::ZooKeeperPtr> dest_zookeeper_pool;

static std::mutex src_zookeeper_mutex;

static std::mutex dest_zookeeper_mutex;


zkutil::ZooKeeperPtr getSrcZooKeeper()
{
    std::random_device rd;  //Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> distrib(0, 19);

    size_t index = distrib(gen);

    std::lock_guard lock(src_zookeeper_mutex);
    zkutil::ZooKeeperPtr zookeeper = src_zookeeper_pool[index];

    if (zookeeper->expired())
    {
        src_zookeeper_pool.erase(src_zookeeper_pool.begin() + index);
        zookeeper = zookeeper->startNewSession();
        src_zookeeper_pool.push_back(zookeeper);
    }

    return zookeeper;
}

zkutil::ZooKeeperPtr getDestZooKeeper()
{
    std::random_device rd;  //Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> distrib(0, 19);

    size_t index = distrib(gen);

    std::lock_guard lock(dest_zookeeper_mutex);
    zkutil::ZooKeeperPtr zookeeper = dest_zookeeper_pool[index];

    if (zookeeper->expired())
    {
        dest_zookeeper_pool.erase(dest_zookeeper_pool.begin() + index);
        zookeeper = zookeeper->startNewSession();
        dest_zookeeper_pool.push_back(zookeeper);
    }

    return zookeeper;
}

int copy(std::shared_ptr<ThreadPool> thread_pool, std::string initial_path)
{
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

    std::list<std::pair<std::string, std::future<Coordination::ListResponse>>> list_futures;
    try
    {
        list_futures.emplace_back(initial_path, getSrcZooKeeper()->asyncGetChildren(initial_path));

        for (auto it = list_futures.begin(); it != list_futures.end(); ++it)
        {
            Coordination::ListResponse response;

            try
            {
                response = it->second.get();
            }
            catch (const Coordination::Exception & e)
            {
                std::cout << "response get() error," << Coordination::errorMessage(e.code) << ", "<< it->first << ", if error is not ZNONODE, will retry" << std::endl;
                if (e.code == Coordination::Error::ZNONODE)
                {
                    continue;
                }
                else if (Coordination::isHardwareError(e.code))
                {
                    list_futures.emplace_back(it->first, getSrcZooKeeper()->asyncGetChildren(it->first));
                    continue;
                }
                else
                {
                    list_futures.emplace_back(it->first, getSrcZooKeeper()->asyncGetChildren(it->first));
                    continue;
                }
            }
            catch (...)
            {
                std::cout << "response get() error, " << it->first << ", will retry" << std::endl;
                list_futures.emplace_back(it->first, getSrcZooKeeper()->asyncGetChildren(it->first));
                continue;
            }

            auto path = it->first;
            auto create = [&, response, path]() mutable
            {
                try
                {
                    Coordination::Stat stat;
                    const auto & data = getSrcZooKeeper()->get(path, &stat);
                    if (!stat.ephemeralOwner)
                    {
                        if (is_sequential(path))
                        {
                            const String & path_created = getDestZooKeeper()->create(
                                path.substr(0, path.size() - 10), data, zkutil::CreateMode::PersistentSequential);
                            if (path_created != path)
                            {
                                //    throw; // TODO Error
                                //    std::cout << it->first << '\t' << "create: " << it->first.substr(0, it->first.size() - 10) << ", path_created: " << path_created << std::endl;
                            }
                        }
                        else
                        {
                            getDestZooKeeper()->create(path, data, zkutil::CreateMode::Persistent);
                        }

                        if (!response.names.empty())
                        {
                            filterAndSortQueueNodes(response.names);

                            if (is_sequential(response.names[0]))
                            {
    //                            std::cout << "setSeqNum, " << path << '\t' << response.names[0].substr(response.names[0].size() - 10, 10)
    //                                      << std::endl;
                                getDestZooKeeper()->setSeqNum(
                                    path,
                                    std::stoi(
                                        response.names[0].substr(response.names[0].size() - 10, 10))); // TODO set seq_num for it->first znode
                            }
                        }
                    }
                }
                catch (const Coordination::Exception & e)
                {
                    if (e.code != Coordination::Error::ZNODEEXISTS && e.code != Coordination::Error::ZNONODE)
                    {
                        std::cout << "create error Coordination::Exception:" << Coordination::errorMessage(e.code) << ", "<< path << std::endl;
                        throw e;
                    }
                }
                catch (...)
                {
                    std::cout << "create error, " << path << '\t' << response.stat.numChildren << '\t' << response.stat.dataLength << std::endl;
                    throw;
                }
            };

            try
            {
                create();
            }
            catch (...)
            {
                try
                {
                    std::cout << path << '\t' << "retry 1st" << std::endl;
                    create();
                }
                catch (...)
                {
                    std::cout << path << '\t' << "retry 2st" << std::endl;
                    create();
                }
            }

            for (const auto & name : response.names)
            {
                std::string child_path = it->first == "/" ? it->first + name : it->first + '/' + name;

                /// parallel
                int num = std::count(child_path.begin(),child_path.end(),'/');
                if ((num == 5 || num == 9) && !is_sequential(child_path))
                {
                    try
                    {
                        thread_pool->trySchedule([thread_pool, child_path]() { copy(thread_pool, child_path); });
                    }
                    catch (...)
                    {
                        std::cout << "trySchedule " << child_path << " error, will retry." << std::endl;
                        thread_pool->trySchedule([thread_pool, child_path]() { copy(thread_pool, child_path); });
                    }
                }
                else
                {
                    try
                    {
                        list_futures.emplace_back(child_path, getSrcZooKeeper()->asyncGetChildren(child_path));
                    }
                    catch (...)
                    {
                        std::cout << "child_path asyncGetChildren error, retry " << child_path << std::endl;
                        list_futures.emplace_back(child_path, getSrcZooKeeper()->asyncGetChildren(child_path));
                    }
                }
            }
        }
    }
    catch (...)
    {
        std::cout << "initial_path asyncGetChildren error, retry " << initial_path << std::endl;
        copy(thread_pool, initial_path);
    }
    return 0;
}


/**
 * run example: ./zookeeper-service-copier -a 10.203.46.164:5102 -d 10.203.24.118:5112 -p /
 *
 */
int main(int argc, char ** argv)
{
    try
    {
        boost::program_options::options_description desc("Allowed options");
        desc.add_options()("help,h", "produce help message")(
            "address,a",
            boost::program_options::value<std::string>()->required(),
            "addresses of src ZooKeeper instances, comma separated. Example: example01e.yandex.ru:2181")(
            "dest,d",
            boost::program_options::value<std::string>()->required(),
            "addresses of dest ZooKeeper instances, comma separated. Example: example01e.yandex.ru:2181")(
            "path,p", boost::program_options::value<std::string>()->default_value("/"), "where to start");

        boost::program_options::variables_map options;
        boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

        if (options.count("help"))
        {
            std::cout << "Dump paths of all nodes in ZooKeeper." << std::endl;
            std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
            std::cout << desc << std::endl;
            return 1;
        }

        src_address = options.at("address").as<std::string>();
        dest_address = options.at("dest").as<std::string>();

        for (size_t i = 0; i < 20; ++i)
        {
            src_zookeeper_pool.emplace_back(std::make_shared<zkutil::ZooKeeper>(options.at("address").as<std::string>(), "", 90000, 30000));
            dest_zookeeper_pool.emplace_back(std::make_shared<zkutil::ZooKeeper>(options.at("dest").as<std::string>(), "", 90000, 30000));
        }

        std::shared_ptr<ThreadPool> thread_pool = std::make_shared<ThreadPool>(64, 64, 0);
        std::string initial_path = options.at("path").as<std::string>();

        copy(thread_pool, initial_path);

        /// wait thread finished
        thread_pool->wait();

        /// free zookeeper client
        src_zookeeper_pool.clear();
        dest_zookeeper_pool.clear();

        std::cout << "Finished." << std::endl;
        return 0;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
        return 1;
    }
}
