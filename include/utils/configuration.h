#pragma once

#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>

#include "bench/bench.h"
#include "defaults.h"

namespace adapt {

struct NodeInfo
{
    std::string ip;
    std::string port;
};

class Configuration
{
    nlohmann::json config;

  public:
    Configuration() = delete;

    explicit Configuration(const std::string& config_file_path)
    {
        // parse config file
        ENSURE(std::filesystem::exists(config_file_path));
        std::ifstream config_file{config_file_path};
        config = nlohmann::json::parse(config_file);
    }

    [[nodiscard]]
    auto get_coordinator_info() const
    {
        auto info_json = config["coordinator"];
        return NodeInfo{info_json["ip"], info_json["port"]};
    }

    [[nodiscard]]
    auto get_worker_info(u16 worker_id) const
    {
        auto info_json = config["workers"][worker_id];
        return NodeInfo{info_json["ip"], info_json["port"]};
    }

    [[nodiscard]]
    auto get_num_workers() const
    {
        return config["workers"].size();
    }
};

} // namespace adapt
