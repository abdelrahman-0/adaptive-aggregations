#pragma once

#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "utils.h"

class Logger {
  private:
    std::vector<std::string> header{};
    std::vector<std::string> row{};
    bool print_header{};
    std::mutex mutex{};

  public:
    explicit Logger(bool print_header) : print_header(print_header) {};

    ~Logger()
    {
        if (print_header) {
            logln<','>(header);
        }
        logln<','>(row);
    }

    template <typename T>
    requires std::is_same_v<std::string, T> or requires(T t) { std::to_string(t); }
    auto& log(const std::string& param, T&& val)
    {
        std::unique_lock<std::mutex> _{mutex};
        header.push_back(param);
        if constexpr (std::is_same_v<std::string, T>) {
            row.push_back(val);
        }
        else {
            row.push_back(std::to_string(val));
        }
        return *this;
    }
};
