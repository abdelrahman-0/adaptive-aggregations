#pragma once

#include <string>
#include <utility>
#include <vector>

#include "utils.h"

class Logger {
  private:
    std::vector<std::string> header{};
    std::vector<std::string> row{};

  public:
    Logger() = default;

    ~Logger() {
        logln(header);
        logln(row);
    }

    template <typename T>
    requires std::is_same_v<std::string, T> or requires(T t) { std::to_string(t); }
    void log(const std::string& param, T&& val) {
        header.push_back(param);
        if constexpr (std::is_same_v<std::string, T>) {
            row.push_back(val);
        } else {
            row.push_back(std::to_string(val));
        }
    }
};