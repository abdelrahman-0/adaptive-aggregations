#pragma once

#include <string>
#include <utility>
#include <vector>

#include "utils.h"

class Logger {
  private:
    std::string header{};
    std::string row{};

  public:
    Logger() = default;

    ~Logger() {
        logln(header);
        logln(row);
    }

    template <typename T>
    requires std::is_same_v<std::string, T> or requires(T t) { std::to_string(t); }
    void log(const std::string& param, T&& val) {
        header += (header.empty() ? "" : ",") + param;
        row += (row.empty() ? "" : ",");
        if constexpr (std::is_same_v<std::string, T>) {
            row += std::string{val};
        } else {
            row += std::to_string(val);
        }
    }
};