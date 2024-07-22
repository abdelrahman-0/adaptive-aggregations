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
        __log(header);
        __log(row);
    }

    template <typename T>
    requires requires(T t) { std::to_string(t); }
    void log(const std::string& param, T&& val) {
        header += (header.empty() ? "" : ",") + param;
        row += (row.empty() ? "" : ",") + std::to_string(val);
    }
};