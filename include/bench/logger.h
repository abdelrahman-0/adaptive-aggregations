#pragma once

#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "utils/utils.h"

class Logger {
  private:
    std::vector<std::string> header{};
    std::vector<std::string> row{};
    std::mutex mutex{};
    bool print_header{};
    bool csv_output{};

  public:
    explicit Logger(bool print_header, bool csv_output) : print_header(print_header), csv_output(csv_output) {};

    ~Logger()
    {
        if (csv_output) {
            if (print_header) {
                logln<','>(header);
            }
            logln<','>(row);
        }
        else {
            u64 max_header_len{0};
            std::for_each(header.begin(), header.end(), [&max_header_len](const std::string& str) { max_header_len = std::max(max_header_len, str.size()); });
            for (u64 i : range(header.size())) {
                auto space_to_leave = max_header_len - header[i].size() + 1;
                logln(header[i], ":", std::string(space_to_leave, ' '), row[i]);
            }
        }
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
