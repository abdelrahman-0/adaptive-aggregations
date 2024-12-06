#pragma once
/* --------------------------------------- */
#include <mutex>
#include <string>
#include <utility>
#include <vector>
/* --------------------------------------- */
#include "utils/utils.h"
/* --------------------------------------- */
class Logger {
    std::vector<std::string> header{};
    std::vector<std::string> row{};
    std::mutex mutex{};
    bool print_header{};
    bool csv_output{};
    /* --------------------------------------- */
    template <typename T>
    static std::string normalize(const T& val)
    {
        if constexpr (std::is_same_v<std::string, T>) {
            return val;
        }
        else {
            return std::to_string(val);
        }
    }

  public:
    explicit Logger(bool print_header, bool csv_output) : print_header(print_header), csv_output(csv_output) {};
    /* --------------------------------------- */
    ~Logger()
    {
        if (csv_output) {
            if (print_header) {
                logln<','>(header);
            }
            logln<','>(row);
        }
        else {
            u64 max_header_len = std::max_element(header.begin(), header.end(), [](const auto& s1, const auto& s2) { return s1.size() < s2.size(); })->size();
            for (u64 j : range(header.size())) {
                auto space_to_leave = max_header_len - header[j].size() + 1;
                logln(header[j], ":", std::string(space_to_leave, ' '), row[j]);
            }
        }
    }
    /* --------------------------------------- */
    template <typename... T>
    requires(std::is_same_v<std::string, std::remove_cvref_t<T>> and ...) or ((requires(T... t) { (std::to_string(t), ...); }))
    decltype(auto) log(std::string&& param, T&&... vals)
    {
        auto _ = std::unique_lock{mutex};
        using namespace std::string_literals;
        header.push_back(std::move(param));
        auto str = (... + (normalize(vals) + ','));
        // remove trailing comma
        str.pop_back();
        if constexpr (sizeof...(vals)) {
            str = "\""s + str + "\""s;
        }
        row.push_back(str);
        return (*this);
    }
};
