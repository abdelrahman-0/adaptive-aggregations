#pragma once

#include <cstdint>
#include <array>
#include <tuple>
#include <boost/core/demangle.hpp>
#include "utils/utils.h"
#include <cstring>

static constexpr std::size_t page_size = 1 << 12;

template <typename... Attributes>
consteval std::size_t calc_max_num_tuples_per_page(){
    return (page_size - sizeof(std::size_t)) / (sizeof(Attributes) + ...);
}

template<typename T, std::size_t length>
void random_column(std::array<T, length>& arr){
    for(auto& el: arr){ random(el); }
}

template<typename T, std::size_t length>
void print_column(const std::array<T, length>& column){
    println("column:", boost::core::demangle(typeid(T).name()));
    if constexpr (std::is_array_v<T>) {
        for(const auto& arr : column){
            for(const auto& el: arr){ print(el); }
            print(" | ");
        }
    } else {
        for(const auto& el: column){ print(el, "| "); }
    }
    println();
}

// PAX-style page
template <typename... Attributes>
struct alignas(page_size) Page {
    static constexpr auto max_num_tuples_per_page = calc_max_num_tuples_per_page<Attributes...>();
    std::tuple<std::array<Attributes, max_num_tuples_per_page>...> columns;
    std::size_t num_tuples{0};

    void clear() {
        memset(this, 0, sizeof(*this));
    }

    void fill_random() {
        std::apply([](auto&... args) {((random_column(args)), ...);}, columns);
    }

    void print_contents() const {
        std::apply([](auto&... args) {((print_column(args)), ...);}, columns);
    }
};

static_assert(sizeof(Page<int32_t>) == page_size);
static_assert(sizeof(Page<int32_t, char[20]>) == page_size);
static_assert(sizeof(Page<int64_t, int64_t, int32_t, unsigned char[4]>) == page_size);
