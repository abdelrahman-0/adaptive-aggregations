#pragma once

#include <array>
#include <boost/core/demangle.hpp>
#include <cstdint>
#include <cstring>
#include <tuple>

#include "defaults.h"
#include "utils/custom_type_traits.h"
#include "utils/utils.h"

template <typename... Attributes>
consteval std::size_t calc_max_num_tuples_per_page(std::size_t page_size) {
    return (page_size - sizeof(std::size_t)) / (sizeof(Attributes) + ...);
}

template <typename T, std::size_t length>
void random_column(std::array<T, length>& column) {
    for (auto& el : column) {
        random(el);
    }
}

template <typename T, std::size_t length>
void print_column(const std::array<T, length>& column, std::integral auto num_tuples) {
    println("column:", boost::core::demangle(typeid(T).name()));
    for (auto i = 0u; i < num_tuples; ++i) {
        print(column[i]);
        print(" | ");
    }
    println();
}

// PAX-style page
template <uint32_t page_size, typename... Attributes>
struct alignas(page_size) Page {
    static constexpr auto max_num_tuples_per_page = calc_max_num_tuples_per_page<Attributes...>(page_size);
    uint32_t num_tuples{0};
    std::tuple<std::array<Attributes, max_num_tuples_per_page>...> columns;

    template <uint32_t other_page_size, typename... OtherAttributes>
    requires(sizeof...(Attributes) == 1 and sizeof...(OtherAttributes) == 1)
    void emplace_back(std::size_t row_idx, const Page<other_page_size, OtherAttributes...>& page) {
        std::get<0>(columns)[num_tuples++] = std::get<0>(page.columns)[row_idx];
    }

    template <uint32_t other_page_size, typename... OtherAttributes,
              typename Indices = std::index_sequence_for<OtherAttributes...>>
    // only allow column-store to row-store (transpose)
    requires(sizeof...(Attributes) == 1 and
             custom_type_traits::is_tuple_v<std::tuple_element_t<0, std::tuple<Attributes...>>>)
    void emplace_back_transposed(std::size_t row_idx, const Page<other_page_size, OtherAttributes...>& page) {
        emplace_back_transposed_helper(row_idx, page, Indices{});
        num_tuples++;
    }

    template <uint32_t other_page_size, typename... OtherAttributes, std::size_t... indexes>
    void emplace_back_transposed_helper(std::size_t row_idx, const Page<other_page_size, OtherAttributes...>& page,
                                        std::index_sequence<indexes...>) {
        std::get<0>(columns)[num_tuples] = std::tuple(std::get<indexes>(page.columns)[row_idx]...);
//        new (&(std::get<0>(columns)[num_tuples])) std::tuple(std::get<indexes>(page.columns)[row_idx]...);
    }

    void clear() { memset(this, 0, page_size); }

    void clear_tuples() { num_tuples = 0; }

    void fill_random() {
        std::apply([](auto&&... args) { ((random_column(args)), ...); }, columns);
    }

    void print_contents() const {
        println<' '>("num tuples:", num_tuples);
        std::apply([this](const auto&... args) { ((print_column(args, num_tuples)), ...); }, columns);
    }

    void print_info() const {
        println<' '>("size of page:", page_size, "( max tuples:", max_num_tuples_per_page, ")");
        println<' '>("schema:", boost::core::demangle(typeid(Attributes).name())...);
    }

    [[nodiscard]] bool full() const { return num_tuples == max_num_tuples_per_page; }

    [[nodiscard]] bool empty() const { return num_tuples == 0; }

    void compress() {}
};

template <uint32_t size, typename... Attributes>
static auto as_bytes(Page<size, Attributes...>* page) {
    return reinterpret_cast<std::byte*>(page);
}
