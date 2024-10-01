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
consteval std::size_t calc_max_tuples_per_page(std::size_t page_size) {
    return ((page_size - alignof(std::max_align_t)) / (sizeof(Attributes) + ...));
}

template <typename T, std::size_t length>
void random_column(std::array<T, length>& column) {
    for (auto& el : column) {
        el = random<std::remove_reference_t<decltype(el)>>();
    }
}

template <typename T, std::size_t length>
void print_column(const std::array<T, length>& column, std::integral auto num_tuples) {
    println("column:", boost::core::demangle(typeid(T).name()));
    println(column);
}

// PAX-style page
template <u64 page_size, typename... Attributes>
struct /* alignas(page_size) */ Page {
    static constexpr auto max_tuples_per_page = calc_max_tuples_per_page<Attributes...>(page_size);
    alignas(std::max_align_t) u64 num_tuples{0};
    std::tuple<std::array<Attributes, max_tuples_per_page>...> columns;

    template <u64 idx, u64 other_idx, u64... other_idxs, u64 other_page_size, typename... OtherAttributes>
    void _emplace_back(std::size_t row_idx, const Page<other_page_size, OtherAttributes...>& page) {
        std::get<idx>(std::get<0>(columns)[num_tuples]) = std::get<other_idx>(std::get<0>(page.columns)[row_idx]);
        if constexpr (idx != 0) {
            _emplace_back<idx + 1, other_idxs...>(row_idx, page);
        }
    }

    template <u64... other_idxs, u64 other_page_size, typename... OtherAttributes>
    void emplace_back(std::size_t row_idx, const Page<other_page_size, OtherAttributes...>& page) {
        _emplace_back<0, other_idxs...>(row_idx, page);
        num_tuples++;
    }

    template <u64 idx, u64 other_idx, u64... other_idxs, u64 other_page_size, typename... OtherAttributes>
    void _emplace_back_transposed(u64 row_idx, const Page<other_page_size, OtherAttributes...>& page) {
        std::get<idx>(std::get<0>(columns)[num_tuples]) = std::get<other_idx>(page.columns)[row_idx];
        if constexpr (idx != 0) {
            _emplace_back_transposed<idx + 1, other_idxs...>(row_idx, page);
        }
    }

    template <u64... other_idxs, u64 other_page_size, typename... OtherAttributes>
    void emplace_back_transposed(u64 row_idx, const Page<other_page_size, OtherAttributes...>& page) {
        _emplace_back_transposed<0, other_idxs...>(row_idx, page);
        num_tuples++;
    }

    void clear() { memset(this, 0, page_size); }

    void clear_tuples() { num_tuples = 0; }

    void fill_random() {
        std::apply([](auto&&... args) { ((random_column(args)), ...); }, columns);
    }

    void print_contents() const {
        println<' '>("num tuples:", num_tuples);
        hexdump(this, sizeof(*this));
    }

    void print_info() const {
        println<' '>("size of page:", page_size, "( max tuples:", max_tuples_per_page, ")");
        print("schema: ");
        println<','>(boost::core::demangle(typeid(Attributes).name())...);
    }

    [[nodiscard]] bool full() const { return num_tuples == max_tuples_per_page; }

    [[nodiscard]] bool empty() const { return num_tuples == 0; }

    void compress() {}
};

template <u64 size, typename... Attributes>
static auto as_bytes(Page<size, Attributes...>* page) {
    return reinterpret_cast<std::byte*>(page);
}

template <u64 page_size, typename... Attributes>
static void check_page(const Page<page_size, Attributes...> page, bool send) {
    for (auto i{0u}; i < page.max_tuples_per_page; ++i) {
        if (std::get<0>(std::get<0>(page.columns)[i]) != 0 and std::get<0>(std::get<0>(page.columns)[i]) != '0' and
            std::get<0>(std::get<0>(page.columns)[i]) != '1') {
            page.print_contents();
            println("weird character:", std::get<0>(std::get<0>(page.columns)[i]),
                    int32_t(std::get<0>(std::get<0>(page.columns)[i])),
                    u64(*reinterpret_cast<const u64*>(&std::get<0>(page.columns)[i])), "idx:", i, "/",
                    page.max_tuples_per_page / 10, page.max_tuples_per_page);
            //            return;
            throw std::runtime_error(send ? "send unexpected page contents!!!"
                                          : "recv unexpected page "
                                            "contents!!!");
        }
    }
}

static_assert(sizeof(Page<defaults::local_page_size, char>) == defaults::local_page_size);