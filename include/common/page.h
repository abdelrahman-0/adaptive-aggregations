#pragma once

#include <array>
#include <boost/core/demangle.hpp>
#include <cstdint>
#include <cstring>
#include <tuple>

#include "concepts_traits/type_traits_common.h"
#include "defaults.h"
#include "utils/utils.h"

template <typename... Attributes>
consteval std::size_t calc_max_tuples_per_page(std::size_t page_size)
{
    return ((page_size - alignof(std::max_align_t)) / (sizeof(Attributes) + ...));
}

template <typename T, std::size_t length>
void random_column(std::array<T, length>& column)
{
    for (auto& el : column) {
        el = random<std::remove_reference_t<decltype(el)>>();
    }
}

// PAX-style page
template <u64 page_size, typename Attribute, typename... Attributes>
struct Page {
    static constexpr auto max_tuples_per_page = calc_max_tuples_per_page<Attribute, Attributes...>(page_size);
    union alignas(std::max_align_t) {
        u64 num_tuples{};
        Attribute* ptr; // used only for row-store
    };
    std::tuple<std::array<Attribute, max_tuples_per_page>, std::array<Attributes, max_tuples_per_page>...> columns;

    Page() { clear_tuples(); }

    template <u16 num_cols, u64 idx, u64 other_idx, u64... other_idxs, typename... OtherAttributes>
    void _emplace_back(const std::tuple<OtherAttributes...>& tuple)
    {
        std::get<idx>(*ptr) = std::get<other_idx>(tuple);
        if constexpr (idx < num_cols - 1) {
            _emplace_back<num_cols, idx + 1, other_idxs...>(tuple);
        }
    }

    [[maybe_unused]]
    auto emplace_back(const Attribute& val)
    {
        *ptr = val;
        return ptr++;
    }

    template <u64... other_idxs, typename... OtherAttributes>
    [[maybe_unused]]
    auto emplace_back(const std::tuple<OtherAttributes...>& tuple)
    {
        _emplace_back<sizeof...(other_idxs), 0, other_idxs...>(tuple);
        return ptr++;
    }

    template <u64... other_idxs, u64 other_page_size, typename... OtherAttributes>
    requires concepts::is_row_store<Attribute, Attributes...>
    [[maybe_unused]]
    auto emplace_back(std::size_t row_idx, const Page<other_page_size, OtherAttributes...>& page)
    {
        _emplace_back<sizeof...(other_idxs), 0, other_idxs...>(page.template get_value<0>(row_idx));
        return ptr++;
    }

    template <u16 num_cols, u16 idx, u16 other_idx, u16... other_idxs, u64 other_page_size, typename... OtherAttributes>
    void _emplace_back_transposed(u64 row_idx, const Page<other_page_size, OtherAttributes...>& page)
    {
        std::get<idx>(*ptr) = page.template get_value<other_idx>(row_idx);
        if constexpr (idx < num_cols - 1) {
            _emplace_back_transposed<num_cols, idx + 1, other_idxs...>(row_idx, page);
        }
    }

    template <u16... other_idxs, u64 other_page_size, typename... OtherAttributes>
    requires concepts::is_row_store<Attribute, Attributes...>
    [[maybe_unused]]
    auto emplace_back_transposed(u64 row_idx, const Page<other_page_size, OtherAttributes...>& page)
    {
        _emplace_back_transposed<sizeof...(other_idxs), 0, other_idxs...>(row_idx, page);
        return ptr++;
    }

    void clear() { memset(this, 0, page_size); }

    void clear_tuples()
    requires concepts::is_row_store<Attribute, Attributes...>
    { // constexpr
        ptr = reinterpret_cast<Attribute*>(reinterpret_cast<std::byte*>(this) + alignof(std::max_align_t));
    }

    void clear_tuples() { num_tuples = 0; }

    template <u16 col_idx = 0>
    auto get_value(std::integral auto row_idx) const
    {
        return std::get<col_idx>(columns)[row_idx];
    }

    [[nodiscard]]
    bool full() const
    requires concepts::is_row_store<Attribute, Attributes...>
    {
        return ptr == (std::get<0>(columns).data() + std::get<0>(columns).size());
    }

    [[nodiscard]]
    bool full() const
    {
        return num_tuples == max_tuples_per_page;
    }

    [[nodiscard]]
    bool empty() const
    requires concepts::is_row_store<Attribute, Attributes...>
    {
        return ptr == reinterpret_cast<std::byte*>(this) + alignof(std::max_align_t);
    }

    [[nodiscard]]
    bool empty() const
    {
        return num_tuples == 0;
    }

    void retire()
    requires concepts::is_row_store<Attribute, Attributes...>
    {
        num_tuples = ptr - std::get<0>(columns).data();
    }

    void retire() const {}

    void compress() {}

    [[nodiscard]]
    std::byte* as_bytes() const
    {
        return reinterpret_cast<std::byte*>(this);
    }

    template <u16 col_idx>
    void print_column()
    {
        //        print("column:", boost::core::demangle(typeid(T).name()));
        print(std::get<col_idx>(columns)); // TODO use std::span to handle non-full pages
    }

    void print_contents() const
    {
        print<' '>("num tuples:", num_tuples);
        hexdump(this, sizeof(*this));
    }

    void print_info() const
    {
        print<' '>("size of page:", page_size, "( max tuples:", max_tuples_per_page, ")");
        print("schema: ");
        print<','>(boost::core::demangle(typeid(Attributes).name())...);
    }

    void fill_random()
    {
        std::apply([](auto&&... args) { ((random_column(args)), ...); }, columns);
    }
};

template <u64 page_size, typename... Attributes>
static void check_page(const Page<page_size, Attributes...> page, bool send)
{
    for (auto i{0u}; i < page.max_tuples_per_page; ++i) {
        if (std::get<0>(std::get<0>(page.columns)[i]) != 0 and std::get<0>(std::get<0>(page.columns)[i]) != '0' and
            std::get<0>(std::get<0>(page.columns)[i]) != '1') {
            page.print_contents();
            print("weird character:", std::get<0>(std::get<0>(page.columns)[i]),
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
