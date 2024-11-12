#pragma once

#include <array>
#include <boost/core/demangle.hpp>
#include <cstdint>
#include <cstring>
#include <tuple>

#include "defaults.h"
#include "misc/concepts_traits/type_traits_common.h"
#include "misc/librand/random.h"
#include "utils/utils.h"

template <typename... Attributes>
consteval std::size_t calc_max_tuples_per_page(std::size_t page_size)
{
    return (page_size - alignof(std::max_align_t)) / (sizeof(Attributes) + ...);
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

    Page()
    {
        clear_tuples();
    }

    void clear()
    {
        memset(this, 0, page_size);
    }

    void clear_tuples()
    {
        num_tuples = 0;
    }

    template <u16 col_idx>
    auto& get_attribute_ref(std::unsigned_integral auto row_idx)
    {
        return std::get<col_idx>(columns)[row_idx];
    }

    template <u16... col_idxs>
    auto get_tuple(std::unsigned_integral auto row_idx) const
    {
        return std::make_tuple(std::get<col_idxs>(columns)[row_idx]...);
    }

    [[nodiscard]]
    bool full() const
    {
        return num_tuples == max_tuples_per_page;
    }

    [[nodiscard]]
    bool empty() const
    {
        return num_tuples == 0;
    }

    void retire() const
    {
    }

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

    void print_info() const
    {
        print("size of page:", page_size, "( max tuples:", max_tuples_per_page, ")");
        print("schema: ", boost::core::demangle(typeid(Attribute).name()), boost::core::demangle(typeid(Attributes).name())...);
        print("-----");
    }

    void print_page() const
    {
        print_info();
        print("num tuples:", num_tuples);
        hexdump(this, sizeof(*this));
        print();
    }

    template <u16 idx>
    void _fill_random()
    {
        if constexpr (idx <= sizeof...(Attributes)) {
            // idx 0 is primary key
            librand::random_iterable<idx == 0>(std::get<idx>(columns));
            _fill_random<idx + 1>();
        }
    }

    void fill_random()
    {
        _fill_random<0>();
    }
};

static_assert(sizeof(Page<defaults::local_page_size, char>) == defaults::local_page_size);

template <u64 page_size, typename Attribute, bool use_ptr = true>
struct PageRowStore : public Page<page_size, Attribute> {
    using PageBase = Page<page_size, Attribute>;
    using PageBase::clear_tuples;
    using PageBase::columns;
    using PageBase::full;
    using PageBase::max_tuples_per_page;
    using PageBase::num_tuples;
    using PageBase::ptr;
    using PageBase::retire;

    PageRowStore()
    {
        clear_tuples();
    }

    [[maybe_unused]]
    auto emplace_back(const Attribute& val)
    requires(use_ptr)
    {
        *ptr = val;
        return ptr++;
    }

    [[maybe_unused]]
    auto emplace_back(const Attribute& val)
    requires(not use_ptr)
    {
        std::get<0>(columns)[num_tuples] = val;
        return num_tuples++;
    }

    template <u16 num_cols, u64 idx, u64 other_idx, u64... other_idxs, typename... OtherAttributes>
    void _emplace_back(const std::tuple<OtherAttributes...>& tuple)
    requires(use_ptr)
    {
        std::get<idx>(*ptr) = std::get<other_idx>(tuple);
        if constexpr (idx < num_cols - 1) {
            _emplace_back<num_cols, idx + 1, other_idxs...>(tuple);
        }
    }

    template <u64... other_idxs, typename... OtherAttributes>
    [[maybe_unused]]
    auto emplace_back(const std::tuple<OtherAttributes...>& tuple)
    requires(use_ptr)
    {
        _emplace_back<sizeof...(other_idxs), 0, other_idxs...>(tuple);
        return ptr++;
    }

    template <u64... other_idxs, u64 other_page_size, typename OtherAttributes>
    [[maybe_unused]]
    auto emplace_back(std::size_t row_idx, const PageRowStore<other_page_size, OtherAttributes>& page)
    requires(use_ptr)
    {
        _emplace_back<sizeof...(other_idxs), 0, other_idxs...>(page.template get_attribute_ref<0>(row_idx));
        return ptr++;
    }

    [[nodiscard]]
    bool empty() const
    requires(use_ptr)
    {
        return ptr == std::get<0>(columns).data();
    }

    [[nodiscard]]
    bool empty() const
    requires(not use_ptr)
    {
        return num_tuples == 0;
    }

    [[nodiscard]]
    bool full() const
    requires(use_ptr)
    {
        return ptr == (std::get<0>(columns).data() + max_tuples_per_page);
    }

    void clear_tuples()
    requires(use_ptr)
    {
        ptr = std::get<0>(columns).data();
    }

    void retire()
    requires(use_ptr)
    {
        num_tuples = ptr - std::get<0>(columns).data();
    }

    auto get_tuple_ref(std::unsigned_integral auto row_idx) -> decltype(auto)
    {
        return std::get<0>(columns)[row_idx];
    }

    auto begin()
    {
        return std::get<0>(columns).begin();
    }

    auto cbegin() const
    {
        return std::get<0>(columns).cbegin();
    }

    auto end()
    {
        return std::get<0>(columns).end();
    }

    auto cend() const
    {
        return std::get<0>(columns).cend();
    }
};

template <u64 page_size, typename... Attributes>
static void check_page(const Page<page_size, Attributes...> page, bool send)
{
    for (auto i{0u}; i < page.max_tuples_per_page; ++i) {
        if (std::get<0>(std::get<0>(page.columns)[i]) != 0 and std::get<0>(std::get<0>(page.columns)[i]) != '0' and std::get<0>(std::get<0>(page.columns)[i]) != '1') {
            page.print_page();
            print("weird character:", std::get<0>(std::get<0>(page.columns)[i]), int32_t(std::get<0>(std::get<0>(page.columns)[i])),
                  u64(*reinterpret_cast<const u64*>(&std::get<0>(page.columns)[i])), "idx:", i, "/", page.max_tuples_per_page / 10, page.max_tuples_per_page);
            //            return;
            throw std::runtime_error(send ? "send unexpected page contents!!!"
                                          : "recv unexpected page "
                                            "contents!!!");
        }
    }
}
