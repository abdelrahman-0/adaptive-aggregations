#pragma once

#include <cstdint>
#include <array>
#include <tuple>

static constexpr std::size_t page_size = 1 << 12;

template <typename... Attributes>
consteval std::size_t calc_num_tuples_per_page(){
    return (page_size - sizeof(std::size_t)) / ((sizeof(Attributes) + ...));
}

// PAX-style page
template <typename... Attributes>
struct alignas(page_size) Page {
    static constexpr auto num_tuples_per_page = calc_num_tuples_per_page<Attributes...>();
    std::tuple<std::array<Attributes, num_tuples_per_page>...> columns;
    std::size_t num_tuples = num_tuples_per_page;
};

static_assert(sizeof(Page<int32_t>) == page_size);
static_assert(sizeof(Page<int32_t, int32_t>) == page_size);
static_assert(sizeof(Page<int32_t, int32_t[20]>) == page_size);