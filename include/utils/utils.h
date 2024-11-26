#pragma once

#include <bit>
#include <boost/core/demangle.hpp>
#include <exception>
#include <iostream>
#include <tuple>
#include <type_traits>

#include "defaults.h"
#include "misc/concepts_traits/concepts_common.h"
#include "misc/concepts_traits/type_traits_common.h"

auto range(std::integral auto start, std::integral auto end)
{
    return std::ranges::iota_view{start, end};
}

auto range(std::unsigned_integral auto end)
{
    return std::ranges::iota_view{static_cast<decltype(end)>(0), end};
}

// need 64-bit system for pointer tagging
static_assert(sizeof(void*) == 8);

static constexpr u64 pointer_tag_mask = (~static_cast<u64>(0)) >> 16;

template <typename T>
static inline auto get_pointer(void* tagged_ptr)
{
    return reinterpret_cast<T*>(reinterpret_cast<uintptr_t>(tagged_ptr) & pointer_tag_mask);
}

template <typename T>
static inline auto get_pointer(uintptr_t tagged_ptr)
{
    return reinterpret_cast<T*>(tagged_ptr & pointer_tag_mask);
}

static inline u16 get_tag(void* tagged_ptr)
{
    return reinterpret_cast<uintptr_t>(tagged_ptr) >> 48;
}

static inline u16 get_tag(uintptr_t tagged_ptr)
{
    return tagged_ptr >> 48;
}

static inline auto tag_pointer(concepts::is_pointer auto ptr, std::integral auto tag)
{
    return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(ptr) | (static_cast<uintptr_t>(tag) << 48));
}

template <char delimiter = 0, typename... Ts, size_t... indexes>
void __print(std::ostream& stream, std::tuple<Ts&&...> args, std::index_sequence<indexes...>)
{
    using namespace std::string_literals;
    (..., [&]() {
        if constexpr (concepts::is_iterable<Ts>) {
            auto iter_idx = 0u;
            for (auto&& el : std::get<indexes>(args)) {
                __print(stream, std::forward_as_tuple(std::forward<std::remove_reference_t<decltype(el)>>(el)), std::make_index_sequence<1>{});
                stream << ((iter_idx++ < std::get<indexes>(args).size() - 1) ? std::string{delimiter} : "");
            }
        }
        else if constexpr (type_traits::is_tuple_v<Ts>) {
            std::apply([&](auto el) { __print<','>(stream, std::forward_as_tuple(std::forward<std::remove_reference_t<decltype(el)>>(el)), std::make_index_sequence<1>{}); },
                       std::get<indexes>(args));
        }
        else {
            stream << std::get<indexes>(args) << ((delimiter and indexes < sizeof...(Ts) - 1) ? std::string{delimiter} : ""s);
        }
    }());
}

template <char delimiter = ' ', char end_char = '\n', typename... Ts>
void print(Ts... args)
{
    __print<delimiter>(std::cout, std::forward_as_tuple(std::forward<Ts>(args)...), std::index_sequence_for<Ts...>{});
    if constexpr (end_char) {
        std::cout << end_char;
    }
}

template <char delimiter = ' ', typename... Ts>
void logln(Ts... args)
{
    __print<delimiter>(std::cerr /* unbuffered */, std::forward_as_tuple(std::forward<Ts>(args)...), std::index_sequence_for<Ts...>{});
    std::cerr << '\n';
}

constexpr auto next_power_2(std::integral auto val) -> decltype(auto)
requires(sizeof(val) <= sizeof(unsigned long long) and std::is_unsigned_v<decltype(val)>)
{
    return (static_cast<decltype(val)>(1)) << ((sizeof(val) << 3) -
                                               (sizeof(val) == sizeof(unsigned)        ? __builtin_clz(val)
                                                : sizeof(val) == sizeof(unsigned long) ? __builtin_clzl(val)
                                                                                       : __builtin_clzll(val)) -
                                               (std::popcount(val) == 1));
}

template <typename T>
requires(sizeof(T) == 8)
static void bin_print(T val)
{
    u64 new_val;
    if constexpr (std::is_pointer_v<T>) {
        new_val = reinterpret_cast<u64>(val);
    }
    else {
        new_val = val;
    }
    for (auto i{0u}; i < 64; ++i) {
        std::cout << ((new_val >> (63 - i)) & 1);
    }
    print();
}

void hexdump(concepts::is_pointer auto ptr, int buflen)
{
    auto* buf = (unsigned char*)ptr;
    int i, j;
    for (i = 0; i < buflen; i += 16) {
        printf("%06x: ", i);
        for (j = 0; j < 16; j++)
            if (i + j < buflen)
                printf("%02x ", buf[i + j]);
            else
                printf("   ");
        printf(" ");
        for (j = 0; j < 16; j++)
            if (i + j < buflen)
                printf("%c", isprint(buf[i + j]) ? buf[i + j] : '.');
        printf("\n");
    }
}

template <typename... Attributes>
std::string get_schema_str()
{
    using namespace std::string_literals;
    auto schema_str = ""s;
    if constexpr (sizeof...(Attributes)) {
        schema_str = (... + (boost::core::demangle(typeid(Attributes).name()) + "|"));
        schema_str.pop_back();
    }
    return "\""s + schema_str + "\""s;
}
