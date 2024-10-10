#pragma once

#include <boost/core/demangle.hpp>
#include <exception>
#include <iostream>
#include <limits>
#include <random>
#include <tuple>
#include <type_traits>

#include "concepts_traits/concepts_common.h"
#include "concepts_traits/type_traits_common.h"

// need 64-bit system for pointer tagging
static_assert(sizeof(std::size_t) == 8 and sizeof(uintptr_t) == 8);

static constexpr uint64_t pointer_tag_mask = (~static_cast<uint64_t>(0)) >> 16;

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

static inline uint16_t get_tag(void* tagged_ptr) { return reinterpret_cast<uintptr_t>(tagged_ptr) >> 48; }

static inline uint16_t get_tag(uintptr_t tagged_ptr) { return tagged_ptr >> 48; }

static inline auto tag_pointer(concepts::is_pointer auto ptr, std::integral auto tag)
{
    return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(ptr) | (static_cast<uintptr_t>(tag) << 48));
}

static auto rng = std::mt19937{std::random_device{}()};

template <char delimiter = 0, typename... Ts, size_t... indexes>
void __print(std::ostream& stream, std::tuple<Ts&&...> args, std::index_sequence<indexes...>)
{
    using namespace std::string_literals;
    (..., [&]() {
        if constexpr (concepts::is_iterable<Ts>) {
            auto iter_idx = 0u;
            for (auto&& el : std::get<indexes>(args)) {
                __print(stream, std::forward_as_tuple(std::forward<std::remove_reference_t<decltype(el)>>(el)),
                        std::make_index_sequence<1>{});
                stream << ((iter_idx++ < std::get<indexes>(args).size() - 1) ? std::string{delimiter} : "");
            }
        }
        else if constexpr (type_traits::is_tuple_v<Ts>) {
            std::apply(
                [&](auto el) {
                    __print<','>(stream, std::forward_as_tuple(std::forward<std::remove_reference_t<decltype(el)>>(el)),
                                 std::make_index_sequence<1>{});
                },
                std::get<indexes>(args));
        }
        else {
            stream << std::get<indexes>(args)
                   << ((delimiter and indexes < sizeof...(Ts) - 1) ? std::string{delimiter} : ""s);
        }
    }());
}

template <char delimiter = ' ', typename... Ts>
void print(Ts... args)
{
    __print<delimiter>(std::cout, std::forward_as_tuple(std::forward<Ts>(args)...), std::index_sequence_for<Ts...>{});
    std::cout << '\n';
}

template <char delimiter = ' ', typename... Ts>
void logln(Ts... args)
{
    __print<delimiter>(std::cerr /* unbuffered */, std::forward_as_tuple(std::forward<Ts>(args)...),
                       std::index_sequence_for<Ts...>{});
    std::cerr << '\n';
}

template <concepts::is_char T>
T random()
{
    thread_local std::uniform_int_distribution<T> dist(33 /* ! */, 126 /* ~ */);
    return dist(rng);
}

template <std::integral T>
requires(not concepts::is_char<T>)
T random(T min = std::numeric_limits<T>::min(), T max = 9)
{
    thread_local std::uniform_int_distribution<T> dist(min, max);
    return dist(rng);
}

template <std::floating_point T>
T random(T min = std::numeric_limits<T>::min(), T max = std::numeric_limits<T>::max())
{
    thread_local std::uniform_real_distribution<T> dist(min, max);
    return dist(rng);
}

template <concepts::is_array T>
T random()
{
    T arr{};
    for (auto& el : arr)
        el = random<typename T::value_type>();
    return arr;
}

template <concepts::is_tuple T>
T random()
{
    T tup{};
    std::apply([](auto&& el) { el = random<decltype(el)>(); }, tup);
    return tup;
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
    uint64_t new_val;
    if constexpr (std::is_pointer_v<T>) {
        new_val = reinterpret_cast<uint64_t>(val);
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
