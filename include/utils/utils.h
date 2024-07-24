#pragma once

#include <exception>
#include <iostream>
#include <limits>
#include <random>
#include <tuple>
#include <type_traits>

#include "custom_concepts.h"
#include "custom_type_traits.h"

static auto rng = std::mt19937{std::random_device{}()};

// TODO no delimiter after last element (use std::forward_as_tuple() and std::get<>, std::apply and last element check)
template <char delimiter = 0, typename... Ts>
void __print(std::ostream& stream, const Ts&... args) {
    (
        [&]() {
            if constexpr (custom_concepts::is_iterable<Ts>) {
                for (const auto& el : args) {
                    __print(stream, el);
                }
            } else if constexpr (custom_type_traits::is_tuple_v<Ts>) {
                std::apply([&](const auto&... el) { (__print<' '>(stream, el), ...); }, args);
            } else {
                if constexpr (std::is_same_v<Ts, bool>) {
                    stream << std::boolalpha << args << std::noboolalpha;
                } else {
                    stream << args;
                }
                if constexpr (delimiter) {
                    stream << delimiter;
                }
            }
        }(),
        ...);
}

template <char delimiter = 0, typename... Ts>
void print(const Ts&... args) {
    __print<delimiter>(std::cout, args...);
}

template <char delimiter = ' ', typename... Ts>
void println(const Ts&... args) {
    __print<delimiter>(std::cout, args...);
    std::cout << '\n';
}

template <char delimiter = ' ', typename... Ts>
void logln(const Ts&... args) {
    __print<delimiter>(std::cerr /* unbuffered */, args...);
    std::cerr << '\n';
}

template <typename T>
void random(T& t) {
    if constexpr (std::is_same_v<unsigned char, T>) {
        std::uniform_int_distribution<T> dist(33 /* ! */, 126 /* ~ */);
        t = dist(rng);
    } else if constexpr (std::is_integral_v<T>) {
        std::uniform_int_distribution<T> dist(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
        t = dist(rng);
    } else if constexpr (std::is_floating_point_v<T>) {
        std::uniform_real_distribution<T> dist(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
        t = dist(rng);
    } else if constexpr (custom_type_traits::is_array_v<T>) {
        for (auto& el : t)
            random(el);
    } else if constexpr (custom_type_traits::is_tuple_v<T>) {
        std::apply([](auto&... el) { ((random(el)), ...); }, t);
    } else {
        throw std::runtime_error("cannot generate random value for this type");
    }
}

constexpr auto next_power_of_2(std::integral auto val) -> decltype(auto)
requires(sizeof(val) <= sizeof(unsigned long long) and std::is_unsigned_v<decltype(val)>)
{
    return (static_cast<decltype(val)>(1)) << ((sizeof(val) << 3) -
                                               (sizeof(val) == sizeof(unsigned)        ? __builtin_clz(val)
                                                : sizeof(val) == sizeof(unsigned long) ? __builtin_clzl(val)
                                                                                       : __builtin_clzll(val)) -
                                               (std::popcount(val) == 1));
}
