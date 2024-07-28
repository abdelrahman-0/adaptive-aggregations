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

template <char delimiter = 0, typename... Ts, size_t... indexes>
void __print(std::ostream& stream, std::tuple<Ts&&...> args, std::index_sequence<indexes...>) {
    (..., [&]() {
        if constexpr (custom_concepts::is_iterable<Ts>) {
            auto iter_idx = 0u;
            for (auto&& el : std::get<indexes>(args)) {
                __print(stream, std::forward_as_tuple(std::forward<std::remove_reference_t<decltype(el)>>(el)),
                        std::make_index_sequence<1>{});
                stream << ((iter_idx++ < std::get<indexes>(args).size() - 1) ? ","s : ""s);
            }
        } else if constexpr (custom_type_traits::is_tuple_v<Ts>) {
            std::apply(
                [&](auto el) {
                    __print<','>(stream, std::forward_as_tuple(std::forward<std::remove_reference_t<decltype(el)>>(el)),
                                 std::make_index_sequence<1>{});
                },
                std::get<indexes>(args));
        } else {
            stream << std::get<indexes>(args)
                   << ((delimiter and indexes < sizeof...(Ts) - 1) ? std::string{delimiter} : ""s);
        }
    }());
}

template <char delimiter = ' ', typename... Ts>
void println(Ts... args) {
    __print<delimiter>(std::cout, std::forward_as_tuple(std::forward<Ts>(args)...), std::index_sequence_for<Ts...>{});
    std::cout << '\n';
}

template <char delimiter = 0, typename... Ts>
void print(Ts... args) {
    __print<delimiter>(std::cout, std::forward_as_tuple(std::forward<Ts>(args)...), std::index_sequence_for<Ts...>{});
}

// template <char delimiter = 0, typename... Ts>
// void print(const Ts&... args) {
//     __print<delimiter>(std::cout, args...);
// }

// template <char delimiter = ' ', typename... Ts>
// void println(const Ts&... args) {
//     __print<delimiter>(std::cout, args...);
//     std::cout << '\n';
// }

template <char delimiter = ' ', typename... Ts>
void logln(Ts... args) {
    __print<delimiter>(std::cerr /* unbuffered */, std::forward_as_tuple(std::forward<Ts>(args)...),
                       std::index_sequence_for<Ts...>{});
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
