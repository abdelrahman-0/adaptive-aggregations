#pragma once

#include <iostream>
#include <random>
#include <type_traits>
#include <limits>
#include <exception>
#include <tuple>
#include "custom_type_traits.h"
#include "custom_concepts.h"

static auto rng = std::mt19937{std::random_device{}()};

template <char delimiter = 0, typename... Ts>
void print(const Ts&... args) {
    ([&](){
        if constexpr (custom_concepts::is_iterable<Ts>) { for(const auto& el: args) { print(el); } }
        else if constexpr (custom_type_traits::is_tuple_v<Ts>) { std::apply([](const auto&... el) {(print<' '>(el), ...);}, args); }
        else { std::cout << args; if constexpr (delimiter) {std::cout << delimiter; } }
    }(), ...);
}

template <char delimiter = 0, typename... Ts>
void println(const Ts&... args) {
    print<delimiter>(args...);
    std::cout << '\n';
}

template<typename T>
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
        std::apply([](auto&... el) {((random(el)), ...);}, t);
    } else {
        throw std::runtime_error("cannot generate random value for this type");
    }
}