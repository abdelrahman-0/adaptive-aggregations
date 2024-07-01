#pragma once

#include <iostream>
#include <random>
#include <type_traits>
#include <limits>
#include <exception>

static auto rng = std::mt19937{std::random_device{}()};

void print() {}

template <typename T>
void print(const T& last){ std::cout << last; }

template <typename T, typename... Ts>
void print(const T& first, const Ts&... args) {
    std::cout << first << " ";
    print(args...);
}

template <typename... Ts>
void println(const Ts&... args) {
    print(args...);
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
    } else if constexpr (std::is_array_v<T>) {
        for (auto& el : t)
            random(el);
    } else {
        throw std::runtime_error("cannot generate random value for this type");
    }
}