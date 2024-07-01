#pragma once

#include <iostream>

template <typename T>
void print(const T& last){ std::cout << last << std::endl; }

template <typename T, typename... Ts>
void print(const T& first, const Ts&... args) noexcept(noexcept(std::cout << first << std::endl)) {
    std::cout << first << " ";
    print(args...);
}