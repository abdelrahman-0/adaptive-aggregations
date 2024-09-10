#pragma once

#include <boost/core/demangle.hpp>
#include <exception>
#include <iostream>
#include <limits>
#include <random>
#include <tuple>
#include <type_traits>

#include "custom_concepts.h"
#include "custom_type_traits.h"

// need 64-bit system for pointer tagging
static_assert(sizeof(std::size_t) == 8 and sizeof(uintptr_t) == 8);

static constexpr uint64_t pointer_tag_mask = (~static_cast<uint64_t>(0)) >> 16;

template <typename T>
static inline auto get_pointer(void* tagged_ptr) {
    return reinterpret_cast<T*>(reinterpret_cast<uintptr_t>(tagged_ptr) & pointer_tag_mask);
}

template <typename T>
static inline auto get_pointer(uintptr_t tagged_ptr) {
    return reinterpret_cast<T*>(tagged_ptr & pointer_tag_mask);
}

static inline uint16_t get_tag(void* tagged_ptr) { return reinterpret_cast<uintptr_t>(tagged_ptr) >> 48; }

static inline uint16_t get_tag(uintptr_t tagged_ptr) { return tagged_ptr >> 48; }

static inline auto tag_pointer(custom_concepts::pointer_type auto ptr, std::integral auto tag) {
    return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(ptr) | (static_cast<uintptr_t>(tag) << 48));
}

static auto rng = std::mt19937{std::random_device{}()};

template <char delimiter = 0, typename... Ts, size_t... indexes>
void __print(std::ostream& stream, std::tuple<Ts&&...> args, std::index_sequence<indexes...>) {
    using namespace std::string_literals;
    (..., [&]() {
        if constexpr (custom_concepts::is_iterable<Ts>) {
            auto iter_idx = 0u;
            for (auto&& el : std::get<indexes>(args)) {
                __print(stream, std::forward_as_tuple(std::forward<std::remove_reference_t<decltype(el)>>(el)),
                        std::make_index_sequence<1>{});
                stream << ((iter_idx++ < std::get<indexes>(args).size() - 1) ? std::string{delimiter} : "");
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

template <char delimiter = ' ', typename... Ts>
void logln(Ts... args) {
    __print<delimiter>(std::cerr /* unbuffered */, std::forward_as_tuple(std::forward<Ts>(args)...),
                       std::index_sequence_for<Ts...>{});
    std::cerr << '\n';
}

template <typename T>
T random() {
    if constexpr (std::is_same_v<unsigned char, T> or std::is_same_v<char, T>) {
        std::uniform_int_distribution<T> dist(33 /* ! */, 126 /* ~ */);
        return dist(rng);
    } else if constexpr (std::is_integral_v<T>) {
        std::uniform_int_distribution<T> dist(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
        return dist(rng);
    } else if constexpr (std::is_floating_point_v<T>) {
        std::uniform_real_distribution<T> dist(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
        return dist(rng);
    } else if constexpr (custom_type_traits::is_array_v<T>) {
        T arr{};
        for (auto& el : arr)
            el = random<typename T::value_type>();
        return arr;
    } else if constexpr (custom_type_traits::is_tuple_v<T>) {
        T tup{};
        std::apply([](auto& el) { el = random<decltype(el)>(); }, tup);
        return tup;
    } else {
        logln("cannot generate random value for this type:", boost::core::demangle(typeid(T).name()));
        throw std::runtime_error("unsupported type");
    }
}

void set_cpu_affinity(int tid, bool hyperthreading) {
    auto nthreads_sys = std::thread::hardware_concurrency();
    ::cpu_set_t mask;
    auto cpu_0 = (2 * tid) % nthreads_sys;
    auto cpu_1 = (2 * tid + 1) % nthreads_sys;
    CPU_ZERO(&mask);
    if (not hyperthreading) {
        std::exit(0);
    }
    CPU_SET(cpu_0, &mask);
    CPU_SET(cpu_1, &mask);
    if (::sched_setaffinity(0, sizeof(mask), &mask)) {
        logln("unable to pin CPU");
        std::exit(0); // TODO exception class
    }
    println("pinned thread", tid, "to cpu(s):", cpu_0, cpu_1);
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

template <typename T>
requires(sizeof(T) == 8)
static void bin_print(T val) {
    uint64_t new_val;
    if constexpr (std::is_pointer_v<T>) {
        new_val = reinterpret_cast<uint64_t>(val);
    } else {
        new_val = val;
    }
    for (auto i{0u}; i < 64; ++i) {
        std::cout << ((new_val >> (63 - i)) & 1);
    }
    println();
}

void hexdump(custom_concepts::pointer_type auto ptr, int buflen) {
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