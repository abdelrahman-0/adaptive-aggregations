#pragma once

#include <tuple>
#include <type_traits>

namespace custom_type_traits {
// std::is_tuple_v does not exist in C++20
template <typename> struct is_tuple : std::false_type {};
template <typename... T> struct is_tuple<std::tuple<T...>> : std::true_type {};
template <typename T> inline constexpr bool is_tuple_v = is_tuple<T>::value;

// std::is_array_v is false for std::array 😢
template <typename> struct is_array : std::false_type {};
template <typename T, std::size_t N>
struct is_array<std::array<T, N>> : std::true_type {};
template <typename T> inline constexpr bool is_array_v = is_array<T>::value;

} // namespace custom_type_traits
