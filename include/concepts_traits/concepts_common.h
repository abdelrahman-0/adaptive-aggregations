#pragma once

#include <concepts>
#include <cstddef>
#include <type_traits>

#include "type_traits_common.h"

namespace concepts {

// concept wrappers for type traits
// needed for passing auto args
template <typename T>
concept is_pointer = std::is_pointer_v<T>;
template <typename T>
concept is_array = type_traits::is_array_v<T>;
template <typename T>
concept is_tuple = type_traits::is_tuple_v<T>;
template <typename T>
concept is_char = type_traits::is_char_v<T>;

// unify arrays and vectors (but not strings)
template <typename T>
concept is_iterable = !std::is_same_v<T, std::string> and requires(T t) {
    begin(t) != end(t);
    ++std::declval<decltype(begin(t))&>();
    *begin(t);
};

template <typename T>
concept is_page = requires(T t) {
    t.num_tuples;
    t.full();
};

template <typename T>
concept is_communication_page = is_page<T> and requires(T t) {
    t.set_last_page();
    t.is_last_page();
};

template <typename... Attributes>
concept is_row_store = sizeof...(Attributes) == 1;

} // namespace concepts
