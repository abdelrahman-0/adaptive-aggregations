#pragma once

#include <concepts>
#include <cstddef>
#include <type_traits>

namespace custom_concepts {

// concept wrapper for type trait
// needed for passing auto args
template <typename T>
concept pointer_type = std::is_pointer_v<T>;

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

template <typename T>
concept Can_Send = requires(T t, size_t i, std::byte* buf) { t.send(i, buf); };

template <typename T>
concept Can_Recv = requires(T t, size_t i, std::byte* buf) { t.recv(i, buf); };

template <typename T>
concept Can_Send_Recv = Can_Send<T> and Can_Recv<T>;
} // namespace custom_concepts
