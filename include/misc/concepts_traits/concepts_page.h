#pragma once

namespace concepts {

template <typename T>
concept is_page = requires(T t) {
    { t.num_tuples };
    { t.full() } -> std::convertible_to<bool>;
};

template <typename T>
concept is_communication_page = is_page<T> and requires(T t) {
    { t.set_last_page() } -> std::convertible_to<void>;
    { t.is_last_page() } -> std::convertible_to<bool>;
};

template <typename... Attributes>
concept is_row_store = sizeof...(Attributes) == 1;

} // namespace concepts