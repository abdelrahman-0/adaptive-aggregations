#pragma once

#include <type_traits>
#include <cstddef>
#include <concepts>

template <typename T>
concept Can_Send = requires(T t, size_t i, std::byte* buf)
{
    t.send(i, buf);
};

template <typename T>
concept Can_Recv = requires(T t, size_t i, std::byte* buf)
{
    t.recv(i, buf);
};

template <typename T>
concept Can_Send_Recv = Can_Send<T> and Can_Recv<T>;
