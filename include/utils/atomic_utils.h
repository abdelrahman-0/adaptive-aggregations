// Taken from spilly's libdb

// (c) Thomas Neumann 2010

#pragma once

namespace utils {

template <class T>
void atomic_max(T& val, const T& other)
{
    while ((other > val) && !__sync_bool_compare_and_swap(&val, val, other))
        ;
}

template <class T>
void atomic_min(T& val, const T& other)
{
    while ((other < val) && !__sync_bool_compare_and_swap(&val, val, other))
        ;
}

template <class T>
requires(sizeof(T) < 16)
void atomic_add(T& val, const T& other)
{
    __sync_fetch_and_add(&val, other);
}

} // namespace utils
