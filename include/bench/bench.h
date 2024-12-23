#pragma once

#include <cassert>
#include <source_location>

// #include "likwid-marker.h"
#include "misc/exceptions/exceptions_misc.h"

#if defined(NDEBUG)
#define DEBUGGING(x...)
#define ENSURE(x...) (__builtin_expect(!!(x), 0) ? ((void)0) : throw except::EnsureError(std::source_location::current().file_name() + ": "s + std::string(#x)))
#define ASSERT(x...)
#define PERF(x...) x
#else
#define DEBUGGING(x...) x
#define ENSURE(expr) assert((expr))
#define ASSERT(expr) assert((expr))
#define PERF(x...) x
#endif
