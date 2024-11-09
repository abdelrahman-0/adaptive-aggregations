#pragma once

#include <cassert>

#include "likwid-marker.h"

#if defined(NDEBUG)
#define DEBUGGING(x...)
#define ASSERT(x...)
#define PERF(x...) x
#else
#define DEBUGGING(x...) x
#define ASSERT(expr) assert((expr))
#define PERF(x...) x
#endif
