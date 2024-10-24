#pragma once

#include <cassert>

#include "likwid-marker.h"

#ifdef NDEBUG
#define DEBUGGING(x...)
#define ASSERT(x...)
#else
#define DEBUGGING(x...) x
#define ASSERT(expr) assert((expr))
#endif
