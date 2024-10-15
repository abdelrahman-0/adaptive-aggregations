// inspired by Maximilian Kuschewski (2023)

#pragma once

#include <cassert>

#ifdef NDEBUG
#define DEBUGGING(x...)
#define ASSERT(x...)
#else
#define DEBUGGING(x...) x
#define ASSERT(expr) assert((expr))
#endif

namespace ubench{


}