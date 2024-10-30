#pragma once

#include <gflags/gflags.h>
#include <thread>

#include "bench/common_flags.h"
#include "bench/stopwatch.h"
#include "core/hashtable/sketch/cpc_wrapper.h"
#include "core/hashtable/sketch/hll_custom.h"
#include "defaults.h"
#include "misc/librand/random.h"
#include "utils/logger.h"
#include "utils/utils.h"

DEFINE_uint32(threads, 1, "number of threads to use");
DEFINE_uint64(n, 100'000, "total number of entries");

 using sketch_t = ht::HLLSketch;
//using sketch_t = ht::CPCSketch;

using union_sketches_t = std::conditional_t<std::is_same_v<sketch_t, ht::CPCSketch>, ht::CPCUnion, sketch_t>;
