#pragma once

#include <DataSketches/MurmurHash3.h>
#include <gflags/gflags.h>
#include <thread>

#include "bench/common_flags.h"
#include "bench/logger.h"
#include "bench/stopwatch.h"
#include "core/hashtable/sketch/cpc_wrapper.h"
#include "core/hashtable/sketch/hll_custom.h"
#include "defaults.h"
#include "misc/librand/random.h"
#include "utils/hash.h"
#include "utils/utils.h"

DEFINE_uint32(threads, 1, "number of threads to use");
DEFINE_uint64(n, 100'000, "total number of entries");
