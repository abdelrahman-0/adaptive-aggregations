#pragma once

#include <gflags/gflags.h>
#include <span>
#include <thread>

#include "bench/bench.h"
#include "bench/common_flags.h"
#include "bench/stopwatch.h"
#include "core/buffer/eviction_buffer.h"
#include "core/buffer/partition_buffer.h"
#include "core/hashtable/ht_base.h"
#include "core/hashtable/ht_global.h"
#include "core/hashtable/ht_local.h"
#include "core/hashtable/sketch/cpc_wrapper.h"
#include "core/hashtable/sketch/hll_custom.h"
#include "core/network/connection.h"
#include "core/network/network_manager.h"
#include "core/storage/page_local.h"
#include "core/storage/table.h"
#include "defaults.h"
#include "system/node.h"
#include "system/topology.h"
#include "utils/hash.h"
#include "utils/utils.h"

/* ----------- CMD LINE PARAMS ----------- */

DEFINE_uint32(threads, 1, "number of threads to use");
DEFINE_uint32(slots, 8192, "number of slots to use per partition");
DEFINE_uint32(bump, 1, "bumping factor to use when allocating memory for partition pages");
DEFINE_double(htfactor, 2.0, "growth factor to use when allocating global hashtable");

/* --------------------------------------- */

#define AGG_VALS 1
#define AGG_KEYS u64
#define GPR_KEYS_IDX 0
#define GRP_KEYS u64

using MemAlloc = mem::MMapAllocator<true>;

/* ----------- HASHTABLE ----------- */

using Groups = std::tuple<GRP_KEYS>;
using Aggregates = std::tuple<AGG_KEYS>;

static void fn_agg(Aggregates& aggs_grp, const Aggregates& aggs_tup)
{
    std::get<0>(aggs_grp) += std::get<0>(aggs_tup);
}

static void fn_agg_concurrent(Aggregates& aggs_grp, const Aggregates& aggs_tup)
{
    __sync_fetch_and_add(&std::get<0>(aggs_grp), std::get<0>(aggs_tup));
}

using SketchLocal = ht::HLLSketch;
// using SketchLocal = ht::CPCSketch;
using SketchGlobal = std::conditional_t<std::is_same_v<SketchLocal, ht::CPCSketch>, ht::CPCUnion, SketchLocal>;

static constexpr ht::IDX_MODE idx_mode_slots = ht::DIRECT;
static constexpr ht::IDX_MODE idx_mode_entries = ht::DIRECT;
static constexpr bool is_salted = true;

static_assert(idx_mode_slots != ht::NO_IDX);
static_assert(not(is_salted and idx_mode_slots == ht::INDIRECT_16));

using HashtableLocal = ht::PartitionedOpenAggregationHashtable<Groups, Aggregates, idx_mode_entries, idx_mode_slots, fn_agg, MemAlloc, SketchLocal, is_salted>;
//using HashtableLocal = ht::PartitionedChainedAggregationHashtable<Groups, Aggregates, idx_mode_entries, idx_mode_slots, fn_agg, MemAlloc, SketchLocal>;

 using HashtableGlobal = ht::ConcurrentOpenAggregationHashtable<Groups, Aggregates, idx_mode_entries, fn_agg_concurrent, MemAlloc, is_salted>;
//using HashtableGlobal = ht::ConcurrentChainedAggregationHashtable<Groups, Aggregates, fn_agg_concurrent, MemAlloc>;

using PageHashtable = HashtableLocal::page_t;

/* ----------- STORAGE ----------- */

using BlockAlloc = mem::BlockAllocator<PageHashtable, MemAlloc, false>;
using Buffer = EvictionBuffer<PageHashtable, BlockAlloc>;
using StorageGlobal = PartitionBuffer<PageHashtable, true>;

#define SCHEMA GRP_KEYS, u32, u32, std::array<char, 4>
using PageTable = PageLocal<SCHEMA>;
