#pragma once

#include <barrier>
#include <cmath>
#include <gflags/gflags.h>
#include <latch>
#include <thread>

#include "adaptive/common.h"
#include "adaptive/node_monitor.h"
#include "adaptive/worker_state.h"
#include "bench/bench.h"
#include "bench/common_flags.h"
#include "bench/stopwatch.h"
#include "core/buffer/partition_buffer.h"
#include "core/buffer/partition_inserter.h"
#include "core/hashtable/ht_global.h"
#include "core/hashtable/ht_local.h"
#include "core/network/connection.h"
#include "core/network/network_manager.h"
#include "core/sketch/hll_custom.h"
#include "core/storage/page_local.h"
#include "core/storage/table.h"
#include "defaults.h"
#include "system/node.h"
#include "utils/configuration.h"

using namespace std::chrono_literals;
/* --------------------------------------- */
#define AGG_VALS 1
#define AGG_KEYS u64
#define GPR_KEYS_IDX 0
#define GRP_KEYS u64
/* --------------------------------------- */
using Groups     = std::tuple<GRP_KEYS>;
using Aggregates = std::tuple<AGG_KEYS>;
#define TABLE_SCHEMA GRP_KEYS, u64, u64, u64, double, double, double, double, char, char, s32, s32, s32, std::array<char, 25>, std::array<char, 10>, std::array<char, 44>
/* --------------------------------------- */
void fn_agg(Aggregates& aggs_grp, const Aggregates& aggs_tup)
{
    std::get<0>(aggs_grp) += std::get<0>(aggs_tup);
}
void fn_agg_concurrent(Aggregates& aggs_grp, const Aggregates& aggs_tup)
{
    __sync_fetch_and_add(&std::get<0>(aggs_grp), std::get<0>(aggs_tup));
}
/* --------------------------------------- */
#if defined(LOCAL_UNCHAINED_HT_16)
static constexpr ht::IDX_MODE idx_mode_slots = ht::INDIRECT_16;
static constexpr bool is_ht_loc_salted       = false;
#elif defined(LOCAL_UNCHAINED_HT_32)
static constexpr ht::IDX_MODE idx_mode_slots = ht::INDIRECT_32;
static constexpr bool is_ht_loc_salted       = true;
#elif defined(LOCAL_UNCHAINED_HT_64)
static constexpr ht::IDX_MODE idx_mode_slots = ht::INDIRECT_64;
static constexpr bool is_ht_loc_salted       = true;
#else
static constexpr ht::IDX_MODE idx_mode_slots = ht::DIRECT;
static constexpr bool is_ht_loc_salted       = false;
#endif
// --------------------------------
#if (defined(LOCAL_UNCHAINED_HT_16) or defined(LOCAL_UNCHAINED_HT_32) or defined(LOCAL_UNCHAINED_HT_64)) and defined(GLOBAL_UNCHAINED_HT)
static constexpr ht::IDX_MODE idx_mode_entries = ht::NO_IDX;
#else
static constexpr ht::IDX_MODE idx_mode_entries = ht::DIRECT;
#endif
// --------------------------------
#if defined(GLOBAL_UNCHAINED_HT)
static constexpr bool is_ht_glob_salted = true;
#else
static constexpr bool is_ht_glob_salted = false;
#endif
/* --------------------------------------- */
using MemAlloc                                 = mem::JEMALLOCator<true>;
using Sketch                                   = ht::HLLSketch<true>;
using PageTable                                = PageLocal<TABLE_SCHEMA>;
using PageResult                               = ht::PageAggregation<Groups, Aggregates, idx_mode_entries, idx_mode_slots == ht::DIRECT, true>;
using BlockAlloc                               = mem::BlockAllocatorNonConcurrent<PageResult, MemAlloc>;
using BufferLocal                              = buf::EvictionBuffer<PageResult, BlockAlloc>;
using StorageLocal                             = buf::PartitionBuffer<PageResult, false>;
using StorageGlobal                            = buf::PartitionBuffer<PageResult, true>;
using InserterLocal                            = buf::PartitionedAggregationInserter<PageResult, idx_mode_entries, BufferLocal, Sketch, true>;
using EgressManager                            = network::HomogeneousEgressNetworkManager<PageResult, Sketch>;
using IngressManager                           = network::HomogeneousIngressNetworkManager<PageResult, Sketch>;
/* --------------------------------------- */
#if defined(LOCAL_UNCHAINED_HT_16) or defined(LOCAL_UNCHAINED_HT_32) or defined(LOCAL_UNCHAINED_HT_64)
using HashtableLocal = ht::PartitionedOpenAggregationHashtable<Groups, Aggregates, idx_mode_entries, idx_mode_slots, fn_agg, MemAlloc, BlockAlloc, Sketch, is_ht_loc_salted, true, false>;
#else
using HashtableLocal = ht::PartitionedChainedAggregationHashtable<Groups, Aggregates, idx_mode_entries, idx_mode_slots, fn_agg, MemAlloc, SketchLocal, true, false>;
#endif
/* --------------------------------------- */
#if defined(GLOBAL_UNCHAINED_HT)
using HashtableGlobal = ht::ConcurrentOpenAggregationHashtable<Groups, Aggregates, idx_mode_entries, fn_agg_concurrent, MemAlloc, true, is_ht_glob_salted>;
#else
using HashtableGlobal = ht::ConcurrentChainedAggregationHashtable<Groups, Aggregates, fn_agg_concurrent, MemAlloc, true>;
#endif
/* --------------------------------------- */
static_assert(idx_mode_slots != ht::NO_IDX);
/* --------------------------------------- */
DEFINE_uint32(threads, 1, "number of threads to use");
DEFINE_uint32(slots, PageResult::max_tuples_per_page * 2, "number of slots to use per partition");
DEFINE_uint32(bump, 1, "bumping factor to use when allocating memory for partition pages");
DEFINE_double(htfactor, 2.0, "growth factor to use when allocating global hashtable");
DEFINE_double(thresh, 0.2, "tuple ratio threshold for disabling local pre-aggregation");
DEFINE_string(policy, "regression", "scale out policy (either regression or static)");
DEFINE_uint32(timeout, 200, "time budget in milliseconds (time limit for regression policy; scale out point for static policy)");
DEFINE_uint32(static_workers, 2, "number of workers to scale out to (only for static policy)");
