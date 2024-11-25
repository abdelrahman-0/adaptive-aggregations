#pragma once

#include <gflags/gflags.h>
#include <thread>

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

using namespace std::chrono_literals;
/* --------------------------------------- */
DEFINE_uint32(threads, 1, "number of threads to use");
DEFINE_uint32(slots, 8192, "number of slots to use per partition");
DEFINE_uint32(bump, 1, "bumping factor to use when allocating memory for partition pages");
DEFINE_double(htfactor, 2.0, "growth factor to use when allocating global hashtable");
DEFINE_bool(consumepart, true, "whether threads should consume partitions or individual pages when building the global hashtable");
DEFINE_bool(adapre, true, "turn local adaptive pre-aggregation on/off initially");
DEFINE_double(thresh, 0.7, "pre-aggregation threshold for disabling local pre-aggregation");
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
static void fn_agg(Aggregates& aggs_grp, const Aggregates& aggs_tup)
{
    std::get<0>(aggs_grp) += std::get<0>(aggs_tup);
}
static void fn_agg_concurrent(Aggregates& aggs_grp, const Aggregates& aggs_tup)
{
    __sync_fetch_and_add(&std::get<0>(aggs_grp), std::get<0>(aggs_tup));
}
/* --------------------------------------- */
static constexpr ht::IDX_MODE idx_mode_slots   = ht::INDIRECT_16;
static constexpr ht::IDX_MODE idx_mode_entries = ht::NO_IDX;
static constexpr bool is_ht_loc_salted         = false;
static constexpr bool is_ht_glob_salted        = true;
/* --------------------------------------- */
using MemAlloc                                 = mem::JEMALLOCator<true>;
using Sketch                                   = ht::HLLSketch<true>;
using PageTable                                = PageLocal<TABLE_SCHEMA>;
using PageHashtable                            = ht::PageAggregation<Groups, Aggregates, idx_mode_entries, idx_mode_slots == ht::DIRECT, true>;
using BlockAlloc                               = mem::BlockAllocatorNonConcurrent<PageHashtable, MemAlloc>;
using BufferLocal                              = buf::EvictionBuffer<PageHashtable, BlockAlloc>;
using StorageGlobal                            = buf::PartitionBuffer<PageHashtable, true>;
using InserterLocal                            = buf::PartitionedAggregationInserter<PageHashtable, idx_mode_entries, BufferLocal, Sketch, true>;
using EgressManager                            = network::HomogeneousEgressNetworkManager<PageHashtable, Sketch>;
using IngressManager                           = network::HomogeneousIngressNetworkManager<PageHashtable, Sketch>;
/* --------------------------------------- */
#if defined(LOCAL_OPEN_HT)
using HashtableLocal = ht::PartitionedOpenAggregationHashtable<Groups, Aggregates, idx_mode_entries, idx_mode_slots, fn_agg, MemAlloc, Sketch, true, is_ht_loc_salted>;
#else
using HashtableLocal = ht::PartitionedChainedAggregationHashtable<Groups, Aggregates, idx_mode_entries, idx_mode_slots, fn_agg, MemAlloc, SketchLocal, true>;
#endif
/* --------------------------------------- */
#if defined(GLOBAL_OPEN_HT)
using HashtableGlobal = ht::ConcurrentOpenAggregationHashtable<Groups, Aggregates, idx_mode_entries, fn_agg_concurrent, MemAlloc, true, is_ht_glob_salted>;
#else
using HashtableGlobal = ht::ConcurrentChainedAggregationHashtable<Groups, Aggregates, fn_agg_concurrent, MemAlloc, true>;
#endif
/* --------------------------------------- */
static_assert(idx_mode_slots != ht::NO_IDX);
