#pragma once

#include <gflags/gflags.h>

#include "bench/bench.h"
#include "bench/common_flags.h"
#include "core/buffer/page_buffer.h"
#include "core/buffer/partition_buffer.h"
#include "core/hashtable/hashtable_base.h"
#include "core/hashtable/hashtable_global.h"
#include "core/hashtable/hashtable_local.h"
#include "core/network/connection.h"
#include "core/network/network_manager.h"
#include "core/storage/page_local.h"
#include "core/storage/table.h"
#include "defaults.h"
#include "system/node.h"
#include "system/stopwatch.h"
#include "system/topology.h"
#include "utils/hash.h"
#include "utils/utils.h"

/* ----------- CMD LINE PARAMS ----------- */

DEFINE_uint32(threads, 1, "number of threads to use");
DEFINE_uint32(slots, 8192, "number of slots to use per partition");
DEFINE_uint32(bump, 1, "bumping factor to use when allocating memory for partition pages");

/* --------------------------------------- */

// TODO show config clearly as static constexpr vars

#define AGG_VAL 1
#define AGG_KEYS u64

#define GPR_KEYS_IDX 0
#define GRP_KEYS u64

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

using MemAlloc = mem::MMapMemoryAllocator<true>;

/* ----------- HASHTABLE ----------- */

static constexpr bool is_salted = true;
static constexpr bool is_chained = true;
using HashtableLocal = ht::PartitionedOpenAggregationHashtable<ht::DIRECT, Groups, Aggregates, fn_agg, MemAlloc, is_salted, is_chained>;
using HashtableGlobal = ht::ConcurrentChainedAggregationHashtable<Groups, Aggregates, fn_agg, MemAlloc>;

/* ----------- STORAGE ----------- */

using PageHashtable = HashtableLocal::page_t;
using BlockAlloc = mem::BlockAllocator<PageHashtable, MemAlloc, false>;
using Buffer = PartitionBuffer<PageHashtable, BlockAlloc>;
using StorageLocal = PageBuffer<PageHashtable, false>;
using StorageGlobal = PageBuffer<PageHashtable, true>;

#define SCHEMA GRP_KEYS, u32, u32, std::array<char, 4>
using PageTable = PageLocal<SCHEMA>;
