#pragma once

#include <gflags/gflags.h>

#include "bench/bench.h"
#include "bench/common_flags.h"
#include "core/buffer/page_buffer.h"
#include "core/buffer/partition_buffer.h"
#include "core/hashtable/hashtable.h"
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

/* ----------- SCHEMA ----------- */

#define KEYS_AGG u64
#define KEYS_IDX 0
#define KEYS_GRP u64
#define SCHEMA KEYS_GRP, u32, u32, std::array<char, 4>

using TablePage = PageLocal<SCHEMA>;

using GroupAttributes = std::tuple<KEYS_GRP>;
using AggregateAttributes = std::tuple<KEYS_AGG>;
auto aggregate_fn = [](AggregateAttributes& aggs_grp, const AggregateAttributes& aggs_tup) { std::get<0>(aggs_grp) += std::get<0>(aggs_tup); };

using MemAlloc = mem::MMapMemoryAllocator<true>;

static constexpr bool salted = true;

template <bool is_global>
using Hashtable = hashtable::PartitionedOpenHashtable<GroupAttributes, AggregateAttributes, void*, salted, aggregate_fn, MemAlloc, false, is_global>;

using HashtableLocal = Hashtable<false>;
using HashtableGlobal = Hashtable<true>;

using BufferPage = HashtableLocal::HashtablePage;
using BlockAlloc = mem::BlockAllocator<BufferPage, MemAlloc, false>;
using Buffer = PartitionBuffer<BufferPage, BlockAlloc>;
using StorageLocal = PageBuffer<BufferPage, false>;
using StorageGlobal = PageBuffer<BufferPage, true>;
