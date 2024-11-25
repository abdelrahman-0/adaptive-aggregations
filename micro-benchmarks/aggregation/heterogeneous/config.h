#pragma once

#include <gflags/gflags.h>
#include <span>
#include <thread>

#include "bench/bench.h"
#include "bench/common_flags.h"
#include "bench/stopwatch.h"
#include "common/alignment.h"
#include "core/buffer/partition_buffer.h"
#include "core/buffer/partition_inserter.h"
#include "core/hashtable/ht_global.h"
#include "core/hashtable/ht_local.h"
#include "core/network/connection.h"
#include "core/network/network_manager.h"
#include "core/sketch/cpc_wrapper.h"
#include "core/sketch/hll_custom.h"
#include "core/storage/page_local.h"
#include "core/storage/table.h"
#include "defaults.h"
#include "system/node.h"
#include "system/topology.h"
#include "utils/hash.h"
#include "utils/utils.h"

using namespace std::chrono_literals;

/* ----------- CMD LINE PARAMS ----------- */

DEFINE_uint32(nthreads, 1, "number of network threads to use");
DEFINE_uint32(qthreads, 1, "number of query-processing threads to use");
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

using MemAlloc = mem::JEMALLOCator<true>;

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

using Sketch = ht::HLLSketch<true>;
// cannot use default CPCSketch since it allocates vectors via std::allocator (part of the sketch is on the heap, so it cannot be sent directly)
// => the solution is to pass a custom allocator to the underlying sketch type as follows:
//       cpc_sketch_alloc<std::allocator<uint8_t>>
//    and route the allocated memory block via the network (i.e. serialize the sketch via the allocator). It is also necessary to dehydrate any
//    pointers and make them use relative addressing inside the allocated block
using SketchGlobal = std::conditional_t<std::is_same_v<Sketch, ht::CPCSketch>, ht::CPCUnion, Sketch>;

static constexpr ht::IDX_MODE idx_mode_slots = ht::INDIRECT_16;
static constexpr ht::IDX_MODE idx_mode_entries = ht::NO_IDX;
static constexpr bool do_adaptive_preagg = true;

static_assert(idx_mode_slots != ht::NO_IDX);

#if defined(LOCAL_OPEN_HT)
static constexpr bool is_ht_loc_salted = true;
using HashtableLocal = ht::PartitionedOpenAggregationHashtable<Groups, Aggregates, idx_mode_entries, idx_mode_slots, fn_agg, MemAlloc, Sketch, true,
                                                               is_ht_loc_salted and idx_mode_slots != ht::INDIRECT_16, true>;
#else
using HashtableLocal = ht::PartitionedChainedAggregationHashtable<Groups, Aggregates, idx_mode_entries, idx_mode_slots, fn_agg, MemAlloc, SketchLocal, true, true>;
#endif
using InserterLocal = buf::PartitionedAggregationInserter<Groups, Aggregates, idx_mode_entries, MemAlloc, Sketch, true, idx_mode_slots == ht::DIRECT, true>;

#if defined(GLOBAL_OPEN_HT)
static constexpr bool is_ht_glob_salted = true;
using HashtableGlobal = ht::ConcurrentOpenAggregationHashtable<Groups, Aggregates, idx_mode_entries, fn_agg_concurrent, MemAlloc, true, is_ht_glob_salted>;
#else
using HashtableGlobal = ht::ConcurrentChainedAggregationHashtable<Groups, Aggregates, fn_agg_concurrent, MemAlloc, true>;
#endif

using PageHashtable = HashtableLocal::page_t;

/* ----------- STORAGE ----------- */

using BlockAllocEgress = mem::BlockAllocatorConcurrent<PageHashtable, MemAlloc>;
using BlockAllocIngress = mem::BlockAllocatorNonConcurrent<PageHashtable, MemAlloc>;
using BufferLocal = buf::EvictionBuffer<PageHashtable, BlockAllocEgress>;
using StorageGlobal = buf::PartitionBuffer<PageHashtable, true>;

// #define SCHEMA GRP_KEYS, u32, u32, std::array<char, 4>
//  TPCH lineitem
#define SCHEMA GRP_KEYS, u64, u64, u64, double, double, double, double, char, char, s32, s32, s32, std::array<char, 25>, std::array<char, 10>, std::array<char, 44>
// #define SCHEMA GRP_KEYS

using PageTable = PageLocal<SCHEMA>;

/* ----------- NETWORK ----------- */

using EgressManager = network::HeterogeneousEgressNetworkManager<PageHashtable, SketchGlobal>;
using IngressManager = network::HeterogeneousIngressNetworkManager<PageHashtable, SketchGlobal>;

struct QueryThreadGroup {
    std::vector<Sketch> sketches_ingress{};
    std::vector<Sketch> sketches_egress{};
    BlockAllocEgress* alloc_egress{nullptr};
    BlockAllocIngress* alloc_ingress{nullptr};

    EgressManager* egress_mgr{nullptr};
    IngressManager* ingress_mgr{nullptr};

    // one egress allocator per query thread
    std::atomic<u32> qthreads_added_last_page{0};
    std::atomic<u32> peers_done{0};
    std::atomic<bool> all_peers_done{false};
    std::atomic<bool> all_qthreads_added_last_page{false};

    explicit QueryThreadGroup(u32 npeers) : sketches_ingress(npeers), sketches_egress(npeers) {};

    QueryThreadGroup(const QueryThreadGroup& other)
    {
        sketches_egress = other.sketches_egress;
        sketches_ingress = other.sketches_ingress;
    }
};
