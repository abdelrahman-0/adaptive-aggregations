#pragma once
/* --------------------------------------- */
#include <gflags/gflags.h>
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
#include "utils/configuration.h"
#include "utils/hash.h"
#include "utils/utils.h"
/* --------------------------------------- */
using namespace std::chrono_literals;
/* --------------------------------------- */
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
using BlockAllocEgress                         = mem::BlockAllocatorConcurrent<PageResult, MemAlloc>;
using BlockAllocIngress                        = mem::BlockAllocatorNonConcurrent<PageResult, MemAlloc>;
using BufferLocal                              = buf::EvictionBuffer<PageResult, BlockAllocEgress>;
using StorageGlobal                            = buf::PartitionBuffer<PageResult, true>;
using InserterLocal                            = buf::PartitionedAggregationInserter<PageResult, idx_mode_entries, BufferLocal, Sketch, true>;
using EgressManager                            = network::HeterogeneousEgressNetworkManager<PageResult, Sketch>;
using IngressManager                           = network::HeterogeneousIngressNetworkManager<PageResult, Sketch>;
/* --------------------------------------- */
#if defined(LOCAL_UNCHAINED_HT_16) or defined(LOCAL_UNCHAINED_HT_32) or defined(LOCAL_UNCHAINED_HT_64)
using HashtableLocal = ht::PartitionedOpenAggregationHashtable<Groups, Aggregates, idx_mode_entries, idx_mode_slots, fn_agg, MemAlloc, BlockAllocEgress, Sketch, is_ht_loc_salted, true, true>;
#else
using HashtableLocal = ht::PartitionedChainedAggregationHashtable<Groups, Aggregates, idx_mode_entries, idx_mode_slots, fn_agg, MemAlloc, Sketch, true, true>;
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
        sketches_egress  = other.sketches_egress;
        sketches_ingress = other.sketches_ingress;
    }
};
/* --------------------------------------- */
// balanced qthread-to-nthread mapping
auto find_dedicated_nthread(std::integral auto qthread_id)
{
    u16 qthreads_per_nthread  = FLAGS_qthreads / FLAGS_nthreads;
    auto num_fat_nthreads     = FLAGS_qthreads % FLAGS_nthreads;
    auto num_fat_qthreads     = (qthreads_per_nthread + 1) * num_fat_nthreads;
    bool has_extra_qthread    = qthread_id < num_fat_qthreads;
    qthreads_per_nthread     += has_extra_qthread;
    auto dedicated_nthread    = has_extra_qthread ? (qthread_id / qthreads_per_nthread) : num_fat_nthreads + ((qthread_id - num_fat_qthreads) / qthreads_per_nthread);
    auto qthread_local_id     = has_extra_qthread ? (qthread_id - dedicated_nthread * qthreads_per_nthread) : (qthread_id - num_fat_nthreads - dedicated_nthread * qthreads_per_nthread);
    return std::make_tuple(dedicated_nthread, qthreads_per_nthread, qthread_local_id);
}
