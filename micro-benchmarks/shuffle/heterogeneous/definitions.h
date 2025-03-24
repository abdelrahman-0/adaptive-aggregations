#pragma once
/* --------------------------------------- */
#include <gflags/gflags.h>
#include <thread>
#include <utils/configuration.h>

#include "bench/bench.h"
#include "bench/common_flags.h"
#include "bench/stopwatch.h"
#include "core/buffer/partition_buffer.h"
#include "core/buffer/partition_inserter.h"
#include "core/network/connection.h"
#include "core/network/network_manager.h"
#include "core/network/page_communication.h"
#include "core/sketch/hll_custom.h"
#include "core/storage/page_local.h"
#include "core/storage/table.h"
#include "defaults.h"
#include "system/node.h"
/* --------------------------------------- */
using namespace std::chrono_literals;
/* --------------------------------------- */
DEFINE_uint32(nthreads, 1, "number of network threads to use");
DEFINE_uint32(qthreads, 1, "number of query-processing threads to use");
DEFINE_uint32(bump, 1, "bumping factor to use when allocating memory for partition pages");
/* --------------------------------------- */
// The table's schema. The first column needs to be a u64 and is populated with FLAGS_groups unique values.
#define TABLE_SCHEMA u64, u64, u64, u64, double, double, double, double, char, char, s32, s32, s32, std::array<char, 25>, std::array<char, 10>, std::array<char, 44>
#define KEY_IDXS 0 // index of partition key(s) from above TABLE_SCHEMA. Multiple keys can be passed as follows: 0,3,7
#define TUPLE_IDXS 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 // index of result tuple's attributes from above TABLE_SCHEMA
/* --------------------------------------- */
using MemAlloc          = mem::JEMALLOCator<true>;
using PageTable         = PageLocal<TABLE_SCHEMA>;
using ResultTuple       = std::tuple<TABLE_SCHEMA>;
using PageResult        = PageCommunication<defaults::network_page_size, ResultTuple>;
using BlockAllocEgress  = mem::BlockAllocatorConcurrent<PageResult, MemAlloc>;
using BlockAllocIngress = mem::BlockAllocatorNonConcurrent<PageResult, MemAlloc>;
using BufferLocal       = buf::EvictionBuffer<PageResult, BlockAllocEgress>;
using InserterLocal     = buf::PartitionedTupleInserter<PageResult, BufferLocal>;
using EgressManager     = network::HeterogeneousEgressNetworkManager<PageResult>;
using IngressManager    = network::HeterogeneousIngressNetworkManager<PageResult>;

/* --------------------------------------- */
struct QueryThreadGroup
{
    BlockAllocEgress* alloc_egress{nullptr};
    BlockAllocIngress* alloc_ingress{nullptr};

    EgressManager* egress_mgr{nullptr};
    IngressManager* ingress_mgr{nullptr};

    // one egress allocator per query thread
    std::atomic<u32> qthreads_added_last_page{0};
    std::atomic<u32> peers_done{0};
    std::atomic<bool> all_peers_done{false};
    std::atomic<bool> all_qthreads_added_last_page{false};

    explicit QueryThreadGroup() = default;

    QueryThreadGroup(const QueryThreadGroup&)
    {
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
