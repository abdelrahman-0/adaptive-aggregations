#pragma once

#include <gflags/gflags.h>
#include <thread>

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

using namespace std::chrono_literals;
/* --------------------------------------- */
DEFINE_uint32(threads, 1, "number of threads to use");
DEFINE_uint32(bump, 1, "bumping factor to use when allocating memory for partition pages");
/* --------------------------------------- */
#define TABLE_SCHEMA u64, u64, u64, u64, double, double, double, double, char, char, s32, s32, s32, std::array<char, 25>, std::array<char, 10>, std::array<char, 44>
#define KEY_IDXS 0
#define TUPLE_IDXS 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
/* --------------------------------------- */
using MemAlloc       = mem::JEMALLOCator<true>;
using PageTable      = PageLocal<TABLE_SCHEMA>;
using ResultTuple    = std::tuple<TABLE_SCHEMA>;
using PageResult     = PageCommunication<defaults::network_page_size, ResultTuple>;
using BlockAlloc     = mem::BlockAllocatorNonConcurrent<PageResult, MemAlloc>;
using BufferLocal    = buf::EvictionBuffer<PageResult, BlockAlloc>;
using InserterLocal  = buf::PartitionedTupleInserter<PageResult, BufferLocal>;
using EgressManager  = network::HomogeneousEgressNetworkManager<PageResult>;
using IngressManager = network::HomogeneousIngressNetworkManager<PageResult>;
/* --------------------------------------- */
