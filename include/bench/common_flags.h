#pragma once

#include <gflags/gflags.h>

// DEFINE_bool(local, true, "run benchmark using loop-back interface");
DEFINE_uint32(nodes, 1, "total number of num_nodes to use");
DEFINE_uint32(depthio, 256, "submission queue size of storage uring");
DEFINE_uint32(depthnw, 256, "submission queue size of network uring");
DEFINE_bool(sqpoll, false, "whether to use kernel-sided submission queue polling");

DEFINE_string(config, "../../configs/config_local.json", "path to config file");

DEFINE_bool(random, true, "whether to use randomly generated data instead of reading in a file");
DEFINE_uint32(npages, 2, "number of random pages to generate (only applicable if 'random' flag is set)");
DEFINE_string(path, "data/random.tbl", "path to input relation (ignored if 'random' flag is set)");
DEFINE_uint32(cache, 100, "percentage of table to cache in-memory in range [0,100] (ignored if 'random' flag is set)");
DEFINE_uint32(morselsz, 1000, "number of pages to process in one morsel");
DEFINE_bool(sequential_io, true, "whether to use sequential or random I/O for cached swips");

DEFINE_uint32(partitions, 64, "number of hashtable partitions (per node) to use");
DEFINE_uint32(partgrpsz, 4, "number of partitions per partition group");
DEFINE_uint64(groups, 1, "number of unique groups to use");
DEFINE_uint64(seed, 0, "random generator seed");

DEFINE_bool(pin, true, "pin threads using balanced affinity at core granularity");
DEFINE_uint32(maxalloc, 50'000, "maximum number of calls the block allocator can be called");
DEFINE_bool(print_header, true, "whether to print metrics header (only valid for csv output, see 'csv' flag)");
DEFINE_bool(csv, true, "whether to print output metrics as csv");
