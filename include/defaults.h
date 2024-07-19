#pragma once

#include <tbb/partitioner.h>

namespace defaults {

static constexpr std::size_t local_page_size = 1 << 12;
static constexpr std::size_t network_page_size = 1 << 12;
static constexpr std::size_t local_io_depth = 256;
static constexpr std::size_t network_io_depth = 256;
static constexpr std::size_t num_pages_on_chunk = 1 << 5;
static auto shuffle_partitioner = tbb::affinity_partitioner();

// 192.168.0.30
static constexpr auto subnet = "192.168.0.";
static constexpr auto sender_host_base = 4u;
static constexpr auto receiver_host_base = 30u;
static constexpr auto port = "3500";

} // namespace defaults
