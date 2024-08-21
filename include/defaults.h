#pragma once

#include <tbb/partitioner.h>

#include "thread"
#define DEFAULTS_AUTO static constexpr auto

namespace defaults {

DEFAULTS_AUTO local_page_size = 1ul << 12;
DEFAULTS_AUTO local_io_depth = 256ul;
DEFAULTS_AUTO num_pages_on_chunk = 1ul << 6;
static auto shuffle_partitioner = tbb::affinity_partitioner();

DEFAULTS_AUTO network_io_depth = 128ul;
DEFAULTS_AUTO network_page_size = 1ul << 16;
DEFAULTS_AUTO kernel_recv_buffer_size = 1u << 28;
DEFAULTS_AUTO kernel_send_buffer_size = 1u << 28;

DEFAULTS_AUTO subnet = "10.0.0.";
DEFAULTS_AUTO node_port_base = 4u;
//DEFAULTS_AUTO subnet = "192.168.0.";
//DEFAULTS_AUTO node_port_base = 30u;
DEFAULTS_AUTO port = 3500;
DEFAULTS_AUTO listen_queue_depth = 100;

} // namespace defaults
