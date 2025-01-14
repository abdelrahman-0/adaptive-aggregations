#pragma once

#include <ranges>

#define ALWAYS_INLINE __attribute__((always_inline)) inline
#define HOT_FUNC __attribute__((hot))
#define COLD_FUNC __attribute__((cold))
#define DEFAULTS_AUTO static constexpr auto
#define CACHELINE_SZ 64

using s8     = int8_t;
using s16    = int16_t;
using s32    = int32_t;
using s64    = int64_t;
using u8     = uint8_t;
using u16    = uint16_t;
using u32    = uint32_t;
using u64    = uint64_t;

using node_t = u16;

namespace defaults {

DEFAULTS_AUTO hashtable_page_size     = 1ul << HT_PAGE_SIZE_POWER;
DEFAULTS_AUTO local_page_size         = 1ul << 12;
DEFAULTS_AUTO local_io_depth          = 256ul;
DEFAULTS_AUTO num_pages_on_chunk      = 1ul << 6;
DEFAULTS_AUTO network_io_depth        = 128ul;
DEFAULTS_AUTO network_page_size       = 1ul << NETWORK_PAGE_SIZE_POWER;

DEFAULTS_AUTO kernel_recv_buffer_size = 1u << 29;
DEFAULTS_AUTO kernel_send_buffer_size = 1u << 29;
DEFAULTS_AUTO listen_queue_depth      = 100;

// node-specific defaults
DEFAULTS_AUTO node_bandwidth_GB_per_s = 100e9 / 8;

} // namespace defaults
