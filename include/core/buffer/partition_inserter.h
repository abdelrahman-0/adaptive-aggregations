// Abdelrahman Adel (2024)

#pragma once
#include <atomic>
#include <cmath>
#include <cstdint>
#include <map>
#include <optional>
#include <tbb/concurrent_queue.h>

#include "bench/bench.h"
#include "core/buffer/eviction_buffer.h"
#include "core/hashtable/ht_page.h"
#include "core/hashtable/ht_utils.h"
#include "core/memory/alloc.h"
#include "core/memory/block_allocator.h"
#include "defaults.h"
#include "misc/concepts_traits/concepts_alloc.h"
#include "misc/concepts_traits/concepts_hashtable.h"
#include "misc/exceptions/exceptions_misc.h"

namespace buf {

template <typename key_t, typename value_t, ht::IDX_MODE entry_mode, concepts::is_mem_allocator Alloc, concepts::is_sketch sketch_t, bool is_grouped, bool use_ptr,
          bool is_heterogeneous = false>
struct PartitionedAggregationInserter {
    static constexpr bool is_chained = entry_mode != ht::NO_IDX;
    using page_t = ht::PageAggregation<key_t, value_t, entry_mode, is_chained, use_ptr>;
    using block_alloc_t = mem::BlockAllocator<page_t, Alloc, is_heterogeneous>;
    using part_buf_t = EvictionBuffer<page_t, block_alloc_t>;

    // partitions in the same group share the same sketch
    struct PartitionGroup {
        sketch_t sketch;
    };

  private:
    std::conditional_t<is_grouped, std::vector<PartitionGroup>, PartitionGroup> group_data;
    part_buf_t& part_buffer;
    u32 group_shift{0};
    u8 partition_shift{0};

  public:
    PartitionedAggregationInserter(u32 _npartitions, part_buf_t& _part_buffer, u32 _npartgroups)
    requires(is_grouped)
        : group_data(_npartgroups), part_buffer(_part_buffer), group_shift(__builtin_ctz(_npartitions / _npartgroups)), partition_shift(64 - __builtin_ctz(_npartitions))
    {
        ASSERT(_npartitions == next_power_2(_npartitions));
        ASSERT(_npartgroups == next_power_2(_npartgroups));
        for (auto& grp : group_data) {
            grp = PartitionGroup{sketch_t{_npartgroups}};
        }
    }

    PartitionedAggregationInserter(u32 _npartitions, part_buf_t& _part_buffer)
    requires(not is_grouped)
        : part_buffer(_part_buffer), partition_shift(64 - __builtin_ctz(_npartitions))
    {
        ASSERT(_npartitions == next_power_2(_npartitions));
        ASSERT(_npartgroups == next_power_2(_npartgroups));
    }

    [[maybe_unused]]
    auto insert(const key_t& key, const value_t& value, u64 key_hash, u64 part_no, page_t* part_page)
    requires(is_grouped)
    {
        bool evicted{false};
        if ((evicted = part_page->full())) {
            // evict if full
            part_page = part_buffer.evict(part_no, part_page);
            part_page->clear_tuples();
        }
        group_data[part_no >> group_shift].sketch.update(key_hash);
        return std::pair(part_page->emplace_back_grp(key, value), evicted);
    }

    [[maybe_unused]]
    auto insert(const key_t& key, const value_t& value, u64 key_hash, u64 part_no, page_t* part_page)
    requires(not is_grouped)
    {
        bool evicted{false};
        if ((evicted = part_page->full())) {
            // evict if full
            part_page = part_buffer.evict(part_no, part_page);
            part_page->clear_tuples();
        }
        group_data.sketch.update(key_hash);
        return std::pair(part_page->emplace_back_grp(key, value), evicted);
    }

    [[maybe_unused]]
    auto insert(const key_t& key, const value_t& value, page_t::idx_t offset, u64 key_hash, u64 part_no, page_t* part_page)
    requires(is_grouped)
    {
        bool evicted{false};
        if ((evicted = part_page->full())) {
            // evict if full
            part_page = part_buffer.evict(part_no, part_page);
            part_page->clear_tuples();
            offset = 0;
        }
        group_data[part_no >> group_shift].sketch.update(key_hash);
        return std::pair(part_page->emplace_back_grp(key, value, offset), evicted);
    }

    [[maybe_unused]]
    auto insert(const key_t& key, const value_t& value, page_t::idx_t offset, u64 key_hash, u64 part_no, page_t* part_page)
    requires(not is_grouped)
    {
        bool evicted{false};
        if ((evicted = part_page->full())) {
            // evict if full
            part_page = part_buffer.evict(part_no, part_page);
            part_page->clear_tuples();
            offset = 0;
        }
        group_data.sketch.update(key_hash);
        return std::pair(part_page->emplace_back_grp(key, value, offset), evicted);
    }

    void insert(key_t& key, value_t& value)
    {
        // extract lower bits from hash
        u64 key_hash = hash_tuple(key);
        u64 part_no = key_hash >> partition_shift;
        auto* part_page = part_buffer.get_partition_page(part_no);
        insert(key, value, key_hash, part_no, part_page);
    }

    const auto& get_sketch(u32 grp_no) const
    requires(is_grouped)
    {
        return group_data[grp_no].sketch;
    }

    const auto& get_sketch() const
    requires(not is_grouped)
    {
        return group_data.sketch;
    }

    [[nodiscard]]
    static std::string get_type()
    {
        return "inserter-"s + ht::get_idx_mode_str(entry_mode) + "_entry" + (is_chained ? "-is_chained" : "");
    }
};

} // namespace buf
