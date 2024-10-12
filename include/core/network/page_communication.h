#pragma once

#include "core/page.h"
#include "defaults.h"

static constexpr u32 highest_bit_mask_32 = static_cast<u32>(1) << 31;
static constexpr u64 highest_bit_mask_64 = static_cast<u64>(1) << 63;

template <u64 page_size, typename Attribute, bool use_ptr = true>
struct PageCommunication : public PageRowStore<page_size, Attribute, use_ptr> {
    using PageBase = PageRowStore<page_size, Attribute, use_ptr>;
    using PageBase::num_tuples;

  public:
    void set_last_page() { num_tuples |= highest_bit_mask_64; }

    void clear_last_page() { num_tuples &= ~highest_bit_mask_64; }

    [[nodiscard]]
    bool is_last_page() const
    {
        return num_tuples & highest_bit_mask_64;
    }

    [[nodiscard]]
    u64 get_num_tuples() const
    {
        return num_tuples & ~highest_bit_mask_64;
    }
};
