#pragma once

#include "core/page.h"
#include "defaults.h"

template <u64 page_size, typename Attribute, bool use_ptr = true>
struct PageCommunication : PageRowStore<page_size, Attribute, use_ptr> {
    using PageBase = PageRowStore<page_size, Attribute, use_ptr>;
    using PageBase::num_tuples;
    static constexpr u64 bit_mask_64_primary_bit   = 0x8000000000000000;
    static constexpr u64 bit_mask_64_secondary_bit = 0x4000000000000000;
    static constexpr u64 bit_mask_64_clear_tuples  = ~(bit_mask_64_primary_bit | bit_mask_64_secondary_bit);

    // TODO generalize to n bits?
    void set_primary_bit()
    {
        num_tuples |= bit_mask_64_primary_bit;
    }

    void clear_primary_bit()
    {
        num_tuples &= ~bit_mask_64_primary_bit;
    }
    [[nodiscard]]
    bool is_primary_bit_set() const
    {
        return num_tuples & bit_mask_64_primary_bit;
    }

    void set_secondary_bit()
    {
        num_tuples |= bit_mask_64_secondary_bit;
    }

    void clear_secondary_bit()
    {
        num_tuples &= ~bit_mask_64_secondary_bit;
    }

    [[nodiscard]]
    bool is_secondary_bit_set() const
    {
        return num_tuples & bit_mask_64_secondary_bit;
    }

    [[nodiscard]]
    u64 get_num_tuples() const
    {
        return num_tuples & bit_mask_64_clear_tuples;
    }
};
