#pragma once

#include "common/page.h"
#include "defaults.h"

static constexpr u32 highest_bit_mask_32 = static_cast<u32>(1) << 31;
static constexpr u64 highest_bit_mask_64 = static_cast<u64>(1) << 63;

template <typename... Attributes>
struct PageCommunication : public Page<defaults::network_page_size, Attributes...> {
    using Page<defaults::network_page_size, Attributes...>::num_tuples;

  public:
    void set_last_page() { num_tuples |= highest_bit_mask_64; }

    void clear_last_page() { num_tuples &= ~highest_bit_mask_64; }

    [[nodiscard]] bool is_last_page() const { return num_tuples & highest_bit_mask_64; }

    [[nodiscard]] u64 get_num_tuples() const { return num_tuples & ~highest_bit_mask_64; }
};
