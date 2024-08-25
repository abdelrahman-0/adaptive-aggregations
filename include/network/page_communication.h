#pragma once

#include "common/page.h"
#include "defaults.h"

static constexpr uint32_t highest_bit_mask_32 = static_cast<uint32_t>(1) << 31;

template <typename... Attributes>
struct PageCommunication : public Page<defaults::network_page_size, Attributes...> {
    using Page<defaults::network_page_size, Attributes...>::num_tuples;

  public:
    void set_last_page() { num_tuples |= highest_bit_mask_32; }

    void clear_last_page() { num_tuples &= ~highest_bit_mask_32; }

    [[nodiscard]] bool is_last_page() const { return num_tuples & highest_bit_mask_32; }
};
