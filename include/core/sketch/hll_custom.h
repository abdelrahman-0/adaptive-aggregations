// -----------------------------------------------------------------------------
// 2023 Maximilian Kuschewski
// -----------------------------------------------------------------------------

// - Changed implementation to use trailing zeros
// - Modified signature of some functions to match cpc sketch
// - Simplified struct to use u64 as register type

#pragma once

#include <algorithm>
#include <array>
#include <cmath>

#include "defaults.h"

namespace ht {

// adapted from algorithms at https://en.wikipedia.org/wiki/HyperLogLog
template <bool is_grouped = false>
struct HLLSketch {
    static constexpr u64 ONE            = 1;
    static constexpr u8 WIDTH           = sizeof(u64);
    static constexpr u64 REG_CNT        = ONE << WIDTH;
    static constexpr u8 CNT             = WIDTH * 8;
    static constexpr u8 REG_SHIFT       = CNT - WIDTH;
    static constexpr u8 REG_MASK        = 0xFF;
    // estimation correction factor
    static constexpr float ALPHA        = 0.7213 / (1 + (1.079 / REG_CNT));

    static constexpr float EST_CORRECT  = REG_CNT * REG_CNT * ALPHA;
    static constexpr double EST_MIN_LIM = 2.5 * REG_CNT;

    std::array<u8, REG_CNT> registers{};
    u64 group_mask{};

    HLLSketch()
    requires(not is_grouped)
    {
        clear();
    }

    explicit HLLSketch(u32 ngroups = 0)
    requires(is_grouped)
    {
        ASSERT(ngroups == 0 or ngroups == next_power_2(ngroups));
        group_mask = 0xFFFFFFFFFFFFFFFF >> __builtin_ctz(ngroups);
        clear();
    }

    HLLSketch(const HLLSketch& other) noexcept
    {
        registers  = other.registers;
        group_mask = other.group_mask;
    }

    HLLSketch& operator=(const HLLSketch& other)
    {
        registers  = other.registers;
        group_mask = other.group_mask;
        return *this;
    }

    void clear()
    {
        std::fill(registers.begin(), registers.end(), 0);
    }

    void update(u64 hash)
    {
        auto& value = registers[hash & REG_MASK];
        if constexpr (is_grouped) {
            // get rid of constant top bits
            hash &= group_mask;
        }
        u64 rest     = hash >> WIDTH;
        uint8_t rank = rest == 0 ? REG_SHIFT : (__builtin_ctzl(rest) + 1);
        value        = std::max(value, rank);
    }

    [[nodiscard]]
    u64 get_estimate() const
    {
        double sum        = 0;
        unsigned zero_cnt = 0;
        for (auto reg : registers) {
            zero_cnt += reg == 0;
            sum      += 1.0 / (1ul << reg);
        }
        double est = EST_CORRECT / sum;
        if (zero_cnt == 0 || est >= EST_MIN_LIM) {
            return est;
        }
        return REG_CNT * ::logl(static_cast<long double>(REG_CNT) / zero_cnt);
    }

    void merge(const HLLSketch& other)
    {
        for (u64 i{0}; i < registers.size(); ++i) {
            registers[i] = std::max(registers[i], other.registers[i]);
        }
    }

    void merge_concurrent(const HLLSketch& other)
    {
        for (u64 i{0}; i < registers.size(); ++i) {
            u8& current_value = registers[i];
            const u8& other_value   = other.registers[i];
            while ((current_value < other_value) && !__sync_bool_compare_and_swap(&current_value, current_value, other_value))
                ;
        }
    }

    [[nodiscard]]
    static std::string get_type()
    {
        return "HLL";
    }
};

} // namespace ht
