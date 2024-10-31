// -----------------------------------------------------------------------------
// 2023 Maximilian Kuschewski
// -----------------------------------------------------------------------------

// - Modified signature of some functions to match cpc sketch
// - Simplified struct to use u64 as register type

#pragma once

#include <mutex>

#include "defaults.h"

namespace ht {

// adapted from algorithms at https://en.wikipedia.org/wiki/HyperLogLog
struct HLLSketch {
    static constexpr u64 ONE = 1;
    static constexpr u8 WIDTH = sizeof(u64);
    static constexpr u64 REG_CNT = ONE << WIDTH;
    static constexpr u8 CNT = WIDTH * 8;
    static constexpr u8 REG_SHIFT = CNT - WIDTH;
    std::mutex merge_mtx{};
    // estimation correction factor
    static constexpr float ALPHA = 0.7213 / (1 + (1.079 / REG_CNT));

    static constexpr float EST_CORRECT = REG_CNT * REG_CNT * ALPHA;
    static constexpr double EST_MIN_LIM = 2.5 * REG_CNT;

    std::array<u8, REG_CNT> registers{};

    HLLSketch()
    {
        std::fill(registers.begin(), registers.end(), 0);
    }

    void update(u64 hash)
    {
        auto& value = registers[hash >> REG_SHIFT];
        u64 rest = hash << WIDTH;
        uint8_t rank = rest == 0 ? REG_SHIFT : (__builtin_clzl(rest) + 1);
        value = std::max(value, rank);
    }

    u64 get_estimate()
    {
        double sum = 0;
        unsigned zero_cnt = 0;
        for (auto reg : registers) {
            zero_cnt += reg == 0;
            sum += 1.0 / (1ul << reg);
        }
        double est = EST_CORRECT / sum;
        if (zero_cnt == 0 || est >= EST_MIN_LIM) {
            return est;
        }
        else {
            return REG_CNT * ::logl(static_cast<long double>(REG_CNT) / zero_cnt);
        }
    }

    void merge(const HLLSketch& other)
    {
        for (u64 i{0}; i < registers.size(); ++i) {
            registers[i] = std::max(registers[i], other.registers[i]);
        }
    }

    void merge_concurrent(const HLLSketch& other)
    {
        std::unique_lock _{merge_mtx};
        merge(other);
    }

    [[nodiscard]]
    static std::string get_type()
    {
        return "HLL";
    }
};

} // namespace ht
