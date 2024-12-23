#pragma once

#include <string>

namespace ht {

enum IDX_MODE : u8 {
    NO_IDX = 0,
    DIRECT,
    INDIRECT_16,
    INDIRECT_32,
    INDIRECT_64,
};

std::string get_idx_mode_str(IDX_MODE mode)
{
    switch (mode) {
    case NO_IDX:
        return "";
    case DIRECT:
        return "direct";
    case INDIRECT_16:
        return "u16";
    case INDIRECT_32:
        return "u32";
    case INDIRECT_64:
        return "u64";
    }
    return "UNKNOWN";
}

} // namespace ht
