#pragma once

namespace adapt {
struct PolicyDefault {
    [[nodiscard]]
    static bool should_scale_out()
    {
        // TODO calculate if need to scale out and respond and SLA and minimum answer time and maximum answer time
        return false;
    }
};

// TODO static polict

using AdaptivePolicy = PolicyDefault;

} // namespace adapt
