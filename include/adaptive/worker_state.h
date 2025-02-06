#pragma once

#include "common.h"
#include "defaults.h"

namespace adapt {

struct WorkerQueryState {
    static constexpr u16 max_outstanding{1};
    static constexpr u16 max_counter_offers{1};
    Task last_task{};
    u16 num_outstanding{0};

    WorkerQueryState()                        = default;

    WorkerQueryState(const WorkerQueryState&) = default;

    [[nodiscard]]
    bool can_accept_work() const
    {
        return num_outstanding > 0;
    }

    void sent_task()
    {
        num_outstanding++;
    }

    bool handle_response(const StateMessage& msg)
    {
        num_outstanding--;
        return msg.requires_more_workers();
    }
};

} // namespace adapt
