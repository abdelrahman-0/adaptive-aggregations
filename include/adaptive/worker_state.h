#pragma once

#include "defaults.h"
#include "common.h"

namespace adapt {

struct WorkerQueryState {
    static constexpr u16 max_outstanding{1};
    static constexpr u16 max_counter_offers{1};
    Task last_task{};
    u16 num_outstanding{0};
    u16 num_remainder_tasks{0};

    WorkerQueryState()                        = default;

    WorkerQueryState(const WorkerQueryState&) = default;

    [[nodiscard]]
    bool can_accept_work() const
    {
        return (num_outstanding < max_outstanding) and (num_remainder_tasks < max_counter_offers);
    }

    void sent_task()
    {
        num_outstanding++;
    }

    bool handle_response(const StateMessage& msg)
    {
        num_outstanding--;
        bool requires_workers  = msg.requires_more_workers();
        num_remainder_tasks  += requires_workers;
        return requires_workers;
    }
};

} // namespace adapre
