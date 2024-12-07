#pragma once

#include "core/memory/block_allocator.h"
#include "defaults.h"

namespace adapre {

// a task offer is simply a page interval [start, end) to process
struct TaskOffer {
    u32 start;
    u32 end;

    auto operator<=>(const TaskOffer&) const = default;

    explicit operator bool() const
    {
        // true for valid offers
        return start < end;
    }

    auto operator-(const TaskOffer& other) const
    {
        // a response-offer is always a subset of the original offer
        // so the difference [60, 100) - [60, 80) is equal to [80, 100)
        ENSURE(other <= *this);
        return TaskOffer{other.end, end};
    };
};

enum MSG_TYPE : s8 {
    TASK_OFFER           = 0,
    TASK_OFFER_RESPONSE = 1,
    NUM_WORKERS_UPDATE   = 2,
    QUERY_END            = 3,
};

struct StateMessage {
    TaskOffer offer;
    u16 nworkers;
    MSG_TYPE type;

    [[nodiscard]]
    bool requires_more_workers() const
    {
        ENSURE(type == TASK_OFFER_RESPONSE);
        return nworkers > 0;
    }
};

struct MessageBuffer {
    mem::BlockAllocatorNonConcurrent<StateMessage, mem::JEMALLOCator<false>> allocator;

    MessageBuffer() : allocator{100, 1}
    {
    }

    auto* get_message_storage()
    {
        return allocator.get_object();
    }

    void return_message(StateMessage* message)
    {
        allocator.return_object(message);
    }
};

static_assert(sizeof(StateMessage) == 12);

} // namespace adapt
