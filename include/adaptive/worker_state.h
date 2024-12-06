#pragma once

#include <bench/bench.h>

#include "core/network/network_manager.h"
#include "defaults.h"
#include "state_messages.h"

namespace adapt {

struct WorkerQueryState {
    static constexpr u16 max_outstanding{2};
    static constexpr u16 max_counter_offers{1};
    TaskOffer last_offer{};
    u16 num_outstanding{0};
    u16 num_remainder_offers{0};

    WorkerQueryState()                        = default;

    WorkerQueryState(const WorkerQueryState&) = default;

    [[nodiscard]]
    bool can_accept_work() const
    {
        return (num_outstanding < max_outstanding) and (num_remainder_offers < max_counter_offers);
    }

    void sent_offer()
    {
        num_outstanding++;
    }

    bool handle_response(const StateMessage& msg)
    {
        num_outstanding--;
        bool requires_workers  = msg.requires_more_workers();
        num_remainder_offers  += requires_workers;
        return requires_workers;
    }
};

struct WorkersManager {
    network::HomogeneousEgressNetworkManager<StateMessage>& egress_mgr;
    network::HomogeneousIngressNetworkManager<StateMessage>& ingress_mgr;
    MessageBuffer message_buffer{};
    std::vector<WorkerQueryState> worker_states;
    u16 max_workers{};
    u16 active_workers{};

    explicit WorkersManager(auto& egress_mgr, auto& ingress_mgr, std::integral auto nworkers, std::integral auto max_workers)
        : egress_mgr{egress_mgr}, ingress_mgr{ingress_mgr}, worker_states(max_workers, WorkerQueryState{}), max_workers{static_cast<u16>(max_workers)},
          active_workers{static_cast<u16>(nworkers)}
    {
        ENSURE(nworkers <= max_workers);
        std::function message_fn = [&](StateMessage* msg, u32 dst) {
            message_buffer.return_message(msg);
            if (worker_states[dst].handle_response(*msg)) {
                // worker did not fully accept offer
                u16 navailable{0};
                std::for_each(worker_states.begin(), worker_states.begin() + active_workers, [&](const WorkerQueryState& s) { navailable += s.can_accept_work(); });
                if (navailable < msg->nworkers) {
                    // increase workers
                    active_workers += msg->nworkers - navailable;
                }
                ENSURE(active_workers <= max_workers);

                // split remainder offer between available nodes
                u32 start  = msg->offer.start;
                u32 end    = msg->offer.end;
                u32 offset = (end - start) / msg->nworkers;
                for (u16 worker_id : range(active_workers)) {
                    if (worker_states[worker_id].can_accept_work()) {
                        // send offer
                        auto* msg_egress =
                            new (message_buffer.get_message_storage()) StateMessage{.offer = TaskOffer{start, std::min(start + offset, end)}, .nworkers = active_workers, .type = TASK_OFFER};
                        egress_mgr.send(worker_id, msg_egress);

                        // recv offer response
                        ingress_mgr.recv(worker_id, message_buffer.get_message_storage());

                        // update state
                        worker_states[worker_id].sent_offer();
                        start += offset;
                    }
                    else {
                        // send update
                        auto* msg_response = new (message_buffer.get_message_storage()) StateMessage{.nworkers = active_workers, .type = NUM_WORKERS_UPDATE};
                        egress_mgr.send(worker_id, msg_response);
                    }
                }
            }
        };
        ingress_mgr.register_consumer_fn(message_fn);
    }

    void finalize_query() const
    {
        StateMessage message{.type = QUERY_END};
        egress_mgr.broadcast(&message);
        egress_mgr.wait_all();
        print("coordinator done");
    }
};

} // namespace adapt
