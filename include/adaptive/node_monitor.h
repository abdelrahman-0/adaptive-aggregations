#pragma once

#include <bench/bench.h>

#include "core/network/network_manager.h"
#include "worker_state.h"

namespace adapre {

template <typename EgressMgr, typename IngressMgr>
class NodeMonitor {
  protected:
    EgressMgr& egress_mgr;
    IngressMgr& ingress_mgr;
    MessageBuffer msg_buffer{};

    NodeMonitor(EgressMgr& _egress_mgr, IngressMgr& _ingress_mgr) : egress_mgr(_egress_mgr), ingress_mgr(_ingress_mgr)
    {
    }
};

template <typename EgressMgr, typename IngressMgr>
class CoordinatorMonitor : NodeMonitor<EgressMgr, IngressMgr> {
    using base_t = NodeMonitor<EgressMgr, IngressMgr>;
    using base_t::egress_mgr;
    using base_t::ingress_mgr;
    using base_t::msg_buffer;

    std::vector<WorkerQueryState> worker_states;
    u16 max_workers{};
    u16 active_workers{};

    void initialize_query(u32 start, u32 end)
    {
        // kickstart query
        u32 offset = (end - start) / active_workers;
        for (u16 worker_id : range(active_workers)) {
            auto* msg = new (msg_buffer.get_message_storage()) StateMessage{.offer = TaskOffer{start, std::min(start + offset, end)}, .nworkers = active_workers, .type = TASK_OFFER};
            egress_mgr.send(worker_id, msg);
            ingress_mgr.recv(worker_id, msg_buffer.get_message_storage());
            worker_states[worker_id].sent_offer();
        }
    }

    void monitor_query_state() const
    {
        while (ingress_mgr.has_inflight()) {
            ingress_mgr.consume_done();
            egress_mgr.try_drain_pending();
        }
    }

    void finalize_query() const
    {
        StateMessage message{.type = QUERY_END};
        egress_mgr.broadcast(&message);
        egress_mgr.wait_all();
    }

  public:
    explicit CoordinatorMonitor(std::integral auto nworkers, std::integral auto max_workers, EgressMgr& _egress_mgr, IngressMgr& _ingress_mgr)
        : base_t(_egress_mgr, _ingress_mgr), worker_states(max_workers, WorkerQueryState{}), max_workers{static_cast<u16>(max_workers)}, active_workers{static_cast<u16>(nworkers)}
    {
        ENSURE(nworkers <= max_workers);
        std::function message_fn_ingress = [&](StateMessage* msg, u32 dst) {
            if (worker_states[dst].handle_response(*msg)) {
                // worker did not fully accept offer
                u16 navailable{0};
                std::for_each(worker_states.begin(), worker_states.begin() + active_workers, [&](const WorkerQueryState& s) { navailable += s.can_accept_work(); });
                bool increased_workers{false};
                if (navailable < msg->nworkers) {
                    // increase workers
                    active_workers    += msg->nworkers - navailable;
                    increased_workers  = true;
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
                            new (msg_buffer.get_message_storage()) StateMessage{.offer = TaskOffer{start, std::min(start + offset, end)}, .nworkers = active_workers, .type = TASK_OFFER};
                        egress_mgr.send(worker_id, msg_egress);

                        // recv offer response
                        ingress_mgr.recv(worker_id, msg_buffer.get_message_storage());

                        // update state
                        worker_states[worker_id].sent_offer();
                        start += offset;
                    }
                    else if (increased_workers) {
                        // send update
                        auto* msg_response = new (msg_buffer.get_message_storage()) StateMessage{.nworkers = active_workers, .type = NUM_WORKERS_UPDATE};
                        _egress_mgr.send(worker_id, msg_response);
                    }
                }
            }
            msg_buffer.return_message(msg);
        };
        ingress_mgr.register_consumer_fn(message_fn_ingress);
        std::function message_fn_egress = [&](StateMessage* msg) { msg_buffer.return_message(msg); };
        egress_mgr.register_consumer_fn(message_fn_egress);
    }

    void process_query(u32 start, u32 end)
    {
        initialize_query(start, end);
        monitor_query_state();
        finalize_query();
    }
};

template <typename EgressMgr, typename IngressMgr>
class WorkerMonitor : NodeMonitor<EgressMgr, IngressMgr> {
    using base_t = NodeMonitor<EgressMgr, IngressMgr>;
    using base_t::egress_mgr;
    using base_t::ingress_mgr;
    using base_t::msg_buffer;
    tbb::concurrent_vector<TaskOffer> work;
    std::atomic<u16> active_workers{0};
    std::atomic<bool> num_workers_finalized{false};

    void initialize_query()
    {
        ingress_mgr.recv(0, msg_buffer.get_message_storage());
    }

    void monitor_query_state() const
    {
        while (not num_workers_finalized) {
            ingress_mgr.consume_done();
            egress_mgr.try_drain_pending();
        }
    }

  public:
    explicit WorkerMonitor(EgressMgr& _egress_mgr, IngressMgr& _ingress_mgr) : base_t(_egress_mgr, _ingress_mgr)
    {
        work.reserve(128);
        std::function message_fn_ingress = [this](StateMessage* msg, u32) {
            switch (msg->type) {
            case TASK_OFFER:
                ingress_mgr.recv(0, msg_buffer.get_message_storage());
                work.push_back(msg->offer);
                break;
            case NUM_WORKERS_UPDATE:
                ingress_mgr.recv(0, msg_buffer.get_message_storage());
                active_workers = msg->nworkers;
                break;
            case QUERY_END:
                num_workers_finalized = true;
                break;
            default:
                __builtin_unreachable();
            }
            msg_buffer.return_message(msg);
        };
        ingress_mgr.register_consumer_fn(message_fn_ingress);
        std::function message_fn_egress = [&](StateMessage* msg) { msg_buffer.return_message(msg); };
        egress_mgr.register_consumer_fn(message_fn_egress);
    }

    void process_query()
    {
        initialize_query();
        monitor_query_state();
    }

    void send_remainder_offer(u16 nworkers, u32 start, u32 end)
    {
        egress_mgr.send(0, new (msg_buffer.get_message_storage()) StateMessage{.offer = TaskOffer{start, end}, .nworkers = nworkers, .type = TASK_OFFER_RESPONSE});
    }
};

} // namespace adapre
