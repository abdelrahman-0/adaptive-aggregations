#pragma once

#include <bench/bench.h>
#include <deque>
#include <numeric>

#include "core/network/network_manager.h"
#include "task_metrics.h"
#include "task_scheduler.h"
#include "worker_state.h"

namespace adapt {

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
class WorkerMonitor : NodeMonitor<EgressMgr, IngressMgr> {
    using base_t = NodeMonitor<EgressMgr, IngressMgr>;
    using base_t::egress_mgr;
    using base_t::ingress_mgr;
    using base_t::msg_buffer;
    TaskScheduler& task_scheduler;
    bool received_first_task{false};

    void initialize_query()
    {
        ingress_mgr.recv(0, msg_buffer.get_message_storage());
    }

    void send_task_response(u16 nworkers, Task task_response)
    {
        print("worker: sending task response (", task_response.start, task_response.end, ") with workers =", nworkers);
        egress_mgr.send(0, new (msg_buffer.get_message_storage()) StateMessage{task_response, nworkers, TASK_OFFER_RESPONSE});
    }

    void check_query_state()
    {
        // check task_scheduler's current query]
        std::optional<std::pair<u16, Task>> res;
        if (received_first_task and ((res = task_scheduler.get_task_state()))) {
            send_task_response(res->first, res->second);
        }
    }

  public:
    explicit WorkerMonitor(EgressMgr& _egress_mgr, IngressMgr& _ingress_mgr, TaskScheduler& _adaptive_state) : base_t(_egress_mgr, _ingress_mgr), task_scheduler(_adaptive_state)
    {
        std::function message_fn_ingress = [this](StateMessage* msg, u32) {
            switch (msg->type) {
            case TASK_OFFER:
                ingress_mgr.recv(0, msg_buffer.get_message_storage());
                print("worker: received task offer (", msg->task.start, msg->task.end, ") with workers =", msg->nworkers, "from coordinator");
                received_first_task = true;
                task_scheduler.enqueue_task(msg->task);
                task_scheduler.update_nworkers(msg->nworkers);
                break;
            case NUM_WORKERS_UPDATE:
                print("worker: received num workers update (", msg->nworkers, ")");
                ingress_mgr.recv(0, msg_buffer.get_message_storage());
                task_scheduler.update_nworkers(msg->nworkers);
                break;
            case QUERY_END:
                print("worker: received QUERY_END");
                task_scheduler.nworkers_is_stable = true;
                break;
            default:
                ENSURE(false);
            }
            msg_buffer.return_message(msg);
        };
        ingress_mgr.register_consumer_fn(message_fn_ingress);
        std::function message_fn_egress = [&](StateMessage* msg) { msg_buffer.return_message(msg); };
        egress_mgr.register_consumer_fn(message_fn_egress);
    }

    void monitor_query()
    {
        initialize_query();
        while (not task_scheduler.nworkers_is_stable) {
            ingress_mgr.consume_done();
            egress_mgr.try_drain_pending();
            check_query_state();
        }
    }
};

template <typename EgressMgr, typename IngressMgr>
class CoordinatorMonitor : NodeMonitor<EgressMgr, IngressMgr> {
    using base_t = NodeMonitor<EgressMgr, IngressMgr>;
    using base_t::egress_mgr;
    using base_t::ingress_mgr;
    using base_t::msg_buffer;

    std::vector<WorkerQueryState> worker_states;
    node_t max_workers{};
    std::deque<node_t> workers_active;
    std::deque<node_t> workers_inactive;

    void initialize_query(u32 start, u32 end)
    {
        print("coordinator: initializing query");
        // kickstart query

        auto num_active_workers = workers_active.size();
        u32 offset              = ((end - start) + num_active_workers - 1) / num_active_workers;
        for (auto worker_id : workers_active) {
            auto* msg = new (msg_buffer.get_message_storage()) StateMessage{Task{start, std::min(start + offset, end)}, num_active_workers, TASK_OFFER};
            print("coordinator: sending offer(", msg->task.start, msg->task.end, ") to", worker_id);
            egress_mgr.send(worker_id, msg);
            ingress_mgr.recv(worker_id, msg_buffer.get_message_storage());
            worker_states[worker_id].sent_task();
            start += offset;
        }
    }

    void monitor_query_state() const
    {
        print("coordinator: monitoring query state");
        while (ingress_mgr.has_inflight()) {
            ingress_mgr.consume_done();
            egress_mgr.try_drain_pending();
        }
    }

    void finalize_query() const
    {
        StateMessage message{QUERY_END};
        egress_mgr.broadcast(&message);
        egress_mgr.wait_all();
    }

  public:
    explicit CoordinatorMonitor(std::unsigned_integral auto initial_nworkers, const std::vector<node_t>& available_workers, EgressMgr& _egress_mgr, IngressMgr& _ingress_mgr)
        : base_t(_egress_mgr, _ingress_mgr), worker_states(available_workers.size(), WorkerQueryState{}), max_workers{static_cast<node_t>(available_workers.size())},
          workers_active(available_workers.begin(), available_workers.begin() + initial_nworkers), workers_inactive(available_workers.begin() + initial_nworkers, available_workers.end())
    {
        std::function message_fn_ingress = [this](StateMessage* msg, u32 dst) {
            print("coordinator: received task response (", msg->task.start, msg->task.end, ")", "with workers =", msg->nworkers);
            if (worker_states[dst].handle_response(*msg)) {
                // worker did not fully accept offer
                auto num_inactive_workers       = workers_inactive.size();
                node_t available_active_workers = 0;
                std::for_each(worker_states.begin(), worker_states.begin() + workers_active.size(), [&](const WorkerQueryState& s) { available_active_workers += s.can_accept_work(); });
                node_t max_available_workers = available_active_workers + num_inactive_workers;
                auto requested_workers       = std::min(msg->nworkers, max_available_workers);
                print("available active workers: ", available_active_workers);
                print("num inactive workers", num_inactive_workers);
                if (requested_workers < msg->nworkers) {
                    print("warning: clipping requested workers [", msg->nworkers, "] to max available workers [", max_available_workers, "]");
                }
                bool increased_workers{false};
                while (available_active_workers++ < requested_workers) {
                    increased_workers = true;
                    workers_active.push_back(workers_inactive.front());
                    workers_inactive.pop_front();
                }
                auto num_active_workers = workers_active.size();
                print("coordinator: requested_workers =", requested_workers);
                print("coordinator: increased_workers =", increased_workers);

                // split remainder offer between available nodes
                u32 start   = msg->task.start;
                u32 end     = msg->task.end;
                auto offset = ((end - start) + requested_workers - 1) / requested_workers;
                print("start: ", start, ", end: ", end, ", offset: ", offset);
                for (u16 worker_id : workers_active) {
                    if (worker_states[worker_id].can_accept_work()) {
                        // send offer
                        auto worker_task = Task{start, std::min(start + offset, end)};
                        auto* msg_egress = new (msg_buffer.get_message_storage()) StateMessage{worker_task, num_active_workers, TASK_OFFER};
                        egress_mgr.send(worker_id, msg_egress);
                        print("coordinator: sent task:", start, std::min(start + offset, end));
                        // recv offer response
                        ingress_mgr.recv(worker_id, msg_buffer.get_message_storage());

                        // update state
                        worker_states[worker_id].sent_task();
                        start += offset;
                    }
                    else if (increased_workers) {
                        // send update
                        auto* msg_response = new (msg_buffer.get_message_storage()) StateMessage{num_active_workers, NUM_WORKERS_UPDATE};
                        egress_mgr.send(worker_id, msg_response);
                    }
                }
            }
            msg_buffer.return_message(msg);
        };
        ingress_mgr.register_consumer_fn(message_fn_ingress);
        std::function message_fn_egress = [&](StateMessage* msg) { msg_buffer.return_message(msg); };
        egress_mgr.register_consumer_fn(message_fn_egress);
    }

    void monitor_query(u32 start, u32 end)
    {
        initialize_query(start, end);
        monitor_query_state();
        finalize_query();
    }
};

} // namespace adapt
