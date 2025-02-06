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
    bool is_starting_worker{false};

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
    explicit WorkerMonitor(EgressMgr& _egress_mgr, IngressMgr& _ingress_mgr, TaskScheduler& _adaptive_state, bool _is_starting_worker)
        : base_t(_egress_mgr, _ingress_mgr), task_scheduler(_adaptive_state), is_starting_worker(_is_starting_worker)
    {
        std::function message_fn_ingress = [this](StateMessage* msg, u32) {
            switch (msg->type) {
            case TASK_OFFER:
                ingress_mgr.recv(0, msg_buffer.get_message_storage());
                print("worker: received task offer (", msg->task.start, msg->task.end, ") with workers =", msg->nworkers, "from coordinator");
                received_first_task = true;
                // TODO split work if starting_worker
                task_scheduler.update_nworkers(msg->nworkers);
                task_scheduler.enqueue_task(msg->task);
                break;
            case NO_WORK:
                print("worker: received NO_WORK");
                task_scheduler.finished = true;
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
        while (ingress_mgr.has_inflight() or egress_mgr.has_inflight()) {
            ingress_mgr.consume_done();
            egress_mgr.try_drain_pending();
            if (is_starting_worker) {
                check_query_state();
            }
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
        print("waiting for all");
        egress_mgr.wait_all();
        print("finished waiting");
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
                auto requested_workers = std::min(msg->nworkers, static_cast<u16>(workers_inactive.size()));
                // split remainder offer between available nodes
                u32 start              = msg->task.start;
                u32 end                = msg->task.end;
                auto offset            = ((end - start) + requested_workers - 1) / requested_workers;
                print("start: ", start, ", end: ", end, ", offset: ", offset);
                auto total_new_workers = requested_workers + 1u;
                while (requested_workers--) {
                    // send offer
                    u16 worker_id    = workers_inactive.front();
                    auto worker_task = Task{start, std::min(start + offset, end)};
                    auto* msg_egress = new (msg_buffer.get_message_storage()) StateMessage{worker_task, total_new_workers, TASK_OFFER};
                    egress_mgr.send(worker_id, msg_egress);
                    print("coordinator: sent task:", start, std::min(start + offset, end));
                    // update state
                    worker_states[worker_id].sent_task();
                    workers_inactive.pop_front();
                    workers_active.push_back(worker_id);
                    start += offset;
                }
            }
            for (u16 worker_id : workers_inactive) {
                auto* msg_egress = new (msg_buffer.get_message_storage()) StateMessage{-1u, NO_WORK};
                egress_mgr.send(worker_id, msg_egress);
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
