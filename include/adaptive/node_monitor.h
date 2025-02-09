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
    bool is_starting_worker;
    std::atomic<bool> sent_response{false};

    void initialize_query()
    {
        ingress_mgr.recv(0, msg_buffer.get_message_storage());
    }

    void send_task_response(u16 nworkers, Task task_response)
    {
        print("worker: sending task response [", task_response.start, task_response.end, "] with workers hint =", nworkers);
        egress_mgr.send(0, new (msg_buffer.get_message_storage()) StateMessage{task_response, nworkers, TASK_OFFER_RESPONSE});
    }

    void check_query_state()
    {
        // check task_scheduler's current query
        if (not sent_response and task_scheduler.response_available) {
            send_task_response(task_scheduler.nworkers - 1, task_scheduler.response);
            sent_response = true;
        }
    }

  public:
    explicit WorkerMonitor(EgressMgr& _egress_mgr, IngressMgr& _ingress_mgr, TaskScheduler& _adaptive_state, bool _is_starting_worker)
        : base_t(_egress_mgr, _ingress_mgr), task_scheduler(_adaptive_state), is_starting_worker(_is_starting_worker)
    {
        std::function message_fn_ingress = [this](StateMessage* msg, u32) {
            switch (msg->type) {
            case TASK_OFFER:
                print("worker: received TASK_OFFER with task [", msg->task.start, msg->task.end, "] with workers =", msg->nworkers);
                received_first_task = true;
                task_scheduler.update_nworkers(msg->nworkers);
                task_scheduler.enqueue_task(msg->task);
                break;
            case NO_WORK:
                print("worker: received NO_WORK");
                task_scheduler.consumed_at_least_one = true;
                break;
            default:
                ENSURE(false);
            }
            task_scheduler.received_message_from_coordinator = true;
            task_scheduler.received_message_from_coordinator.notify_one();
            msg_buffer.return_message(msg);
        };
        ingress_mgr.register_consumer_fn(message_fn_ingress);
        std::function message_fn_egress = [&](StateMessage* msg) { msg_buffer.return_message(msg); };
        egress_mgr.register_consumer_fn(message_fn_egress);
    }

    void monitor_query()
    {
        initialize_query();
        while (ingress_mgr.has_inflight()) {
            ingress_mgr.consume_done();
        }
        if (is_starting_worker) {
            while (not sent_response) {
                check_query_state();
            }
            egress_mgr.wait_all();
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

    void log_task_sent(node_t worker_id, Task task) const
    {
        print("coordinator: sent task: [", task.start, task.end, "] to worker", worker_id);
    }

    void initialize_query(u32 start, u32 end)
    {
        print("coordinator: initializing query");
        // kickstart query

        auto num_active_workers = workers_active.size();
        u32 offset              = ((end - start) + num_active_workers - 1) / num_active_workers;
        for (auto worker_id : workers_active) {
            Task task{start, std::min(start + offset, end)};
            auto* msg = new (msg_buffer.get_message_storage()) StateMessage{task, num_active_workers, TASK_OFFER};
            egress_mgr.send(worker_id, msg);
            log_task_sent(worker_id, task);
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
        print("coordinator: waiting");
        egress_mgr.wait_all();
    }

  public:
    explicit CoordinatorMonitor(std::unsigned_integral auto initial_nworkers, const std::vector<node_t>& available_workers, EgressMgr& _egress_mgr, IngressMgr& _ingress_mgr)
        : base_t(_egress_mgr, _ingress_mgr), worker_states(available_workers.size(), WorkerQueryState{}), max_workers{static_cast<node_t>(available_workers.size())},
          workers_active(available_workers.begin(), available_workers.begin() + initial_nworkers), workers_inactive(available_workers.begin() + initial_nworkers, available_workers.end())
    {
        std::function message_fn_ingress = [this](StateMessage* msg, u32 dst) {
            print("coordinator: received task remainder [", msg->task.start, msg->task.end, "]", "with workers hint =", msg->nworkers);
            if (worker_states[dst].handle_response(*msg)) {
                // worker did not fully accept offer
                auto requested_workers = std::min(msg->nworkers, static_cast<u16>(workers_inactive.size()));
                // split remainder offer between available nodes
                u32 start              = msg->task.start;
                u32 end                = msg->task.end;
                auto offset            = ((end - start) + requested_workers - 1) / requested_workers;
                auto total_new_workers = requested_workers + 1u;
                while (requested_workers--) {
                    // send offer
                    node_t worker_id = workers_inactive.front();
                    auto worker_task = Task{start, std::min(start + offset, end)};
                    auto* msg_egress = new (msg_buffer.get_message_storage()) StateMessage{worker_task, total_new_workers, TASK_OFFER};
                    egress_mgr.send(worker_id, msg_egress);
                    log_task_sent(worker_id, worker_task);
                    // update state
                    worker_states[worker_id].sent_task();
                    workers_inactive.pop_front();
                    workers_active.push_back(worker_id);
                    start += offset;
                }
            }
            for (node_t worker_id : workers_inactive) {
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
