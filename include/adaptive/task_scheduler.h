#pragma once

#include <atomic>
#include <tbb/concurrent_queue.h>

#include "common.h"
#include "defaults.h"
#include "policy.h"
#include "task_metrics.h"
#include "utils/math.h"

namespace adapt {

struct TaskScheduler {
    static constexpr u16 total_task_splits = 10;

    tbb::concurrent_queue<Task> tasks;
    TaskMetrics& task_metrics;
    u32 morsel_begin{0};
    std::atomic<u32> morsel_current{0};
    std::atomic<u32> morsel_end{0};
    std::atomic<u16> nworkers;
    std::atomic<bool> finished{false};
    u32 morsel_sz;
    std::atomic<bool> received_message_from_coordinator{false};
    std::atomic<bool> consumed_at_least_one{false};
    std::atomic<bool> task_available{false};
    u32 max_workers;
    u32 threads_per_worker;
    std::atomic<Task> total_task{};
    bool is_first_worker;
    u32 offset{};
    u16 split;
    u16 splits_done{0};
    std::atomic<Task> response;
    std::atomic<bool> response_available{false};
    policy::Policy scale_out_policy;
    std::chrono::time_point<std::chrono::high_resolution_clock> scale_out_timestamp;

    explicit TaskScheduler(u32 _morsel_sz, TaskMetrics& _task_metrics, u32 _max_workers, u32 _threads_per_worker, bool _is_first_worker, policy::Policy _scale_out_policy)
        : task_metrics(_task_metrics), morsel_sz(_morsel_sz), max_workers(_max_workers), threads_per_worker(_threads_per_worker), is_first_worker(_is_first_worker),
          split(is_first_worker ? total_task_splits : 1), scale_out_policy(_scale_out_policy)
    {
    }

    void enqueue_task(Task task)
    {
        total_task.store(task);
        offset         = (task.end - task.start + split - 1) / split;
        task_available = true;
    }

    void consume_chunk()
    {
        if (not finished) {
            Task task{total_task.load().start, std::min(total_task.load().start + offset, total_task.load().end)};
            if (task.end == total_task.load().end) {
                finished           = true;
                response_available = true;
            }
            tasks.push(task);
            task.start = task.end;
            task.end   = total_task.load().end;
            total_task.store(task);
        }
    }

    void check_query_health()
    {
        if (consumed_at_least_one and not response_available) {
            auto new_workers = estimate_nworkers();
            if (new_workers > nworkers) {
                scale_out_timestamp = std::chrono::high_resolution_clock::now();
                // split remaining work between all new_workers
                auto new_offset     = (total_task.load().end - total_task.load().start + new_workers - 1) / new_workers;
                auto task_separator = std::min(total_task.load().start + new_offset, total_task.load().end);
                Task task{total_task.load().start, task_separator};
                tasks.push(task);
                finished           = true;
                response           = Task{task_separator, total_task.load().end};
                nworkers           = new_workers;
                response_available = true;
            }
        }
    }

    // guaranteed to happen strongly-before any threads are unblocked at the barrier since it is called in std::barrier's callback
    [[maybe_unused]]
    bool dequeue_task_starting_worker()
    {
        if (not consumed_at_least_one) {
            task_metrics.reset();
        }
        received_message_from_coordinator.wait(false);
        check_query_health();
        consume_chunk();
        if (Task next_task{}; tasks.try_pop(next_task)) {
            morsel_begin          = next_task.start;
            morsel_current        = next_task.start;
            morsel_end            = next_task.end;
            consumed_at_least_one = true;
            splits_done++;
            return true;
        }
        return false;
    }

    [[nodiscard]]
    bool dequeue_task_auxilary_worker()
    {
        if (not consumed_at_least_one) {
            task_metrics.reset();
        }
        received_message_from_coordinator.wait(false);
        if (task_available) {
            consume_chunk();
            if (Task next_task{}; tasks.try_pop(next_task)) {
                morsel_begin   = next_task.start;
                morsel_current = next_task.start;
                morsel_end     = next_task.end;
                finished       = true;
                return true;
            }
        }
        finished = true;
        return false;
    }

    [[nodiscard]]
    Task get_next_morsel()
    {
        auto begin = morsel_current.fetch_add(morsel_sz);
        auto end   = std::min(begin + morsel_sz, morsel_end.load());
        return {begin, end};
    }

    void update_nworkers(u16 _nworkers)
    {
        nworkers = _nworkers;
    }

    [[nodiscard]]
    u16 estimate_nworkers()
    {
        switch (scale_out_policy.type) {
        case policy::STATIC: {
            if (task_metrics.get_elapsed_time_ms() > scale_out_policy.time_out) {
                return scale_out_policy.workers;
            }
            return nworkers.load();
        }
        case policy::REGRESSION: {
            auto unique_groups = task_metrics.estimate_total_groups(static_cast<double>(splits_done) / static_cast<double>(split));
            print("estimate unique groups: ", unique_groups);
            for (auto i = nworkers.load(); i < max_workers; i++) {
                u64 remaining_ms = std::max(static_cast<s64>(scale_out_policy.time_out) - task_metrics.get_elapsed_time_ms(), 0l);
                auto estimated_time = estimate_time_ms(i, unique_groups, threads_per_worker);
                if (estimated_time < remaining_ms) {
                    print("estimate time:", estimated_time, "workers:", i);
                    return i;
                }
            }
            print("estimating max workers:", max_workers);
            return max_workers;
        }
        default:
            ENSURE(false);
        }
    }

    [[nodiscard]]
    static u32 estimate_time_ms(u32 groups, u16 nworkers, u16 threads_per_worker)
    {
        return std::exp(5.828258 - 0.387998 * nworkers + 0.136706 * std::log(groups) - 0.887925 * std::log(threads_per_worker) + 0.014630 * nworkers * threads_per_worker);
    }
};

} // namespace adapt
