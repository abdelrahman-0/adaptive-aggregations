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
    static constexpr double min_wait_percentage = 10.0;
    static constexpr double max_wait_percentage = 80.0;
    tbb::concurrent_queue<Task> tasks;
    TaskMetrics& task_metrics;
    u32 morsel_begin{0};
    std::atomic<u32> morsel_current{0};
    std::atomic<u32> morsel_end{0};
    std::atomic<u16> nworkers;
    std::atomic<bool> finished{false};
    std::atomic<bool> consumed_at_least_one{false};
    u32 morsel_sz;
    std::atomic<bool> responded_to_current_task{true};
    u16 npeers_max;
    u16 threads_per_worker;
    u32 time_budget{};

    explicit TaskScheduler(u32 _morsel_sz, u32 budget, TaskMetrics& _task_metrics, u16 _npeers_max, u16 _threads_per_worker)
        : task_metrics(_task_metrics), morsel_sz(_morsel_sz), npeers_max(_npeers_max), threads_per_worker(_threads_per_worker), time_budget(budget)
    {
    }

    [[nodiscard]]
    std::optional<std::pair<u16, Task>> get_task_state()
    {
        u32 morsel_current_snapshot = morsel_current.load();
        double percent_done         = 100.0 * (morsel_current_snapshot - morsel_begin) / (morsel_end - morsel_begin);
        std::optional<std::pair<u16, Task>> task_state{std::nullopt};
        if (not responded_to_current_task and percent_done > min_wait_percentage) {
            auto estimated_workers = estimate_nworkers();
            if (estimated_workers > nworkers) {
                // TODO split properly
                auto new_morsel_end       = morsel_current_snapshot + (morsel_end - morsel_current_snapshot) / estimated_workers;
                auto old_morsel_end       = morsel_end.exchange(new_morsel_end);
                responded_to_current_task = true;
                // notify possibly blocked query threads
                responded_to_current_task.notify_one();
                task_state = std::pair{estimated_workers - 1, Task{new_morsel_end, old_morsel_end}};
            }
            else if (percent_done > max_wait_percentage) {
                responded_to_current_task = true;
                // notify possibly blocked query threads
                responded_to_current_task.notify_one();
                // no additional workers needed
                task_state = std::pair{0, Task{}};
            }
        }
        return task_state;
    }

    [[nodiscard]]
    bool is_done() const
    {
        return finished;
    }

    void enqueue_task(Task task)
    {
        tasks.push(task);
    }

    // guaranteed to happen strongly-before any threads are unblocked at the barrier since it is called in std::barrier's callback
    [[nodiscard]]
    bool dequeue_task()
    {
        responded_to_current_task.wait(false);
        if (Task next_task{}; tasks.try_pop(next_task)) {
            task_metrics.reset();
            responded_to_current_task = false;
            morsel_begin              = next_task.start;
            morsel_current            = next_task.start;
            morsel_end                = next_task.end;
            consumed_at_least_one     = true;
            return true;
        }
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
        // TODO switch policy
        auto unique_groups = task_metrics.estimate_unique_groups(1);
        for (auto i = nworkers.load(); nworkers < npeers_max; nworkers++) {
            if (estimate_time_ms(i, unique_groups, threads_per_worker) < (time_budget - task_metrics.get_elapsed_time_ms())) {
                return i;
            }
        }
        return npeers_max;
    }

    [[nodiscard]]
    u32 estimate_time_ms(u32 groups, u16 nworkers, u16 threads_per_worker)
    {
        return std::exp(5.828258 - 0.387998 * nworkers + 0.136706 * std::log(groups) - 0.887925 * log(threads_per_worker) + 0.014630 * nworkers * threads_per_worker);
    }
};

} // namespace adapt
