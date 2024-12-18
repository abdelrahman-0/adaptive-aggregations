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
    std::atomic<bool> nworkers_is_stable{false};
    u32 morsel_sz;
    std::atomic<bool> responded_to_current_task{true};
    u64 SLA_s{};

    explicit TaskScheduler(u32 _morsel_sz, double _SLA_s, TaskMetrics& _task_metrics) : task_metrics(_task_metrics), morsel_sz(_morsel_sz), SLA_s(_SLA_s)
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
                // TODO update morsel_end safely
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

    void reset()
    {
        SLA_s                     -= task_metrics.get_elapsed_time_ms() / 1000.0;
        responded_to_current_task  = false;
        task_metrics.reset();
    }

    [[nodiscard]]
    bool is_done() const
    {
        return nworkers_is_stable;
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
        Task next_task;
        if (tasks.try_pop(next_task)) {
            reset();
            morsel_begin   = next_task.start;
            morsel_current = next_task.start;
            morsel_end     = next_task.end;
            return true;
        }
        return false;
    }

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
    double estimate_glob_ht_build_s(u64 nunique_grps)
    {
        // TODO GLM? (output tuple sz + estimated grps)
        return nunique_grps / 1'000'000.0;
    }

    u16 estimate_nworkers()
    {
        u32 morsel_now                        = morsel_current;
        double elapsed_time_s                 = task_metrics.get_elapsed_time_ms() / 1000.0;
        double SLA_adjusted_s                 = SLA_s - elapsed_time_s;
        u32 pages_done                        = morsel_now - morsel_begin;
        u32 pages_left                        = morsel_end - morsel_now;
        double remainder_factor               = (1.0 * pages_left) / pages_done;
        u64 nunique_grps                      = task_metrics.estimate_unique_groups(nworkers) * (remainder_factor + 1);
        double estimated_glob_ht_built_time_s = estimate_glob_ht_build_s(nunique_grps);
        // TODO also initial pages need to be transferred
        double estimated_transfer_time_s      = remainder_factor * task_metrics.get_output_size_B() / defaults::node_bandwidth_GB_per_s;
        double cpu_bound_estimate             = (elapsed_time_s * remainder_factor + estimated_glob_ht_built_time_s) / SLA_adjusted_s;
        double network_bound_estimate         = math::find_max_quadratic_root(SLA_adjusted_s, -estimated_transfer_time_s - estimated_glob_ht_built_time_s, estimated_transfer_time_s);
        static u64 printed                    = 0;
        if (printed++ % 1000 == 0) {
            print("remainder factor", remainder_factor, "nunique_grps", nunique_grps, "estimated glob ht time", estimated_glob_ht_built_time_s, "estimated transfer time",
                  estimated_transfer_time_s);
            print("cpu bound estimate:", cpu_bound_estimate, "network bound estimate:", network_bound_estimate);
        }
        node_t nworkers_estimated = std::ceil(std::max(cpu_bound_estimate, network_bound_estimate));
        return nworkers_estimated;
    }
};

} // namespace adapt
