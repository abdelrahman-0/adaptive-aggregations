#pragma once

#include <fstream>
#include <mutex>
#include <thread>

#include "bench/bench.h"
#include "utils/utils.h"

using namespace std::string_literals;

namespace sys {

struct NodeTopology {
    struct ThreadInfo {
        u16 package_id;
        u16 core_id;
    };

    std::vector<ThreadInfo> threads;
    u32 nthreads_sys{0};
    u32 requested_threads{0};
    u32 nphysical_cores{0};
    u16 threads_per_core{0};
    bool compact_assignment{false};
    std::mutex print_mut{};

    explicit NodeTopology(u32 requested_threads) : requested_threads(requested_threads) {};

    // inspired by LIKWID's topology parsing
    // https://github.com/RRZE-HPC/likwid/blob/master/src/topology_proc.c#L606
    void init()
    {
        nthreads_sys = std::thread::hardware_concurrency();
        threads.resize(nthreads_sys);
        for (auto tid{0}; tid < nthreads_sys; ++tid) {
            auto topology_dir = "/sys/devices/system/cpu/cpu"s + std::to_string(tid) + "/topology/"s;

            std::ifstream ifstream_package_id{topology_dir + "physical_package_id"s};
            std::string str_package_id;
            std::getline(ifstream_package_id, str_package_id);
            threads[tid].package_id = std::stoi(str_package_id);

            std::ifstream ifstream_core_id{topology_dir + "core_id"s};
            std::string str_core_id;
            std::getline(ifstream_core_id, str_core_id);
            threads[tid].core_id = std::stoi(str_core_id);

            if (threads[tid].package_id == 0 && threads[tid].core_id == 0) {
                threads_per_core++;
            }
        }
        if (threads_per_core == 2 and threads[0].core_id == threads[1].core_id) {
            compact_assignment = true;
        }
        nphysical_cores = nthreads_sys / threads_per_core;
        print("available threads:", nthreads_sys);
        print("physical cores:", nphysical_cores);
        print("threads used:", requested_threads);
        print("threads per core:", threads_per_core);
        print("logical CPU assignment:", compact_assignment ? "compact" : "round-robin");
    }

    // set affinity of calling thread
    // (core granularity is used to allow threads to float between different hyper-threads) as recommended here:
    // https://www.intel.com/content/www/us/en/docs/dpcpp-cpp-compiler/developer-guide-reference/2023-0/thread-affinity-interface.html
    void pin_thread(int tid)
    {
        ::cpu_set_t mask;
        CPU_ZERO(&mask);
        int cpu;
        if (threads_per_core == 1) {
            cpu = tid;
            CPU_SET(cpu, &mask);
        }
        else {
            // first determine physical core
            auto num_siblings  = (requested_threads > nphysical_cores) ? requested_threads - nphysical_cores : 0;
            bool has_sibling   = (tid / threads_per_core) < num_siblings;
            auto physical_core = has_sibling ? (tid / threads_per_core) : tid - num_siblings;

            // then determine the logical CPU (pin to both hyper-threads)
            cpu                = compact_assignment ? physical_core * threads_per_core : physical_core;
            CPU_SET(cpu, &mask);
            DEBUGGING(log_thread_pinned(tid, cpu));

            cpu += compact_assignment ? 1 : nphysical_cores;
            CPU_SET(cpu, &mask);
            DEBUGGING(log_thread_pinned(tid, cpu));
        }

        if (::sched_setaffinity(0, sizeof(mask), &mask)) {
            logln("unable to pin CPU");
            std::exit(0); // TODO exception class
        }
    }

    void log_thread_pinned(std::integral auto tid, std::integral auto cpu)
    {
        std::unique_lock _{print_mut};
        print("pinning thread", tid, "to cpu", cpu);
    }
};

} // namespace sys
