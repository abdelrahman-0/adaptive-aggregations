#pragma once

#include <fstream>
#include <thread>

#include "utils.h"

using namespace std::string_literals;

struct NodeTopology {
    struct ThreadInfo {
        u16 package_id;
        u16 core_id;
    };

    std::vector<ThreadInfo> threads;
    u16 nthreads_sys{0};
    u16 nphysical_cores{0};
    u16 threads_per_core{0};
    bool compact{false};

    NodeTopology() = default;

    // inspired by LIKWID's topology parsing
    // https://github.com/RRZE-HPC/likwid/blob/master/src/topology_proc.c#L606
    void init() {
        nthreads_sys = std::thread::hardware_concurrency();
        threads.resize(nthreads_sys);
        for (auto tid{0}; tid < nthreads_sys; ++tid) {
            auto topology_dir = "/sys/devices/system/cpu/cpu"s + std::to_string(tid) + "/topology/"s;

            std::ifstream ifstream_package_id{topology_dir + "physical_package_id"s};
            std::string str_physical_package_id;
            std::getline(ifstream_package_id, str_physical_package_id);
            threads[tid].package_id = std::stoi(str_physical_package_id);

            std::ifstream ifstream_core_id{topology_dir + "core_id"s};
            std::string str_core_id;
            std::getline(ifstream_core_id, str_core_id);
            threads[tid].core_id = std::stoi(str_core_id);

            if (threads[tid].package_id == 0 && threads[tid].core_id == 0) {
                threads_per_core++;
            }
        }
        if (threads_per_core == 2 and threads[0].core_id == threads[1].core_id) {
            compact = true;
        }
        nphysical_cores = nthreads_sys / threads_per_core;
        println("available threads:", nthreads_sys);
        println("physical cores:", nphysical_cores);
        println("threads per core:", threads_per_core);
        println("compact affinity:", compact);
    }

    // set affinity of calling thread
    void set_cpu_affinity(int tid) const {
        if (tid >= nthreads_sys) {
            logln("oversubscription: reduce number of threads");
            std::exit(0);
        }
        ::cpu_set_t mask;
        int cpu = (tid * (compact ? 2 : 1)) % nthreads_sys;
        CPU_ZERO(&mask);
        CPU_SET(cpu, &mask);
        println("pinning thread", tid, "to cpu", cpu);

        // allow thread to float between hyper-threads on same physical core
        if (threads_per_core == 2) {
            cpu += compact ? 1 : nthreads_sys / 2;
            cpu %= nthreads_sys;
            CPU_SET(cpu, &mask);
            println("pinning thread", tid, "to cpu", cpu);
        }
        if (::sched_setaffinity(0, sizeof(mask), &mask)) {
            logln("unable to pin CPU");
            std::exit(0); // TODO exception class
        }
    }
};
