#include <gflags/gflags.h>
#include <likwid-marker.h>
#include <sys/resource.h>
#include <tbb/enumerable_thread_specific.h>
#include <thread>

#include "defaults.h"
#include "exceptions/exceptions_misc.h"
#include "network/connection.h"
#include "network/network_manager_old.h"
#include "storage/chunked_list.h"
#include "utils/hash.h"
#include "utils/logger.h"
#include "utils/stopwatch.h"

DEFINE_uint32(connections, 5, "number of ingress connections");

using NetworkPage = PageNetwork<int64_t>;

int main(int argc, char* argv[]) {
    LIKWID_MARKER_INIT;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Connection conn{FLAGS_connections};
    conn.setup_ingress();

    // thread pool
    std::vector<std::thread> threads{};
    std::atomic<bool> wait{true};

    // track metrics
    std::atomic<uint64_t> pages_received{0};
    std::atomic<uint64_t> tuples_received{0};
    Logger logger_thread{};

    // pre-register 1 socket fd per rig
    for (auto i{0u}; i < FLAGS_connections; ++i) {

        threads.emplace_back([i, &conn, &wait, &pages_received, &tuples_received, &logger_thread]() {
            LIKWID_MARKER_THREADINIT;
            LIKWID_MARKER_REGISTER("recv");

            NetworkPage page{};
            uint64_t local_pages_received{0};
            uint64_t local_tuples_received{0};
            ::ssize_t res{0};

            while (wait)
                ;

            LIKWID_MARKER_START("recv");
            ::rusage usage_start{};
            if (::getrusage(RUSAGE_THREAD, &usage_start) == -1) {
                throw GetResourceUsageError{};
            }
            // receiver loop
            while (true) {
                res = ::recv(conn.socket_fds[i], &page, defaults::network_page_size, MSG_WAITALL);
                if (res == -1) {
                    throw NetworkRecvError{};
                }
                if (page.empty() || res == 0) {
//                    println(res);
                    goto done;
                }
                assert(res == defaults::network_page_size);
                local_pages_received++;
                local_tuples_received += page.num_tuples;
            }
        done:;
            pages_received += local_pages_received;
            tuples_received += local_tuples_received;
            ::rusage usage_stop{};
            if (::getrusage(RUSAGE_THREAD, &usage_stop) == -1) {
                throw GetResourceUsageError{};
            }
            LIKWID_MARKER_STOP("recv");
            auto cpu_id = "Thread "s + std::to_string(sched_getcpu());
            logger_thread.log(cpu_id + " - context switches (invol)", usage_stop.ru_nivcsw - usage_start.ru_nivcsw);
            logger_thread.log(cpu_id + " - context switches (vol)", usage_stop.ru_nvcsw - usage_start.ru_nvcsw);
            logger_thread.log(cpu_id + " - user time (ms)",
                              ((usage_stop.ru_utime.tv_sec - usage_start.ru_utime.tv_sec) * 1'000'000 +
                               usage_stop.ru_utime.tv_usec - usage_start.ru_utime.tv_usec) /
                                  1000);
            logger_thread.log(cpu_id + " - kernel time (ms)",
                              ((usage_stop.ru_stime.tv_sec - usage_start.ru_stime.tv_sec) * 1'000'000 +
                               usage_stop.ru_stime.tv_usec - usage_start.ru_stime.tv_usec) /
                                  1000);
        });
    }

    // track metrics
    Stopwatch swatch{};
    swatch.start();
    wait = false;
    for (auto& t : threads) {
        t.join();
    }
    swatch.stop();

    Logger logger{};
    logger.log("traffic", "ingress"s);
    logger.log("primitive", "recv"s);
    logger.log("implementation", "sync"s);
    logger.log("threads", FLAGS_connections);
    logger.log("connections", FLAGS_connections);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", pages_received);
    logger.log("tuples", tuples_received);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("throughput (Gb/s)", (pages_received * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));

    LIKWID_MARKER_CLOSE;
}
