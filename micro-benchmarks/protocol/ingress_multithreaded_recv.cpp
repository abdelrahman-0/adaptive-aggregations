#include <gflags/gflags.h>
#include <tbb/enumerable_thread_specific.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>
#include <thread>

#include "defaults.h"
#include "network/connection.h"
#include "network/network_manager.h"
#include "storage/chunked_list.h"
#include "storage/policy.h"
#include "storage/table.h"
#include "utils/hash.h"
#include "utils/logger.h"
#include "utils/stopwatch.h"
#include "utils/utils.h"

DEFINE_int32(connections, 1, "number of ingress connections");
DEFINE_uint32(buffers, 1000, "number of buffer pages");

using NetworkPage = PageCommunication<int64_t>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Connection conn{FLAGS_connections};
    conn.setup_ingress();

    // thread-uring pool
    std::vector<std::thread> threads{};
    std::atomic<bool> wait{true};
    std::atomic<uint64_t> pages_received{0};
    std::atomic<uint64_t> tuples_received{0};

    // pre-register 1 socket fd per rig
    for (auto i{0u}; i < FLAGS_connections; ++i) {

        threads.emplace_back([i, &conn, &wait, &pages_received, &tuples_received]() {
            std::vector<NetworkPage> pages(FLAGS_buffers);
            uint64_t local_pages_received{0};
            uint64_t local_tuples_received{0};
            int64_t res{0};

            while (wait)
                ;

            // receiver loop
            while (true) {
                res =
                    ::recv(conn.socket_fds[i], pages.data(), defaults::network_page_size * FLAGS_buffers, MSG_WAITALL);
                for (auto j{0u}; j < FLAGS_buffers; ++j) {
                    auto& page = pages[j];
                    if (res == 0 || page.num_tuples == 0) {
                        goto done;
                    }
                    local_tuples_received += page.num_tuples;
                    local_pages_received++;
                }
            }
        done:;
            pages_received += local_pages_received;
            tuples_received += local_tuples_received;
        });
    }

    // track metrics
    Stopwatch swatch{};
    swatch.start();
    wait = false;
    for (auto& thread : threads) {
        thread.join();
    }
    swatch.stop();

    Logger logger{};
    logger.log("traffic", "ingress"s);
    logger.log("implementation", "io_uring"s);
    logger.log("connections", FLAGS_connections);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", pages_received);
    logger.log("tuples", tuples_received);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("bandwidth (Gb/s)", (pages_received * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));

    println("ingress done");
}
