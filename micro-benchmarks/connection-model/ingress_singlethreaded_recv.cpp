#include <gflags/gflags.h>
#include <tbb/enumerable_thread_specific.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>
#include <thread>

#include "bench/logger.h"
#include "defaults.h"
#include "network/connection.h"
#include "network/network_manager_old.h"
#include "performance/stopwatch.h"
#include "storage/chunked_list.h"
#include "storage/policy.h"
#include "storage/table.h"
#include "utils/hash.h"
#include "utils/utils.h"

DEFINE_int32(connections, 10, "number of ingress connections");

using NetworkPage = PageNetwork<int64_t>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Connection conn{FLAGS_connections};
    conn.setup_ingress();

    // track metrics
    NetworkPage page{};
    uint64_t pages_received{0};
    uint64_t tuples_received{0};
    ::ssize_t res{0};
    uint64_t end_pages{0};
    int32_t next_conn{0};

    Stopwatch swatch{};
    swatch.start();
    while (end_pages < FLAGS_connections) {
        res = ::recv(conn.socket_fds[next_conn], &page, defaults::network_page_size, MSG_WAITALL);
        next_conn = (next_conn + 1) % FLAGS_connections;
        if (res == -1) {
            throw NetworkRecvError{};
        }
        if (res > 0 && res < defaults::network_page_size) {
            print("short read");
        }
        if (res == 0) {
            continue;
        }
        if (page.empty()) {
            end_pages++;
            continue;
        }
        pages_received++;
        tuples_received += page.num_tuples;
    }

    // track metrics
    swatch.stop();

    Logger logger{};
    logger.log("traffic", "ingress"s);
    logger.log("primitive", "recv"s);
    logger.log("implementation", "sync"s);
    logger.log("threads", 1);
    logger.log("connections", FLAGS_connections);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", pages_received);
    logger.log("tuples", tuples_received);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("throughput (Gb/s)", (pages_received * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));
}
