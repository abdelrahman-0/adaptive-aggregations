#include <gflags/gflags.h>
#include <tbb/enumerable_thread_specific.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>
#include <thread>

#include "bench/logger.h"
#include "defaults.h"
#include "network/connection.h"
#include "network/network_manager_old.h"
#include "network/page_communication.h"
#include "performance/stopwatch.h"
#include "storage/chunked_list.h"
#include "storage/policy.h"
#include "storage/table.h"
#include "utils/hash.h"
#include "utils/utils.h"

DEFINE_bool(local, true, "run benchmark using loop-back interface");

#define SCHEMA char

using NetworkPage = PageCommunication<SCHEMA>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto node_id = 1u;
    Connection conn{node_id, 1, 0};
    conn.setup_ingress();

    // track metrics
    NetworkPage page{};
    uint64_t pages_received{0};
    uint64_t tuples_received{0};
    ::ssize_t res;

    Stopwatch swatch{};
    swatch.start();
    while (not page.is_last_page()) {
        res = ::recv(conn.socket_fds[0], &page, sizeof(NetworkPage), MSG_WAITALL);
        if (res != sizeof(NetworkPage)) {
            throw NetworkRecvError{res == -1 ? "" : "short recv"};
        }
        pages_received++;
        tuples_received += page.get_num_tuples();
    }

    // track metrics
    swatch.stop();

    Logger logger{};
    logger.log("traffic", "ingress"s);
    logger.log("primitive", "recv"s);
    logger.log("implementation", "sync"s);
    logger.log("threads", 1);
    logger.log("connections", 1);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", pages_received);
    logger.log("tuples", tuples_received);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("throughput (Gb/s)", (pages_received * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));
}
