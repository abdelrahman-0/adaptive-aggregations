#include <gflags/gflags.h>

#include "defaults.h"
#include "network/connection.h"
#include "network/network_manager.h"
#include "network/page_communication.h"
#include "utils/stopwatch.h"

DEFINE_bool(local, true, "run benchmark using loop-back interface");

#define SCHEMA char

using NetworkPage = PageCommunication<SCHEMA>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto node_id = 1u;
    Connection conn{node_id, 1, 0};
    conn.setup_ingress();

    IngressNetworkManager<NetworkPage> manager_recv{1, 256, 20, false, conn.socket_fds};

    NetworkPage* network_page;
    bool last_page = false;

    // track metrics
    uint64_t pages_received{0};
    uint64_t tuples_received{0};

    manager_recv.post_recvs(0);

    Stopwatch swatch{};
    swatch.start();
    while (not last_page) {
        do {
            std::tie(network_page, std::ignore) = manager_recv.get_page();
        } while (not network_page);
        last_page = network_page->is_last_page();
        if (!last_page) {
            // still more pages
            manager_recv.post_recvs(0);
        }
        tuples_received += network_page->get_num_tuples();
        pages_received++;
        manager_recv.done_page(network_page);
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
