#include <gflags/gflags.h>

#include "defaults.h"
#include "network/connection.h"
#include "network/network_manager.h"
#include "network/page_communication.h"
#include "performance/stopwatch.h"

DEFINE_bool(local, true, "run benchmark using loop-back interface");
DEFINE_uint32(pages, 1'000, "total number of pages to send via egress traffic");

#define SCHEMA char

using NetworkPage = PageCommunication<SCHEMA>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto node_id = 0u;
    auto subnet = FLAGS_local ? defaults::LOCAL_subnet : defaults::AWS_subnet;
    auto host_base = FLAGS_local ? defaults::LOCAL_host_base : defaults::AWS_host_base;

    // setup connections
    auto destination_ip = std::string{subnet} + std::to_string(host_base);
    Connection conn{node_id, 1, 0, destination_ip};
    conn.setup_egress(1);

    SimpleEgressNetworkManager<NetworkPage> manager_send{1, 256, 20, false, conn.socket_fds};

    // track metrics
    Stopwatch swatch{};
    uint64_t pages_sent{0};
    uint64_t tuples_sent{0};

    // send loop
    swatch.start();
    while (pages_sent < FLAGS_pages) {
        auto* page = manager_send.get_page(0);
        page->num_tuples = NetworkPage::max_tuples_per_page;
        pages_sent++;
        tuples_sent += page->get_num_tuples();
    }
    manager_send.flush_all();
    manager_send.wait_all();

    swatch.stop();

    Logger logger{};
    logger.log("traffic", "egress"s);
    logger.log("primitive", "send"s);
    logger.log("implementation", "sync"s);
    logger.log("threads", 1);
    logger.log("connections", 1);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", pages_sent);
    logger.log("tuples", tuples_sent);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("throughput (Gb/s)", (pages_sent * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));
}
