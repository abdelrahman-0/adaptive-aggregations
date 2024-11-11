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

    auto env_var = std::getenv("NODE_ID");
    u32 node_id = std::stoul(env_var ? env_var : "0");
    auto subnet = FLAGS_local ? defaults::LOCAL_subnet : defaults::AWS_subnet;
    auto host_base = FLAGS_local ? defaults::LOCAL_host_base : defaults::AWS_host_base;

    // setup connections
    std::vector<int> socket_fds{};
    if (node_id) {
        Connection conn{node_id, 1, 0};
        conn.setup_ingress();
        socket_fds = std::move(conn.socket_fds);
    } else {
        auto destination_ip = std::string{subnet} + std::to_string(host_base);
        Connection conn{node_id, 1, 0, destination_ip};
        conn.setup_egress(1);
        socket_fds = std::move(conn.socket_fds);
    }

    IngressNetworkManager<NetworkPage> manager_recv{1, 256, 1, false, socket_fds};
    SimpleEgressNetworkManager<NetworkPage> manager_send{1, 256, 1, false, socket_fds};

    // track metrics
    Stopwatch swatch{};
    u64 pages_sent{0}, tuples_sent{0}, pages_recv{0}, tuples_recv{0};

    NetworkPage* network_page{nullptr};
    bool last_page{false};

    manager_recv.post_recvs(0);

    // loop
    swatch.start();
    while (pages_sent < FLAGS_pages) {
        auto* page = manager_send.get_page(0);
        page->num_tuples = NetworkPage::max_tuples_per_page;
        while (not last_page) {
            std::tie(network_page, std::ignore) = manager_recv.get_page();
            if (network_page) {
                last_page = network_page->is_last_page();
                tuples_recv += network_page->get_num_tuples();
                manager_recv.done_page(network_page);
                if (not last_page) {
                    manager_recv.post_recvs(0);
                }
                pages_recv++;
            } else {
                break;
            }
        }
        pages_sent++;
        tuples_sent += page->get_num_tuples();
    }
    print("flushing");
    manager_send.flush_all();
    print("listening for last page");
    while (not last_page) {
        std::tie(network_page, std::ignore) = manager_recv.get_page();
        if (network_page) {
            last_page = network_page->is_last_page();
            tuples_recv += network_page->get_num_tuples();
            manager_recv.done_page(network_page);
            if (not last_page) {
                manager_recv.post_recvs(0);
            }
            pages_recv++;
        }
    }
    print("waiting");
    manager_send.wait_all();
    print("done");

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
