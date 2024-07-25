#include <gflags/gflags.h>
#include <tbb/enumerable_thread_specific.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>
#include <thread>

#include "common/io_uring_pool.h"
#include "defaults.h"
#include "network/connection.h"
#include "network/network_manager.h"
#include "storage/chunked_list.h"
#include "storage/policy.h"
#include "storage/table.h"
#include "utils/hash.h"
#include "utils/stopwatch.h"
#include "utils/utils.h"

DEFINE_int32(connections, 1, "number of egress connections");
DEFINE_uint32(pages, 10'000, "total number of pages to send via egress traffic");

using NetworkPage = PageCommunication<int64_t>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // setup connections
    auto destination_ip = std::string{defaults::subnet} + std::to_string(defaults::receiver_host_base);
    Connection conn{FLAGS_connections, destination_ip};
    conn.setup_egress();

    // track metrics
    Stopwatch swatch{};
    uint64_t pages_sent{0};
    uint64_t tuples_sent{0};

    NetworkPage page{};
    page.num_tuples = NetworkPage::max_num_tuples_per_page;
    int next_conn{0};
    uint64_t bytes_sent{0};

    // send loop
    swatch.start();
    while (pages_sent < FLAGS_pages) {
        auto page_bytes_sent{0u};
        do {
            page_bytes_sent += ::send(conn.socket_fds[next_conn], reinterpret_cast<std::byte*>(&page) + page_bytes_sent,
                                      defaults::network_page_size - page_bytes_sent, 0);

        } while (page_bytes_sent != defaults::network_page_size);
        bytes_sent += page_bytes_sent;
        pages_sent++;
        tuples_sent += page.num_tuples;
        next_conn = (next_conn + 1) % FLAGS_connections;
    }
    println(bytes_sent);
    //    assert(bytes_sent == defaults::network_page_size * FLAGS_pages);

    // send empty page as end of stream
    //    page.clear();
    //    for (auto i{0u}; i < FLAGS_connections; ++i) {
    //        auto ret = ::send(conn.socket_fds[next_conn], &page, defaults::network_page_size, 0);
    //        assert(ret == defaults::network_page_size);
    //    }
    swatch.stop();

    Logger logger{};
    logger.log("traffic", "egress"s);
    logger.log("implementation", "io_uring"s);
    logger.log("connections", FLAGS_connections);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", pages_sent);
    logger.log("tuples", tuples_sent);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("bandwidth (Gb/s)", (pages_sent * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));

    println("egress done");
}
