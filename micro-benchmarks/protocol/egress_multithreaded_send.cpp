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
DEFINE_uint32(pages, 1000'000, "total number of pages to send via egress traffic");

using NetworkPage = PageCommunication<int64_t>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // setup connections
    auto destination_ip = std::string{defaults::subnet} + std::to_string(defaults::receiver_host_base);
    Connection conn{FLAGS_connections, destination_ip};
    conn.setup_egress();

    // thread pool
    std::vector<std::thread> threads{};
    std::atomic<bool> wait{true};
    std::atomic<uint64_t> pages_sent{0};
    std::atomic<uint64_t> actual_pages_sent{0};
    std::atomic<uint64_t> tuples_sent{0};

    for (auto i{0u}; i < FLAGS_connections; ++i) {
        threads.emplace_back([&actual_pages_sent, &wait, &pages_sent, dst_fd=conn.socket_fds[i]]() {
            NetworkPage page{};
            page.num_tuples = NetworkPage::max_num_tuples_per_page;
            uint64_t bytes_sent{0};
            uint64_t local_pages_sent{0};
            ::ssize_t res{0};

            while (wait)
                ;

            while (pages_sent.fetch_add(1) < FLAGS_pages) {
                res = ::send(dst_fd, &page, defaults::network_page_size, 0);
                if (res == -1){
                    println(strerror(errno));
                    exit(0);
                }
                if (res < defaults::network_page_size) {
                    println("short write");
                }
                local_pages_sent++;
            }
            actual_pages_sent += local_pages_sent;
            page.clear();
            res = ::send(dst_fd, &page, defaults::network_page_size, 0);
            assert(res == defaults::network_page_size);
        });
    }

    // send loop
    Stopwatch swatch{};
    swatch.start();
    wait = false;
    for (auto& t : threads) {
        t.join();
    }
    swatch.stop();

    Logger logger{};
    logger.log("traffic", "egress"s);
    logger.log("implementation", "send"s);
    logger.log("connections", FLAGS_connections);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", actual_pages_sent);
    logger.log("tuples", tuples_sent);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("bandwidth (Gb/s)", (pages_sent * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));

    println("egress done");
}
