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

DEFINE_int32(connections, 10, "number of egress connections");
DEFINE_uint32(pages, 100, "total number of pages to send via egress traffic");

using NetworkPage = PageCommunication<int64_t>;

inline void send_page(NetworkPage& page, int dst_fd) {
    ::ssize_t page_bytes_sent = 0;
    do {
        page_bytes_sent += ::send(dst_fd, reinterpret_cast<std::byte*>(&page) + page_bytes_sent,
                                  defaults::network_page_size - page_bytes_sent, 0);

        if (page_bytes_sent == -1) {
            throw NetworkSendError{};
        }
    } while (page_bytes_sent != defaults::network_page_size);
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // setup connections
    auto destination_ip = std::string{defaults::subnet} + std::to_string(defaults::receiver_host_base);
    Connection conn{FLAGS_connections, destination_ip};
    conn.setup_egress();

    // thread pool
    std::vector<std::thread> threads{};
    std::atomic<bool> wait{true};

    // track metrics
    std::atomic<uint64_t> pages_sent{0};
    std::atomic<uint64_t> pages_sent_actual{0};
    std::atomic<uint64_t> tuples_sent{0};

    for (auto i{0u}; i < FLAGS_connections; ++i) {
        threads.emplace_back([&pages_sent_actual, &wait, &pages_sent, &tuples_sent, dst_fd = conn.socket_fds[i]]() {
            NetworkPage page{};
            page.num_tuples = NetworkPage::max_num_tuples_per_page;

            // local metrics
            uint64_t local_pages_sent{0};
            uint64_t local_tuples_sent{0};

            while (wait)
                ;

            while (pages_sent.fetch_add(1) < FLAGS_pages) {
                send_page(page, dst_fd);
                local_pages_sent++;
                local_tuples_sent += page.num_tuples;
            }
            // send empty page to mark End-Of-Stream
            page.clear();
            send_page(page, dst_fd);
            pages_sent_actual += local_pages_sent;
            tuples_sent += local_tuples_sent;
        });
    }

    // measure time, excluding thread creation/destruction time
    Stopwatch swatch{};
    swatch.start();
    wait = false;
    for (auto& t : threads) {
        t.join();
    }
    swatch.stop();

    Logger logger{};
    logger.log("traffic", "egress"s);
    logger.log("primitive", "send"s);
    logger.log("implementation", "sync"s);
    logger.log("threads", FLAGS_connections);
    logger.log("connections", FLAGS_connections);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", pages_sent_actual);
    logger.log("tuples", tuples_sent);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("throughput (Gb/s)",
               (pages_sent_actual * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));
}
