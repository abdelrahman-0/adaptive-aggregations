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

DEFINE_int32(connections, 10, "number of ingress connections");
DEFINE_uint32(buffers, 1, "number of buffer pages");

using NetworkPage = PageCommunication<int64_t>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Connection conn{FLAGS_connections};
    conn.setup_ingress();

    // thread pool
    std::vector<std::thread> threads{};
    std::atomic<bool> wait{true};

    // track metrics
    std::atomic<uint64_t> pages_received{0};
    std::atomic<uint64_t> tuples_received{0};

    // pre-register 1 socket fd per rig
    for (auto i{0u}; i < FLAGS_connections; ++i) {

        threads.emplace_back([i, &conn, &wait, &pages_received, &tuples_received]() {
            std::vector<NetworkPage> pages(FLAGS_buffers);
            uint64_t local_pages_received{0};
            uint64_t local_tuples_received{0};
            uint64_t unprocessed_bytes{0};
            ::ssize_t res{0};

            while (wait)
                ;

            // receiver loop
            while (true) {
                unprocessed_bytes = 0;
                do {
                    res = ::recv(conn.socket_fds[i], reinterpret_cast<std::byte*>(pages.data()) + unprocessed_bytes,
                                 defaults::network_page_size * FLAGS_buffers - unprocessed_bytes, MSG_WAITALL);
                    if (res == -1) {
                        throw NetworkRecvError{};
                    }
                    unprocessed_bytes += res;
                    // read whole pages
                } while (unprocessed_bytes % defaults::network_page_size != 0);
                for (auto bytes_processed{0u}; bytes_processed < unprocessed_bytes;
                     bytes_processed += defaults::network_page_size) {
                    auto& page = pages[bytes_processed / defaults::network_page_size];
                    if (page.num_tuples == 0) {
                        goto done;
                    }
                    local_tuples_received += page.num_tuples;
                    local_pages_received++;
                }
                if (res == 0) {
                    goto done;
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
    logger.log("bandwidth (Gb/s)", (pages_received * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));

    println("ingress done");
}
