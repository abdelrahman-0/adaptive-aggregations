#include <gflags/gflags.h>
#include <likwid-marker.h>
#include <sys/resource.h>
#include <thread>
#include <utility>

#include "common/concurrent_mpsc_queue.h"
#include "common/io_uring_pool.h"
#include "defaults.h"
#include "exceptions/exceptions_misc.h"
#include "network/connection.h"
#include "performance/stopwatch.h"
#include "storage/chunked_list.h"
#include "utils/hash.h"

// std::min(1u, std::thread::hardware_concurrency() - FLAGS_connections)
DEFINE_uint32(connections, 5, "number of egress connections to open (1 thread per connection)");
DEFINE_uint32(workers, 10, "number of query-processing (non-networking) threads to use");
DEFINE_uint64(pages, 1000, "total number of pages to send via egress traffic");
DEFINE_uint64(batch, 10, "number of pages that each thread will process at once");
DEFINE_uint64(mpsc, 10, "size of thread-local MPSC queues used by networking threads");

using NetworkPage = PageNetwork<int64_t>;

inline void send_page(NetworkPage& page, int dst_fd) {
    ::ssize_t res;
    ::ssize_t page_bytes_sent = 0;
    do {
        res = ::send(dst_fd, reinterpret_cast<std::byte*>(&page) + page_bytes_sent,
                     defaults::network_page_size - page_bytes_sent, 0);

        if (res == -1) {
            throw NetworkSendError{};
        }
        page_bytes_sent += res;
        // handle short sends in a loop
    } while (page_bytes_sent != defaults::network_page_size);
}

int main(int argc, char* argv[]) {
    LIKWID_MARKER_INIT;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // setup connections
    auto destination_ip = std::string{defaults::subnet} + std::to_string(defaults::node_port_base);
    Connection conn{FLAGS_connections, destination_ip};
    conn.setup_egress();

    // thread pool
    std::vector<std::thread> threads_network{};
    std::vector<std::thread> threads_workers{};
    std::vector<ConcurrentMPSCQueue<NetworkPage*>> tls_mpscs{};
    std::vector<std::atomic<uint32_t>> done_pages(FLAGS_connections);
    std::vector<NetworkPage> tls_pages{};
    std::atomic<bool> done{false};
    std::atomic<bool> wait{true};

    // spawn network threads
    for (auto i{0u}; i < FLAGS_connections; ++i) {
        done_pages[i] = 0;
        tls_mpscs._emplace_back((FLAGS_workers / FLAGS_connections) + (i < FLAGS_workers % FLAGS_connections),
                                FLAGS_mpsc);
        threads_network.emplace_back([i, dst_fd = conn.socket_fds[i], &done, &tls_mpscs, &done_pages, &wait]() {
            uint32_t local_pages_sent{0};
            auto level = 0u;
            while (wait)
                ;
            while (!done or local_pages_sent < done_pages[i]) {
                if (local_pages_sent < done_pages[i]) {
                    send_page(*tls_mpscs[i].get(), dst_fd);
                    local_pages_sent++;
                }
            }
            // send empty page
            NetworkPage empty_page{};
            send_page(empty_page, dst_fd);
        });
    }

    // track metrics
    std::atomic<uint64_t> pages_sent{0};
    std::atomic<uint64_t> pages_sent_actual{0};
    std::atomic<uint64_t> tuples_sent{0};
    std::atomic<uint32_t> workers_done{0};
    Logger logger_thread{};

    // spawn worker threads
    for (auto i{0u}; i < FLAGS_workers; ++i) {
        tls_pages._emplace_back();
        tls_pages.back().num_tuples = NetworkPage::max_num_tuples_per_page;
        threads_workers.emplace_back([i, &done, &tls_mpscs, &tls_pages, &wait, &pages_sent, &pages_sent_actual,
                                      &tuples_sent, &workers_done, &done_pages]() {
            auto network_thread = i % FLAGS_connections;
            auto& mpsc = tls_mpscs[network_thread];
            // local metrics
            uint64_t local_pages_sent{0};
            uint64_t local_tuples_sent{0};
            uint64_t prev_pages_sent{0};

            while (wait)
                ;

            while ((prev_pages_sent = pages_sent.fetch_add(FLAGS_batch)) < FLAGS_pages) {
                auto pages_to_send = std::min(FLAGS_batch, FLAGS_pages - prev_pages_sent);
                for (auto j{0u}; j < pages_to_send; ++j) {
                    mpsc.insert(i / FLAGS_connections, tls_pages.data() + i);
                    local_pages_sent++;
                    local_tuples_sent += tls_pages[i].num_tuples;
                }
                done_pages[network_thread] += pages_to_send;
            }

            // send empty page
            pages_sent_actual += local_pages_sent;
            tuples_sent += local_tuples_sent;
            if (++workers_done == FLAGS_workers) {
                done = true;
            }
        });
    }

    // measure time, excluding thread creation/destruction time
    //    Stopwatch swatch{};
    //    swatch.start();
    wait = false;
    for (auto& t : threads_workers) {
        t.join();
    }

    for (auto& t : threads_network) {
        t.join();
    }
    //    swatch.stop();

    Logger logger{};
    logger.log("traffic", "egress"s);
    logger.log("primitive", "send"s);
    logger.log("implementation", "sync"s);
    logger.log("threads", FLAGS_connections);
    logger.log("connections", FLAGS_connections);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", pages_sent_actual);
    logger.log("tuples", tuples_sent);
    //    logger.log("time (ms)", swatch.time_ms);
    //    logger.log("throughput (Gb/s)",
    //               (pages_sent_actual * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));

    LIKWID_MARKER_CLOSE;
}
