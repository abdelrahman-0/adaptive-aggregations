#include <gflags/gflags.h>
#include <likwid-marker.h>
#include <sys/resource.h>
#include <thread>

#include "common/io_uring_pool.h"
#include "defaults.h"
#include "exceptions/exceptions_misc.h"
#include "network/connection.h"
#include "storage/chunked_list.h"
#include "utils/hash.h"
#include "utils/stopwatch.h"

DEFINE_uint32(connections, 10, "number of egress connections");
DEFINE_uint64(pages, 100'000, "total number of pages to send via egress traffic");
DEFINE_uint64(batch, 10, "number of pages that each thread will process at once");

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
    Logger logger_thread{};

    for (auto i{0u}; i < FLAGS_connections; ++i) {
        threads.emplace_back(
            [&pages_sent_actual, &wait, &pages_sent, &tuples_sent, &logger_thread, dst_fd = conn.socket_fds[i]]() {
                LIKWID_MARKER_THREADINIT;
                LIKWID_MARKER_REGISTER("send");

                NetworkPage page{};
                page.num_tuples = NetworkPage::max_num_tuples_per_page;

                // local metrics
                uint64_t local_pages_sent{0};
                uint64_t local_tuples_sent{0};
                uint64_t prev_pages_sent{0};

                while (wait)
                    ;

                LIKWID_MARKER_START("send");
                ::rusage usage_start{};
                if (::getrusage(RUSAGE_THREAD, &usage_start) == -1) {
                    throw GetResourceUsageError{};
                }
                while ((prev_pages_sent = pages_sent.fetch_add(FLAGS_batch)) < FLAGS_pages) {
                    for (auto i{0u}; i < std::min(FLAGS_batch, FLAGS_pages - prev_pages_sent); ++i) {
                        send_page(page, dst_fd);
                        local_pages_sent++;
                        local_tuples_sent += page.num_tuples;
                    }
                }
                // send empty page to mark End-Of-Stream
                page.clear();
                send_page(page, dst_fd);
                pages_sent_actual += local_pages_sent;
                tuples_sent += local_tuples_sent;
                ::rusage usage_stop{};
                if (::getrusage(RUSAGE_THREAD, &usage_stop) == -1) {
                    throw GetResourceUsageError{};
                }
                LIKWID_MARKER_STOP("send");
                auto cpu_id = "Thread "s + std::to_string(sched_getcpu());
                logger_thread.log(cpu_id + " - context switches (invol)", usage_stop.ru_nivcsw - usage_start.ru_nivcsw);
                logger_thread.log(cpu_id + " - context switches (vol)", usage_stop.ru_nvcsw - usage_start.ru_nvcsw);
                logger_thread.log(cpu_id + " - user time (ms)",
                                  ((usage_stop.ru_utime.tv_sec - usage_start.ru_utime.tv_sec) * 1'000'000 +
                                   usage_stop.ru_utime.tv_usec - usage_start.ru_utime.tv_usec) /
                                      1000);
                logger_thread.log(cpu_id + " - kernel time (ms)",
                                  ((usage_stop.ru_stime.tv_sec - usage_start.ru_stime.tv_sec) * 1'000'000 +
                                   usage_stop.ru_stime.tv_usec - usage_start.ru_stime.tv_usec) /
                                      1000);
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

    LIKWID_MARKER_CLOSE;
}
