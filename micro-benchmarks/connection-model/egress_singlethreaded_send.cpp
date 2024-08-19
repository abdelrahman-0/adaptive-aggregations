#include <cassert>
#include <gflags/gflags.h>
#include <likwid-marker.h>
#include <sched.h>
#include <sys/resource.h>

#include "common/io_uring_pool.h"
#include "defaults.h"
#include "exceptions/exceptions_misc.h"
#include "network/connection.h"
#include "storage/chunked_list.h"
#include "utils/hash.h"
#include "utils/stopwatch.h"

DEFINE_int32(connections, 10, "number of egress connections");
DEFINE_uint32(pages, 100'000, "total number of pages to send via egress traffic");

using NetworkPage = PageNetwork<int64_t>;

int main(int argc, char* argv[]) {
    LIKWID_MARKER_INIT;
    LIKWID_MARKER_REGISTER("send");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // setup connections
    auto destination_ip = std::string{defaults::subnet} + std::to_string(defaults::node_port_base);
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
    LIKWID_MARKER_START("send");
    ::rusage usage_start{};
    if (::getrusage(RUSAGE_THREAD, &usage_start) == -1) {
        throw GetResourceUsageError{};
    }
    while (pages_sent < FLAGS_pages) {
        auto page_bytes_sent{0u};
        ::ssize_t res;
        do {
            res = ::send(conn.socket_fds[next_conn], reinterpret_cast<std::byte*>(&page) + page_bytes_sent,
                         defaults::network_page_size - page_bytes_sent, 0);
            if (res == -1) {
                throw NetworkSendError{};
            }
            page_bytes_sent += res;
        } while (page_bytes_sent != defaults::network_page_size);
        bytes_sent += page_bytes_sent;
        pages_sent++;
        tuples_sent += page.num_tuples;
        next_conn = (next_conn + 1) % FLAGS_connections;
    }

    // send empty page as end of stream
    page.clear();
    for (auto i{0u}; i < FLAGS_connections; ++i) {
        auto ret = ::send(conn.socket_fds[next_conn], &page, defaults::network_page_size, 0);
        assert(ret == defaults::network_page_size);
    }
    ::rusage usage_stop{};
    if (::getrusage(RUSAGE_THREAD, &usage_stop) == -1) {
        throw GetResourceUsageError{};
    }
    LIKWID_MARKER_STOP("send");
    swatch.stop();

    Logger logger_thread{};
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

    Logger logger{};
    logger.log("traffic", "egress"s);
    logger.log("primitive", "send"s);
    logger.log("implementation", "sync"s);
    logger.log("threads", 1);
    logger.log("connections", FLAGS_connections);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", pages_sent);
    logger.log("tuples", tuples_sent);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("throughput (Gb/s)", (pages_sent * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));

    LIKWID_MARKER_CLOSE;
}
