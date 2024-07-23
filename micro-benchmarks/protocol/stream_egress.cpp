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

// DEFINE_uint32(egress, 1, "number of egress nodes");
DEFINE_int32(connections, 1, "number of connections to use (1 thread per connection)");
DEFINE_bool(sqpoll, false, "use submission queue polling");
DEFINE_uint32(depth, 64, "number of io_uring entries for network I/O");
DEFINE_uint32(pages, 10'000, "total number of pages to send via egress traffic");

using NetworkPage = PageCommunication<int64_t>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // setup connections
    auto destination_ip = std::string{defaults::subnet} + std::to_string(defaults::receiver_host_base);
    Connection conn{FLAGS_connections, destination_ip};
    conn.setup_egress();

    // setup uring
    io_uring ring{};
    int ret;
    if ((ret = io_uring_queue_init(FLAGS_depth, &ring, FLAGS_sqpoll ? IORING_SETUP_SQPOLL : 0))) {
        throw IOUringInitError{ret};
    }

    // register egress socket fds
    if ((ret = io_uring_register_files(&ring, conn.socket_fds.data(), conn.num_connections)) < 0) {
        throw IOUringRegisterFileError{ret};
    }

    // track metrics
    Stopwatch swatch{};
    uint64_t pages_sent{0};
    uint64_t tuples_sent{0};
    uint64_t cqes_seen{0};

    NetworkPage page{};
    std::vector<io_uring_cqe*> cqes(FLAGS_depth * 2, nullptr);
    page.num_tuples = NetworkPage::max_num_tuples_per_page;
    int next_conn{0};

    // send loop
    swatch.start();
    while (pages_sent < FLAGS_pages) {
        auto* sqe = io_uring_get_sqe(&ring);
        io_uring_prep_send(sqe, next_conn, &page, defaults::network_page_size, 0);
        sqe->flags |= IOSQE_FIXED_FILE;
        ret = io_uring_submit_and_wait(&ring, 1);
        assert(ret == 1);
        //        io_uring_cqe* cqe{nullptr};
        //        io_uring_peek_cqe(&ring, &cqe);
        //        auto bytes_sent = cqe->res;
        //        io_uring_cqe_seen(&ring, cqe);
        auto peeked = io_uring_peek_batch_cqe(&ring, cqes.data(), 256);
        for (auto i{0u}; i < peeked; ++i) {
            if (cqes[i]->res <= 0) {
                throw NetworkSendError{cqes[i]->res};
            }
            assert(cqes[i]->res == defaults::network_page_size);
        }
        io_uring_cq_advance(&ring, peeked);
        cqes_seen += peeked;

        //        while (bytes_sent != defaults::network_page_size) {
        //            // handle page fragmentation
        //            println("fragmentation");
        //            sqe = io_uring_get_sqe(&ring);
        //            io_uring_prep_send(sqe, next_conn, reinterpret_cast<std::byte*>(&page) + bytes_sent,
        //                               defaults::network_page_size - bytes_sent, 0);
        //            sqe->flags |= IOSQE_FIXED_FILE;
        //            ret = io_uring_submit_and_wait(&ring, 1);
        //            assert(ret == 1);
        //            cqe = nullptr;
        //            io_uring_peek_cqe(&ring, &cqe);
        //            if (cqe->res <= 0) {
        //                throw NetworkSendError{cqe->res};
        //            }
        //            bytes_sent += cqe->res;
        //            io_uring_cqe_seen(&ring, cqe);
        //        }
        pages_sent++;
        tuples_sent += page.num_tuples;
        next_conn = (next_conn + 1) % conn.num_connections;
    }
    io_uring_wait_cqe_nr(&ring, cqes.data(), FLAGS_pages - cqes_seen);
    for (auto i{0u}; i < FLAGS_pages - cqes_seen; ++i) {
        if (cqes[i]->res <= 0) {
            throw NetworkSendError{cqes[i]->res};
        }
        assert(cqes[i]->res == defaults::network_page_size);
    }
    io_uring_cq_advance(&ring, FLAGS_pages - cqes_seen);

    // send empty page as end of stream
    page.clear();
    for (auto i = 0; i < conn.num_connections; ++i) {
        auto* sqe = io_uring_get_sqe(&ring);
        io_uring_prep_send(sqe, i, &page, defaults::network_page_size, 0);
        sqe->flags |= IOSQE_FIXED_FILE;
        ret = io_uring_submit_and_wait(&ring, 1);
        assert(ret == 1);
        io_uring_cqe* cqe{nullptr};
        io_uring_peek_cqe(&ring, &cqe);
        if (cqe->res <= 0) {
            throw NetworkSendError{cqe->res};
        }
        assert(cqe->res == defaults::network_page_size);
        io_uring_cqe_seen(&ring, cqe);
    }
    swatch.stop();

    Logger logger{};
    logger.log("sqpoll", FLAGS_sqpoll);
    logger.log("connections", FLAGS_connections);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", pages_sent);
    logger.log("tuples", tuples_sent);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("bandwidth (Gb/s)", (pages_sent * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));

    println("egress done");
}
