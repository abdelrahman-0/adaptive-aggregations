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

DEFINE_int32(connections, 1, "number of ingress connections");
DEFINE_bool(sqpoll, false, "use submission queue polling");
DEFINE_uint32(depth, 64, "number of io_uring entries for network I/O");

using NetworkPage = PageCommunication<int64_t>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Connection conn_ingress{FLAGS_connections, ""};
    conn_ingress.setup_ingress();

    io_uring ring{};
    int ret;
    if ((ret = io_uring_queue_init(FLAGS_depth, &ring, FLAGS_sqpoll ? IORING_SETUP_SQPOLL : 0))) {
        throw IOUringInitError{ret};
    }

    if ((ret = io_uring_register_files(&ring, conn_ingress.socket_fds.data(), conn_ingress.num_connections)) < 0) {
        throw IOUringRegisterFileError{ret};
    }

    // track metrics
    Stopwatch swatch{};
    uint64_t pages_received{0};
    uint64_t tuples_received{0};
    NetworkPage page{};

    int32_t res;
    io_uring_cqe* cqe{};
    int32_t next_conn{0};
    uint32_t done_conn{0};

    // receiver loop
    swatch.start();
    while (done_conn < FLAGS_connections) {
        auto* sqe = io_uring_get_sqe(&ring);
        io_uring_prep_recv(sqe, next_conn, &page, defaults::network_page_size, MSG_WAITALL);
        sqe->flags |= IOSQE_FIXED_FILE;
        io_uring_submit_and_wait(&ring, 1);
        cqe = nullptr;
        io_uring_peek_cqe(&ring, &cqe);
        if (cqe == nullptr || (res = cqe->res) == 0) {
            break;
        }
        if (res < 0) {
            throw NetworkRecvError{res};
        }
        assert(res == defaults::network_page_size);
        auto bytes_received = res;
        io_uring_cqe_seen(&ring, cqe);
        if (page.is_empty()) {
            println("finished consuming ingress");
            pages_received--;
            done_conn++;
        }
        pages_received++;
        tuples_received += page.num_tuples;
        next_conn = (next_conn + 1) % FLAGS_connections;
    }
    swatch.stop();

    Logger logger{};
    logger.log("sqpoll", FLAGS_sqpoll);
    logger.log("connections", FLAGS_connections);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", pages_received);
    logger.log("tuples", tuples_received);
    logger.log("time (s)", swatch.time_ms);
    logger.log("bandwidth (Gb/s)", (pages_received * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));

    println("ingress done");
}
