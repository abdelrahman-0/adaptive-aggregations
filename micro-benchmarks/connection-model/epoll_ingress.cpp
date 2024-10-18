#include <gflags/gflags.h>
#include <sys/epoll.h>
#include <tbb/enumerable_thread_specific.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>
#include <thread>

#include "defaults.h"
#include "network/connection.h"
#include "network/network_manager_old.h"
#include "performance/stopwatch.h"
#include "storage/chunked_list.h"
#include "storage/policy.h"
#include "storage/table.h"
#include "utils/hash.h"
#include "utils/logger.h"
#include "utils/utils.h"

DEFINE_int32(connections, 1, "number of ingress connections");
DEFINE_bool(sqpoll, false, "use submission queue polling");
DEFINE_uint32(depth, 128, "number of io_uring entries for network I/O");

using NetworkPage = PageCommunication<int64_t>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Connection conn_ingress{FLAGS_connections};
    conn_ingress.setup_ingress();

    io_uring ring{};
    int ret;
    if ((ret = io_uring_queue_init(FLAGS_depth, &ring, FLAGS_sqpoll ? IORING_SETUP_SQPOLL : 0))) {
        throw IOUringInitError{ret};
    }

    // setup epoll
    std::vector<::epoll_event> events(FLAGS_depth);
    ::epoll_event ev{};
    int epollfd = ::epoll_create1(0);
    if (epollfd == -1) {
        perror("epoll_create1");
        exit(EXIT_FAILURE);
    }
    for (auto fd : conn_ingress.socket_fds) {
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        if (::epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &ev) == -1) {
            logln("epoll_ctl: conn_sock", fd);
            exit(EXIT_FAILURE);
        }
    }

    // track metrics
    Stopwatch swatch{};
    uint64_t pages_received{0};
    uint64_t tuples_received{0};
    std::vector<NetworkPage> pages(FLAGS_depth);

    int32_t res;
    std::vector<io_uring_cqe*> cqes(FLAGS_depth * 2);
    uint32_t done_conn{0};

    // receiver loop
    int polled;
    swatch.start();
    while (done_conn < FLAGS_connections) {

        do {
            polled = epoll_wait(epollfd, events.data(), events.size(), -1);
        } while (polled == -1);

        for (auto i{0u}; i < polled; ++i) {
            auto* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_recv(sqe, events[i].data.fd, pages.data() + i, defaults::network_page_size, MSG_WAITALL);
            sqe->user_data = i;
        }
        io_uring_submit_and_wait(&ring, polled);
        auto peeked = io_uring_peek_batch_cqe(&ring, cqes.data(), polled);
        assert(peeked == polled);
        for (auto i{0u}; i < peeked; ++i) {
            if (cqes[i] == nullptr || (res = cqes[i]->res) == 0) {
                break;
            }
            if (res < 0) {
                throw NetworkRecvError{res};
            }
            assert(res == defaults::network_page_size);
            auto& page = pages[cqes[i]->user_data];
            if (page.is_empty()) {
                println("finished consuming ingress");
                pages_received--;
                done_conn++;
            }
            pages_received++;
            tuples_received += page.num_tuples;
        }
        io_uring_cq_advance(&ring, peeked);
    }
    swatch.stop();

    Logger logger{};
    logger.log("traffic", "ingress"s);
    logger.log("implementation", "io_uring_epoll"s);
    logger.log("sqpoll", FLAGS_sqpoll);
    logger.log("connections", FLAGS_connections);
    logger.log("page size", defaults::network_page_size);
    logger.log("pages", pages_received);
    logger.log("tuples", tuples_received);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("bandwidth (Gb/s)", (pages_received * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));

    println("ingress done");
}
