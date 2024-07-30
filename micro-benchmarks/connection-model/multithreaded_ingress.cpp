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
DEFINE_bool(fixed, true, "whether to pre-register connections file descriptors with io_uring");

using NetworkPage = PageCommunication<int64_t>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Connection conn_ingress{FLAGS_connections};
    conn_ingress.setup_ingress();

    // thread-uring pool
    std::vector<std::thread> threads{};
    std::vector<io_uring> rings(FLAGS_connections);
    std::atomic<bool> wait{true};
    std::atomic<uint64_t> pages_received{0};
    std::atomic<uint64_t> tuples_received{0};

    // pre-register 1 socket fd per rig
    //    for (auto i{0u}; i < FLAGS_connections; ++i) {
    int i = 0;
    auto& ring = rings[i];
    int ret;
    if ((ret = io_uring_queue_init(FLAGS_depth, &ring, FLAGS_sqpoll ? IORING_SETUP_SQPOLL : 0))) {
        throw IOUringInitError{ret};
    }

    if ((ret = io_uring_register_files(&ring, conn_ingress.socket_fds.data() + i, 1)) < 0) {
        throw IOUringRegisterFileError{ret};
    }

    threads.emplace_back([&ring, &wait, &pages_received, &tuples_received]() {
        std::vector<NetworkPage> pages(FLAGS_depth);
        for (auto& page : pages) {
            page.num_tuples = NetworkPage::max_num_tuples_per_page;
        }
        std::vector<uint64_t> free_pages(FLAGS_depth);
        std::iota(free_pages.begin(), free_pages.end(), 0u);
        uint64_t local_pages_received{0};
        uint64_t local_tuples_received{0};
        int32_t res{0};
        std::vector<io_uring_cqe*> cqes(FLAGS_depth * 2, nullptr);

        while (wait)
            ;

        // receiver loop
        bool cont = true;
        while (cont) {
            if (!free_pages.empty()) {
                auto* sqe = io_uring_get_sqe(&ring);
                auto page_idx = free_pages.back();
                io_uring_prep_recv(sqe, 0, pages.data() + page_idx, defaults::network_page_size, MSG_WAITALL);
                sqe->user_data = page_idx;
                sqe->flags |= IOSQE_FIXED_FILE;
                io_uring_submit(&ring);
                free_pages.pop_back();
            }
            auto peeked = io_uring_peek_batch_cqe(&ring, cqes.data(), cqes.size());
            for (auto i{0u}; i < peeked; ++i) {
                if (cqes[i] == nullptr or (res = cqes[i]->res) == 0) {
                    io_uring_cqe_seen(&ring, cqes[i]);
                    println("weird cqe");
                    continue;
                }
                if (res < 0) {
                    throw NetworkRecvError{res};
                }
                assert(res == defaults::network_page_size);
                auto page_idx = cqes[i]->user_data;
                auto& page = pages[page_idx];
                io_uring_cqe_seen(&ring, cqes[i]);
                if (page.is_empty()) {
                    println("saw free page", page_idx, local_pages_received);
                    cont = false;
                    goto done;
                    ;
                }
                local_pages_received++;
                local_tuples_received += page.num_tuples;
                free_pages.push_back(page_idx);
            }
        }
    done:;
        pages_received += local_pages_received;
        tuples_received += local_tuples_received;
    });
    //    }

    // track metrics
    Stopwatch swatch{};
    swatch.start();
    wait = false;
    for (auto& thread : threads) {
        thread.join();
    }
    swatch.stop();

    Logger logger{};
    logger.log("traffic", "ingress"s);
    logger.log("implementation", "io_uring"s);
    logger.log("sqpoll", FLAGS_sqpoll);
    logger.log("fixed fds", FLAGS_fixed);
    logger.log("connections", FLAGS_connections);
    logger.log("page_size", defaults::network_page_size);
    logger.log("pages", pages_received);
    logger.log("tuples", tuples_received);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("bandwidth (Gb/s)", (pages_received * defaults::network_page_size * 8 * 1000) / (1e9 * swatch.time_ms));

    println("ingress done");
}
