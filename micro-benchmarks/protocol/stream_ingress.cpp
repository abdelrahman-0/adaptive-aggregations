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

DEFINE_int32(ingress, 1, "number of ingress nodes");
DEFINE_uint32(depth, 256, "number of io_uring entries for network I/O");
DEFINE_uint32(threads, 1, "number of threads to use");

using NetworkPage = PageCommunication<int64_t>;

int main() {

    Connection conn_ingress{FLAGS_ingress};
    conn_ingress.setup_ingress();

    std::atomic<uint64_t> pages_received{0};
    std::atomic<uint64_t> tuples_received{0};
    std::atomic<bool> fragmented_once{false};
    std::vector<std::thread> threads;
    Logger logger{};

    for (auto i{0u}; i < FLAGS_threads; ++i) {
        threads.emplace_back([&]() {
            io_uring ring{};
            int ret;
            if ((ret = io_uring_queue_init(FLAGS_depth, &ring, 0))) {
                throw IOUringInitError{ret};
            }

            if ((ret = io_uring_register_files(&ring, conn_ingress.socket_fds.data(), conn_ingress.num_connections)) <
                0) {
                throw IOUringRegisterFileError{ret};
            }

            NetworkPage page{};

            io_uring_cqe* cqe{nullptr};
            auto local_pages_received = 0u;
            auto local_tuples_received = 0u;
            int res;
            while (true) {
                auto* sqe = io_uring_get_sqe(&ring);
                io_uring_prep_recv(sqe, 0, &page, defaults::network_page_size, MSG_WAITALL);
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
                    break;
                }
                local_pages_received++;
                local_tuples_received += page.num_tuples;
            }
            pages_received += local_pages_received;
            tuples_received += local_tuples_received;
        });
    }

    {
        Stopwatch _{logger};
        for (auto& t : threads) {
            t.join();
        }
    }

    logger.log("nodes", FLAGS_ingress);
    logger.log("threads", FLAGS_threads);
    logger.log("pages", pages_received);
    logger.log("tuples", tuples_received);

    println("ingress done");
}
