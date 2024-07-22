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

    {
        Stopwatch _{};
        for (auto i{0u}; i < FLAGS_threads; ++i) {

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
                assert(res > 0);
                auto bytes_received = res;
                io_uring_cqe_seen(&ring, cqe);
                while (bytes_received != defaults::network_page_size) {
                    fragmented_once = true;
                    sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_recv(sqe, 0, reinterpret_cast<std::byte*>(&page) + bytes_received,
                                       defaults::network_page_size - bytes_received, 0);
                    sqe->flags |= IOSQE_FIXED_FILE;
                    io_uring_submit_and_wait(&ring, 1);
                    cqe = nullptr;
                    io_uring_peek_cqe(&ring, &cqe);
                    if (cqe == nullptr || (res = cqe->res) == 0) {
                        // if ingress traffic is closed, we might miss the final few fragments
                        println("unreachable 2");
                    }
                    res = cqe->res;
                    io_uring_cqe_seen(&ring, cqe);
                    if (res < 0) {
                        throw NetworkRecvError{res};
                    }
                    bytes_received += res;
                }
                if (page.is_empty()) {
                    println("finished consuming ingress");
                    break;
                }
                pages_received++;
                tuples_received += page.num_tuples;
            }
        }
    }
    println("ingress done");

    Logger logger{};
    logger.log("nodes", FLAGS_ingress);
    logger.log("threads", FLAGS_threads);
    logger.log("pages", pages_received);
    logger.log("number of threads", FLAGS_threads);
    logger.log("number of threads", FLAGS_threads);

}
