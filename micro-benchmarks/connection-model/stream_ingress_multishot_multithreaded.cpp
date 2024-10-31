#include <gflags/gflags.h>
#include <tbb/enumerable_thread_specific.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>
#include <thread>

#include "bench/logger.h"
#include "defaults.h"
#include "network/connection.h"
#include "network/network_manager_old.h"
#include "performance/stopwatch.h"
#include "storage/chunked_list.h"
#include "storage/policy.h"
#include "storage/table.h"
#include "utils/hash.h"
#include "utils/utils.h"

DEFINE_int32(connections, 1, "number of ingress connections");
DEFINE_bool(sqpoll, false, "use submission queue polling");
DEFINE_uint32(depth, 128, "number of io_uring entries for network I/O");
DEFINE_uint32(buffers, 512, "number of buffers to use for multishot receive");
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
    std::atomic<uint64_t> bytes_received{0};
    std::atomic<uint64_t> tuples_received{0};
    std::atomic<uint64_t> threads_done{0};

    // pre-register 1 socket fd per rig
    for (auto i{0u}; i < FLAGS_connections; ++i) {
        auto& ring = rings[i];
        int ret;
        if ((ret = io_uring_queue_init(FLAGS_depth, &ring, FLAGS_sqpoll ? IORING_SETUP_SQPOLL : 0))) {
            throw IOUringInitError{ret};
        }

        if ((ret = io_uring_register_files(&ring, conn_ingress.socket_fds.data() + i, 1)) < 0) {
            throw IOUringRegisterFileError{ret};
        }

        threads.emplace_back([&ring, &wait, &pages_received, &bytes_received, &threads_done]() {
            int ret{0};
            auto* buf_ring = io_uring_setup_buf_ring(&ring, FLAGS_buffers, 0, 0, &ret);
            FLAGS_buffers = next_power_of_2(FLAGS_buffers);
            std::vector<NetworkPage> buffers(FLAGS_buffers);
            for (auto i{0u}; i < FLAGS_buffers; i++) {
                io_uring_buf_ring_add(buf_ring, buffers.data() + i, defaults::network_page_size, i,
                                      io_uring_buf_ring_mask(FLAGS_buffers), i);
            }
            io_uring_buf_ring_advance(buf_ring, FLAGS_buffers);

            // prepare multi-shot
            auto sqe = io_uring_get_sqe(&ring);
            io_uring_prep_recv_multishot(sqe, 0, buffers.data(), 0, 0);
            sqe->flags |= IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT;
            sqe->buf_group = 0;
            std::array<io_uring_cqe*, 128> cqes{};
            auto local_bytes_received{0u};
            auto local_pages_received{0u};

            auto num_submitted = io_uring_submit(&ring);
            assert(num_submitted == 1);
            io_uring_cqe* cqe;
            ret = io_uring_wait_cqe(&ring, &cqe);
            if (cqe->res < 0) {
                throw IOUringMultiShotRecvError{cqe->res};
            }

            while (wait)
                ;

            while (true) {
                auto count = io_uring_peek_batch_cqe(&ring, cqes.data(), cqes.size());
                for (auto i{0u}; i < count; i++) {
                    cqe = cqes[i];
                    if (cqe->res < 0) {
                        throw NetworkRecvError{cqe->res};
                    }
                    if (cqe->res == 0) {
                        goto done;
                    }

                    auto idx = cqe->flags >> 16;

                    if (cqe->res <= defaults::network_page_size) {
                        local_bytes_received += cqe->res;
                    }

                    io_uring_buf_ring_add(buf_ring, buffers.data() + idx, defaults::network_page_size, idx,
                                          io_uring_buf_ring_mask(FLAGS_buffers), 0);
                    io_uring_buf_ring_advance(buf_ring, 1);
                    io_uring_cqe_seen(&ring, cqe);
                    if (!(cqe->flags & IORING_CQE_F_MORE)) {
                        sqe = io_uring_get_sqe(&ring);
                        io_uring_prep_recv_multishot(sqe, 0, buffers.data(), 0, 0);
                        sqe->flags |= IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT;
                        sqe->buf_group = 0;
                        num_submitted = io_uring_submit(&ring);
                        assert(num_submitted == 1);
                    }
                }
                bytes_received += local_bytes_received;
                local_bytes_received = 0;
                if (bytes_received / defaults::network_page_size == 10'000) {
                    break;
                }
            }

        done:;
            bytes_received += local_bytes_received;
            threads_done++;
        });
    }

    // track metrics
    Stopwatch swatch{};
    swatch.start();
    wait = false;
    while(threads_done != FLAGS_connections);
    swatch.stop();
    pages_received = bytes_received / defaults::network_page_size;
    for (auto& thread : threads) {
        thread.join();
    }

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
