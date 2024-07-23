#include <gflags/gflags.h>
#include <thread>

#include "defaults.h"
#include "network/connection.h"
#include "network/network_manager.h"
#include "storage/chunked_list.h"
#include "utils/hash.h"
#include "utils/stopwatch.h"
#include "utils/utils.h"

DEFINE_int32(ingress, 1, "number of ingress nodes");
DEFINE_uint32(depth, 256, "number of io_uring entries for network I/O");
DEFINE_uint32(buffers, 128, "number of buffers to use for multishot receive");

using NetworkPage = PageCommunication<int64_t>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    Connection conn_ingress{FLAGS_ingress};
    conn_ingress.setup_ingress();

    std::atomic<uint64_t> pages_received{0};
    std::atomic<uint64_t> tuples_received{0};
    bool fragmented_once = false;
    io_uring ring{};

    int ret;
    if ((ret = io_uring_queue_init(FLAGS_depth, &ring, 0))) {
        throw IOUringInitError{ret};
    }

    if ((ret = io_uring_register_files(&ring, conn_ingress.socket_fds.data(), conn_ingress.num_connections)) < 0) {
        throw IOUringRegisterFileError{ret};
    }

    // register buffers
    auto* buf_ring = io_uring_setup_buf_ring(&ring, FLAGS_buffers, 0, 0, &ret);
    FLAGS_buffers = next_power_of_2(FLAGS_buffers);
    std::vector<NetworkPage> buffers(FLAGS_buffers);
    for (auto i{0u}; i < FLAGS_buffers; i++) {
        io_uring_buf_ring_add(buf_ring, buffers.data() + i, defaults::network_page_size, i,
                              io_uring_buf_ring_mask(FLAGS_buffers), i);
    }
    io_uring_buf_ring_advance(buf_ring, FLAGS_buffers);

    //////////////////////////////////////

    auto sqe = io_uring_get_sqe(&ring);
    io_uring_prep_recv_multishot(sqe, 0, buffers.data(), 0, 0);
    sqe->flags |= IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT;
    sqe->buf_group = 0;
    std::array<io_uring_cqe*, 128> cqes{};
    auto i = 0u;

    auto num_submitted = io_uring_submit(&ring);
    assert(num_submitted == 1);
    io_uring_cqe* cqe;
    ret = io_uring_wait_cqe(&ring, &cqe);
    if (cqe->res < 0) {
        throw IOUringMultiShotRecvError{cqe->res};
    }

    {
        Stopwatch _{};
        int res;
        while (true) {
            auto count = io_uring_peek_batch_cqe(&ring, cqes.data(), cqes.size());
            for (i = 0; i < count; i++) {
                cqe = cqes[i];
                if (cqe->res < 0) {
                    throw NetworkRecvError{cqe->res};
                }

                if (!(cqe->flags & IORING_CQE_F_MORE) || cqe->res == 0) {
                    goto done;
                }

                auto idx = cqe->flags >> 16;

                if (cqe->res < defaults::network_page_size) {
                    auto bytes_received = cqe->res;
                    println("fragmentation", bytes_received);
                }

                io_uring_buf_ring_add(buf_ring, buffers.data() + idx, defaults::network_page_size, idx,
                                      io_uring_buf_ring_mask(FLAGS_buffers), 0);
                io_uring_buf_ring_advance(buf_ring, 1);
                io_uring_cq_advance(&ring, 1);
            }
        }
    done:;
    }

    //    println("using", FLAGS_threads, "threads");
    println("received", pages_received, "pages,", tuples_received, "tuples, (fragmented:", fragmented_once, ")");
    println("ingress done");
}
