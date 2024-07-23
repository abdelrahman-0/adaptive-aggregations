#include <cassert>
#include <gflags/gflags.h>
#include <liburing.h>
#include <tbb/enumerable_thread_specific.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>
#include <tbb/task_arena.h>

#include "network/connection.h"
#include "network/network_manager.h"
#include "storage/chunked_list.h"
#include "storage/page.h"
#include "utils/stopwatch.h"

DEFINE_int32(ingress, 1, "number of sender nodes to accept requests from");

#define SCHEMA int64_t, int64_t, int32_t, std::array<unsigned char, 4>

using ResultTuple = std::tuple<SCHEMA>;
using ResultPage = PageLocal<ResultTuple>;
using NetworkPage = PageCommunication<ResultTuple>;

// auto g = tbb::global_control(tbb::global_control::max_allowed_parallelism, 1);
static auto num_threads = 1u;
// static auto num_threads = std::thread::hardware_concurrency();

struct TLS {
    NetworkManager network;
    ChunkedList<ResultPage> result{};

    explicit TLS(const Connection& conn) : network(conn) {}
};

std::atomic<unsigned> total_num_pages_received{0u};
std::atomic<unsigned> total_num_tuples_received{0u};
std::atomic<unsigned> total_num_bytes_received{0u};

// TODO output stats in CSV format
int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    //     setup connection
    Connection conn_ingress{FLAGS_ingress};
    conn_ingress.setup_ingress();
    println("Num threads:", num_threads);
    std::vector<std::thread> threads;
    {
        Stopwatch _{};
        for (auto i = 0u; i < num_threads; ++i) {
            threads.emplace_back([&]() {
                io_uring ring{};
                io_uring_queue_init(256, &ring, 0);
                int ret;
                if ((ret = io_uring_register_files(&ring, conn_ingress.socket_fds.data(),
                                                   conn_ingress.num_connections)) < 0) {
                    throw IOUringRegisterFileError{ret};
                }
                assert(ret == 0);
                NetworkPage page{};
                unsigned num_pages_received{0u};
                unsigned num_tuples_received{0u};
                unsigned num_bytes_received{0u};
                while (true) {
                    page.clear();
                    auto sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_recv(sqe, 0, &page, defaults::network_page_size, MSG_WAITALL);
                    sqe->flags |= IOSQE_FIXED_FILE;
                    auto num_submitted = io_uring_submit(&ring);
                    assert(num_submitted == 1);
                    io_uring_cqe* cqe{nullptr};
                    ret = io_uring_wait_cqe(&ring, &cqe);
                    if (cqe == nullptr || cqe->res <= 0 || ret != 0) {
                        break;
                    }
                    auto received_bytes = cqe->res;
                    if (received_bytes != defaults::network_page_size) {
                        println("oops: received", received_bytes);
                    }
                    //                    while (received_bytes != defaults::network_page_size) {
                    //                        println("fragmented size:", received_bytes, "on",
                    //                        std::this_thread::get_id()); io_uring_prep_recv(sqe, 0,
                    //                        reinterpret_cast<std::byte*>(&page) + received_bytes,
                    //                                           defaults::network_page_size, MSG_WAITALL);
                    //                        sqe->flags |= IOSQE_FIXED_FILE;
                    //                        num_submitted = io_uring_submit_and_wait(&ring, 1);
                    //                        assert(num_submitted == 1);
                    //                        cqe = nullptr;
                    //                        ret = io_uring_peek_cqe(&ring, &cqe);
                    //                        if (cqe == nullptr || cqe->res <= 0 || ret != 0) {
                    //                            break;
                    //                        }
                    //                        received_bytes += cqe->res;
                    //                        io_uring_cqe_seen(&ring, cqe);
                    //                    }
                    num_bytes_received += received_bytes;
                    num_pages_received++;
                    if (page.num_tuples > 682) {
                        println("Unexpected", page.num_tuples);
                        //                        page.print_info();
                    }
                    num_tuples_received += page.num_tuples;

                    //                    io_uring_cq_advance(&ring, 1);
                    io_uring_cqe_seen(&ring, cqe);
                }
                total_num_pages_received += num_pages_received;
                total_num_tuples_received += num_tuples_received;
                total_num_bytes_received += num_bytes_received;
            });
        }
        for (auto& t : threads) {
            t.join();
        }
    }
    println("Pages received:", total_num_pages_received);
    println("Tuples received:", total_num_tuples_received);
    println("Bytes received:", total_num_bytes_received);

    // multi-threaded?

    //    io_uring ring{};
    //    io_uring_queue_init(256, &ring, 0);
    //    // use local fd
    //    auto ret = io_uring_register_files(&ring, &new_fd, 1);
    //    if (ret) {
    //        fprintf(stderr, "register files: %s\n", strerror(-ret));
    //        return -1;
    //    }
    // register buffers
    //    constexpr auto num_buffers{128u};
    //    auto* buf_ring = io_uring_setup_buf_ring(&ring, num_buffers, 0, 0, &ret);
    //    std::array<NetworkPage, num_buffers> buffers{};
    //    for (auto i{0u}; i < num_buffers; i++) {
    //        io_uring_buf_ring_add(buf_ring, buffers.data() + i, defaults::network_page_size, i,
    //                              io_uring_buf_ring_mask(num_buffers), i);
    //    }
    //    io_uring_buf_ring_advance(buf_ring, num_buffers);

    //    auto sqe = io_uring_get_sqe(&ring);
    //    io_uring_prep_recv_multishot(sqe, 0, buffers.data(), 0, 0);
    //    sqe->flags |= IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT;
    //    sqe->buf_group = 0;
    //    std::array<io_uring_cqe*, 256> cqes{};
    //    auto i = 0u;
    //    {
    //        Stopwatch _{};
    //        while (true) {
    //            auto sqe = io_uring_get_sqe(&ring);
    //            io_uring_prep_recv(sqe, 0, &page, defaults::network_page_size, 0);
    //            sqe->flags |= IOSQE_FIXED_FILE;
    //            auto num_submitted = io_uring_submit(&ring);
    //            assert(num_submitted == 1);
    //            io_uring_cqe* cqe;
    //            ret = io_uring_wait_cqe(&ring, &cqe);
    //            if (cqe->res <= 0) {
    //                break;
    //            }
    //            io_uring_cq_advance(&ring, 1);
    //            //            auto count = io_uring_peek_batch_cqe(&ring, cqes.data(), 256u);
    //            //            for (i = 0; i < count; i++) {
    //            //                auto cqe = cqes[i];
    //            //                if (cqe->res < 0) {
    //            //                    fprintf(stderr, "CQE error: %s\n", strerror(-cqe->res));
    //            //                    break;
    //            //                }
    //            //                auto idx = cqe->flags >> 16;
    //            //                io_uring_buf_ring_add(buf_ring, buffers.data() + idx, defaults::network_page_size,
    //            idx,
    //            //                                      io_uring_buf_ring_mask(num_buffers), 0);
    //            //                io_uring_buf_ring_advance(buf_ring, 1);
    //            //                if (!(cqe->flags & IORING_CQE_F_MORE)) {
    //            //                    return 0;
    //            //                }
    //            //                io_uring_cq_advance(&ring, 1);
    //            //            }
    //        }
    //    }
}
