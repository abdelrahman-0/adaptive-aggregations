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
#include "utils/stopwatch.h"
#include "utils/utils.h"

DEFINE_uint32(egress, 1, "number of egress nodes");
DEFINE_uint32(threads, 1, "number of threads to use");
DEFINE_uint32(depth, 256, "number of io_uring entries for network I/O");
DEFINE_uint32(pages, 10'000, "number of pages to send via egress traffic");

using NetworkPage = PageCommunication<int64_t>;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    Connection conn{FLAGS_egress};
    conn.setup_egress();
    Logger logger{};

    std::atomic<uint64_t> pages_sent{0};
    std::atomic<uint64_t> tuples_sent{0};
    std::atomic<uint64_t> actual_pages_sent{0};
    {
        Stopwatch _{logger};
        std::vector<std::thread> threads{};
        std::atomic<uint32_t> finished_threads{0};
        for (auto i = 0u; i < FLAGS_threads; ++i) {
            threads.emplace_back([&, i]() {
                io_uring ring{};

                int ret;
                if ((ret = io_uring_queue_init(FLAGS_depth, &ring, 0))) {
                    throw IOUringInitError{ret};
                }

                if ((ret = io_uring_register_files(&ring, conn.socket_fds.data(), conn.num_connections)) < 0) {
                    throw IOUringRegisterFileError{ret};
                }

                NetworkPage page{};
                page.num_tuples = NetworkPage::max_num_tuples_per_page;
                while (pages_sent.fetch_add(1) < FLAGS_pages) {
                    page.fill_random();
                    auto* sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_send(sqe, 0, &page, defaults::network_page_size, 0);
                    sqe->flags |= IOSQE_FIXED_FILE;
                    ret = io_uring_submit_and_wait(&ring, 1);
                    assert(ret == 1);
                    io_uring_cqe* cqe;
                    io_uring_peek_cqe(&ring, &cqe);
                    if (cqe->res <= 0) {
                        throw NetworkSendError{cqe->res};
                    }
                    assert(cqe->res <= defaults::network_page_size);
                    auto bytes_sent = cqe->res;
                    io_uring_cqe_seen(&ring, cqe);
                    while (bytes_sent != defaults::network_page_size) {
                        println("fragmented");
                        sqe = io_uring_get_sqe(&ring);
                        io_uring_prep_send(sqe, 0, reinterpret_cast<std::byte*>(&page) + bytes_sent,
                                           defaults::network_page_size - bytes_sent, 0);
                        sqe->flags |= IOSQE_FIXED_FILE;
                        ret = io_uring_submit_and_wait(&ring, 1);
                        assert(ret == 1);
                        io_uring_cqe* cqe;
                        io_uring_peek_cqe(&ring, &cqe);
                        if (cqe->res <= 0) {
                            throw NetworkSendError{cqe->res};
                        }
                        bytes_sent += cqe->res;
                        io_uring_cqe_seen(&ring, cqe);
                    }
                    // TODO finish sending page if cqe->res < defaults::network_page_size
                    actual_pages_sent++;
                    tuples_sent += page.num_tuples;
                }

                if (finished_threads.fetch_add(1) == FLAGS_threads - 1) {
                    // final threads sends an empty page
                    println("thread", i, "sending empty page");
                    page.clear();
                    auto* sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_send(sqe, 0, &page, defaults::network_page_size, 0);
                    sqe->flags |= IOSQE_FIXED_FILE;
                    ret = io_uring_submit_and_wait(&ring, 1);
                    assert(ret == 1);
                    io_uring_cqe* cqe;
                    io_uring_peek_cqe(&ring, &cqe);
                    if (cqe->res <= 0) {
                        throw NetworkSendError{cqe->res};
                    }
                    assert(cqe->res > 0);
                    io_uring_cqe_seen(&ring, cqe);
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }
    }

    logger.log("threads", FLAGS_threads);
    logger.log("pages", actual_pages_sent);
    logger.log("tuples", tuples_sent);

    println("egress done");
}
