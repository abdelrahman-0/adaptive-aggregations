#include <cassert>
#include <gflags/gflags.h>
#include <liburing.h>
#include <tbb/enumerable_thread_specific.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>
#include <tbb/task_arena.h>

#include "core/page.h"
#include "network/connection.h"
#include "network/network_manager_old.h"
#include "network/pointer_ring.h"
#include "performance/stopwatch.h"
#include "storage/chunked_list.h"

DEFINE_int32(ingress, 1, "number of sender nodes to accept requests from");

#define SCHEMA int64_t, int64_t, int32_t, std::array<unsigned char, 4>

using ResultTuple = std::tuple<SCHEMA>;
using ResultPage = PageLocal<ResultTuple>;
using NetworkPage = PageNetwork<ResultTuple>;

// auto g = tbb::global_control(tbb::global_control::max_allowed_parallelism, 1);

static auto num_threads = 10u;

static constexpr auto pages_per_thread = 10u;
static constexpr auto pointer_ring_size = next_power_2(512u);

struct TLS {
    NetworkManagerOld network;
    PageChunkedList<ResultPage> result{};

    explicit TLS(const Connection& conn) : network(conn) {}
};

std::atomic<unsigned> total_num_pages_received{0u};
std::atomic<unsigned> total_num_tuples_received{0u};
std::atomic<unsigned> total_num_bytes_received{0u};

// TODO output stats in CSV format
int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    // setup connection
    print("Num threads:", num_threads);
    Connection conn_ingress{FLAGS_ingress};
    conn_ingress.setup_ingress();

    print("=========");

    // prepare waiting threads
    std::atomic<bool> done{false};

    assert(pointer_ring_size >= num_threads * pages_per_thread);
    PointerRing<pointer_ring_size> pr{num_threads};

    std::vector<std::thread> threads;
    for (auto i = 0u; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            // thread-local buffer
            std::array<NetworkPage, pages_per_thread> buffer{};
            uint64_t thread_id = i;

            // set memory to 0
            ::memset(buffer.data(), 0, pages_per_thread * sizeof(NetworkPage));

            // add buffers to queue
            for (auto& page : buffer) {
                auto tagged_ptr = uintptr_t(&page);
                tagged_ptr |= (thread_id << 48);
                pr.insert(tagged_ptr);
            }

            // process pages
            auto num_tuples = 0u;
            auto num_pages = 0u;
            auto current_page = 0u;
            while (num_pages <= pr.thread_pages[thread_id]) {
                while (num_pages == pr.thread_pages[thread_id]) {
                    // TODO investigate HERE
                    if (done) {
                        goto done_goto;
                    }
                }
                auto tagged_ptr = uintptr_t(buffer.data() + current_page);
                tagged_ptr |= (thread_id << 48);
                num_tuples += buffer[current_page].num_tuples;
                buffer[current_page].num_tuples = 0;
                num_pages++;
                pr.insert(tagged_ptr);
                current_page = (current_page + 1) % pages_per_thread;
                //                print(total_num_pages_received + num_pages);
            }
        done_goto:;
            total_num_pages_received += num_pages;
            total_num_tuples_received += num_tuples;
        });
    }

    // prepare io_uring
    io_uring ring{};
    io_uring_queue_init(64u, &ring, 0);
    // use local fd
    int ret;
    if ((ret = io_uring_register_files(&ring, conn_ingress.socket_fds.data(), conn_ingress.num_connections)) < 0) {
        throw IOUringRegisterFilesError{ret};
    }

    // listen loop
    {
        Stopwatch _{};
        while (true) {
            auto sqe = io_uring_get_sqe(&ring);
            uintptr_t tagged_ptr = pr.get_next();
            uint64_t idx = tagged_ptr >> 48;
            auto* page = reinterpret_cast<NetworkPage*>(tagged_ptr & (((uint64_t)(-1)) >> 16));
            io_uring_prep_recv(sqe, 0, page, defaults::network_page_size, MSG_WAITALL);
            sqe->flags |= IOSQE_FIXED_FILE;
            auto num_submitted = io_uring_submit_and_wait(&ring, 1);
            assert(num_submitted == 1);
            io_uring_cqe* cqe{nullptr};
            io_uring_peek_cqe(&ring, &cqe);
            ret = io_uring_peek_cqe(&ring, &cqe);
            if (cqe->res <= 0 || ret != 0 || cqe == nullptr) {
                done = true;
                break;
            }
            auto received_bytes = cqe->res;
            io_uring_cqe_seen(&ring, cqe);
            while (received_bytes != defaults::network_page_size) {
                print("FRAGMENTATION:", received_bytes, "/", defaults::network_page_size,
                      defaults::network_page_size - received_bytes, "remaining | ", std::this_thread::get_id());
                sqe = io_uring_get_sqe(&ring);
                io_uring_prep_recv(sqe, 0, reinterpret_cast<std::byte*>(page) + received_bytes,
                                   defaults::network_page_size - received_bytes, MSG_WAITALL);
                sqe->flags |= IOSQE_FIXED_FILE;
                num_submitted = io_uring_submit_and_wait(&ring, 1);
                assert(num_submitted == 1);
                cqe = nullptr;
                ret = io_uring_peek_cqe(&ring, &cqe);
                print(cqe->res);
                io_uring_cqe_seen(&ring, cqe);
                if (cqe == nullptr || cqe->res <= 0 || ret != 0) {
                    break;
                }
                received_bytes += cqe->res;
            }
            pr.thread_pages[idx]++;
            //            total_num_tuples_received += reinterpret_cast<NetworkPage*>(page)->num_tuples;
            total_num_bytes_received += received_bytes;
            //            total_num_pages_received++;
        }
    done_listening:
        auto i = 0;
        for (auto& t : threads) {
            t.join();
        }
    }

    print("Pages received:", total_num_pages_received);
    print("Tuples received:", total_num_tuples_received);
    print("Bytes received:", total_num_bytes_received);
}
