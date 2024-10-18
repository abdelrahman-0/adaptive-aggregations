#include <span>
#include <thread>

#include "common/alignment.h"
#include "core/network/connection.h"
#include "core/network/network_manager.h"
#include "core/network/page_communication.h"
#include "core/storage/page_local.h"
#include "core/storage/table.h"
#include "defaults.h"
#include "misc/exceptions/exceptions_misc.h"
#include "system/stopwatch.h"
#include "system/topology.h"
#include "ubench/common_flags.h"
#include "ubench/debug.h"
#include "ubench/heterogeneous_thread_group.h"
#include "utils/hash.h"
#include "utils/utils.h"

using namespace std::chrono_literals;

/* ----------- CMD LINE PARAMS ----------- */

DEFINE_uint32(nthreads, 1, "number of network threads to use");
DEFINE_uint32(qthreads, 1, "number of query-processing threads to use");

/* ----------- SCHEMA ----------- */

#define SCHEMA u64, u32, u32, std::array<char, 4>

using TablePage = PageLocal<SCHEMA>;
using ResultTuple = std::tuple<SCHEMA>;
using ResultPage = PageLocal<ResultTuple>;

/* ----------- NETWORK ----------- */

using NetworkPage = PageCommunication<defaults::network_page_size, ResultTuple>;
using IngressManager = HeterogeneousIngressNetworkManager<NetworkPage>;
using EgressManager = ConcurrentBufferedEgressNetworkManager<NetworkPage>;
using ThreadGroup = ubench::HeterogeneousThreadGroup<EgressManager, IngressManager, NetworkPage>;

/* ----------- FUNCTIONS ----------- */

// balanced qthread-to-nthread mapping
std::tuple<u16, u16> find_dedicated_nthread(std::integral auto qthread_id)
{
    u16 qthreads_per_nthread = FLAGS_qthreads / FLAGS_nthreads;
    auto num_fat_nthreads = FLAGS_qthreads % FLAGS_nthreads;
    auto num_fat_qthreads = (qthreads_per_nthread + 1) * num_fat_nthreads;
    bool fat_nthread = qthread_id < num_fat_qthreads;
    qthreads_per_nthread += fat_nthread;
    auto dedicated_nthread = fat_nthread ? (qthread_id / qthreads_per_nthread)
                                         : num_fat_nthreads + ((qthread_id - num_fat_qthreads) / qthreads_per_nthread);
    return {dedicated_nthread, qthreads_per_nthread};
}

ALWAYS_INLINE void consume_ingress(IngressManager& manager_recv)
{
    auto* network_page = manager_recv.try_dequeue_page();
    while (network_page) {
        manager_recv.done_page(network_page);
        network_page = manager_recv.try_dequeue_page();
    }
}

/* ----------- MAIN ----------- */

int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto subnet = FLAGS_local ? defaults::LOCAL_subnet : defaults::AWS_subnet;
    auto host_base = FLAGS_local ? defaults::LOCAL_host_base : defaults::AWS_host_base;

    auto env_var = std::getenv("NODE_ID");
    u32 node_id = std::stoul(env_var ? env_var : "0");

    print("NODE", node_id, "of", FLAGS_nodes - 1, ":");
    print("--------------");

    if (FLAGS_nthreads > FLAGS_qthreads) {
        throw InvalidOptionError{"Number of query threads must not be less than number of network threads"};
    }

    /* ----------- DATA LOAD ----------- */

    Table table{FLAGS_random};
    if (FLAGS_random) {
        table.prepare_random_swips(FLAGS_npages / FLAGS_nodes);
    }
    else {
        // prepare local IO at node offset (adjusted for page boundaries)
        File file{FLAGS_path, FileMode::READ};
        auto offset_begin =
            (((file.get_total_size() / FLAGS_nodes) * node_id) / defaults::local_page_size) * defaults::local_page_size;
        auto offset_end = (((file.get_total_size() / FLAGS_nodes) * (node_id + 1)) / defaults::local_page_size) *
                          defaults::local_page_size;
        if (node_id == FLAGS_nodes - 1) {
            offset_end = file.get_total_size();
        }
        file.set_offset(offset_begin, offset_end);
        table.bind_file(std::move(file));
        table.prepare_file_swips();
        print("reading bytes:", offset_begin, "→", offset_end, (offset_end - offset_begin) / defaults::local_page_size,
              "pages");
    }

    auto& swips = table.get_swips();

    // prepare cache
    u32 num_pages_cache = FLAGS_random ? ((FLAGS_cache * swips.size()) / 100u) : FLAGS_npages;
    Cache<TablePage> cache{num_pages_cache};
    table.populate_cache(cache, num_pages_cache, FLAGS_sequential_io);

    /* ----------- THREAD SETUP ----------- */

    NodeTopology topology{static_cast<u16>(FLAGS_nthreads + FLAGS_qthreads)};
    topology.init();
    auto npeers = FLAGS_nodes - 1;

    // control atomics
    std::vector<CachelineAlignedAtomic<bool>> nthread_continue(FLAGS_nthreads);
    std::fill(nthread_continue.begin(), nthread_continue.end(), true);
    DEBUGGING(std::atomic<u64> pages_recv{0});

    // barriers
    ::pthread_barrier_t barrier_start{};
    ::pthread_barrier_t barrier_end{};
    ::pthread_barrier_init(&barrier_start, nullptr, FLAGS_qthreads + 1);
    ::pthread_barrier_init(&barrier_end, nullptr, FLAGS_nthreads + FLAGS_qthreads + 1);

    // networking
    std::vector<ThreadGroup> thread_grps(FLAGS_nthreads,
                                         ThreadGroup{npeers * FLAGS_bufs_per_peer * 10, FLAGS_maxalloc});
    ::pthread_barrier_t barrier_network{};
    ::pthread_barrier_init(&barrier_network, nullptr, FLAGS_nthreads + 1);
    std::vector<std::jthread> threads_network{};
    for (auto thread_id{0u}; thread_id < FLAGS_nthreads; ++thread_id) {
        threads_network.emplace_back([=, &topology, &thread_grps, &barrier_network, &barrier_end,
                                      &nthread_continue DEBUGGING(, &pages_recv)]() {
            if (FLAGS_pin) {
                topology.pin_thread(thread_id);
            }

            /* ----------- NETWORK I/O ----------- */

            // setup connections to each node, forming a logical clique topology
            // note that connections need to be setup in a particular order to avoid deadlocks!
            std::vector<int> socket_fds{};

            // accept from [0, node_id)
            if (node_id) {
                Connection conn{node_id, FLAGS_nthreads, thread_id, node_id};
                conn.setup_ingress();
                socket_fds = std::move(conn.socket_fds);
            }

            // connect to [node_id + 1, FLAGS_nodes)
            for (auto i{node_id + 1u}; i < FLAGS_nodes; ++i) {
                auto destination_ip = std::string{subnet} + std::to_string(host_base + (FLAGS_local ? 0 : i));
                Connection conn{node_id, FLAGS_nthreads, thread_id, destination_ip, 1};
                conn.setup_egress(i);
                socket_fds.emplace_back(conn.socket_fds[0]);
            }

            auto qthreads_per_nthread =
                (FLAGS_qthreads / FLAGS_nthreads) + (thread_id < (FLAGS_qthreads % FLAGS_nthreads));

            EgressManager manager_send{
                static_cast<u16>(npeers),         FLAGS_depthnw, npeers * FLAGS_bufs_per_peer * qthreads_per_nthread,
                static_cast<u16>(FLAGS_nthreads), FLAGS_sqpoll,  socket_fds};
            IngressManager manager_recv{static_cast<u16>(npeers), FLAGS_depthnw, FLAGS_sqpoll, socket_fds};

            auto* recv_alloc = &(thread_grps[thread_id].ingress_block_alloc);

            thread_grps[thread_id].egress_mgr = &manager_send;
            thread_grps[thread_id].ingress_mgr = &manager_recv;

            // barrier
            ::pthread_barrier_wait(&barrier_network);

            // network loop
            while (nthread_continue[thread_id].val) {
                manager_recv.try_drain_done();
                for (auto dst{0u}; dst < npeers; ++dst) {
                    manager_send.try_flush(dst);
                }
                if (manager_recv.pending_peers()) {
                    for (auto dst{0u}; dst < npeers; ++dst) {
                        manager_recv.try_post_recvs(dst);
                    }
                }
                manager_send.try_drain_done();
            }
            manager_send.wait_all();
            ::pthread_barrier_wait(&barrier_end);
            DEBUGGING(pages_recv += manager_recv.get_pages_recv());
        });
    }
    ::pthread_barrier_wait(&barrier_network);

    // query processing
    std::atomic<u32> current_swip{0};
    DEBUGGING(std::atomic<u64> tuples_processed{0});
    DEBUGGING(std::atomic<u64> tuples_sent{0});
    DEBUGGING(std::atomic<u64> tuples_received{0});

    std::vector<CachelineAlignedAtomic<u32>> qthreads_done(FLAGS_nthreads);
    std::vector<CachelineAlignedAtomic<u32>> added_last_page(FLAGS_nthreads);
    std::fill(qthreads_done.begin(), qthreads_done.end(), 0);

    std::vector<std::jthread> threads_query;
    for (auto thread_id{0u}; thread_id < FLAGS_qthreads; ++thread_id) {
        threads_query.emplace_back(
            [=, &topology, &current_swip, &swips, &table, &barrier_start, &barrier_end, &added_last_page,
             &qthreads_done, &nthread_continue,
             &thread_grps DEBUGGING(, &tuples_processed, &tuples_sent, &tuples_received, &pages_recv)]() {
                if (FLAGS_pin) {
                    topology.pin_thread(thread_id + FLAGS_nthreads);
                }

                /* -------- THREAD MAPPING -------- */

                auto [dedicated_network_thread, qthreads_per_nthread] = find_dedicated_nthread(thread_id);
                auto& manager_send = *thread_grps[dedicated_network_thread].egress_mgr;
                auto& manager_recv = *thread_grps[dedicated_network_thread].ingress_mgr;
                DEBUGGING(print("assigning qthread", thread_id, "to nthread", dedicated_network_thread));

                /* ----------- LOCAL I/O ----------- */

                // setup local uring manager
                IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
                if (not FLAGS_path.empty()) {
                    thread_io.register_files({table.get_file().get_file_descriptor()});
                }

                /* ------------ BUFFERS ------------ */

                // TODO use alloc
                std::vector<NetworkPage*> active_buffers(npeers);
                for (auto*& page_ptr : active_buffers) {
                    page_ptr = manager_send.get_new_page();
                }
                std::vector<TablePage> local_buffers(defaults::local_io_depth);

                /* ------------ LAMBDAS ------------ */

                auto process_local_page = [node_id, &manager_send, &active_buffers](const TablePage& page) {
                    for (auto j{0u}; j < page.num_tuples; ++j) {
                        // hash tuple
                        auto tup = page.get_tuple<0, 1, 2, 3>(j);
                        auto dst = hash_key(std::get<0>(tup)) % FLAGS_nodes;
                        if (dst != node_id) {
                            auto actual_dst = dst - (dst > node_id);
                            auto* dst_page = active_buffers[actual_dst];
                            if (dst_page->full()) {
                                manager_send.enqueue_page(actual_dst, dst_page);
                                dst_page = manager_send.get_new_page();
                                active_buffers[actual_dst] = dst_page;
                            }
                            dst_page->emplace_back<0, 1, 2, 3>(tup);
                        }
                    }
                };

                // barrier
                ::pthread_barrier_wait(&barrier_start);

                /* ----------- BEGIN ----------- */

                // morsel loop
                u32 morsel_begin, morsel_end;
                while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < swips.size()) {
                    morsel_end = std::min(morsel_begin + FLAGS_morselsz, static_cast<u32>(swips.size()));

                    if (manager_recv.pending_peers() or manager_recv.pending_pages()) {
                        consume_ingress(manager_recv);
                    }

                    // partition swips such that unswizzled swips are at the beginning of the morsel
                    auto swizzled_idx = std::stable_partition(swips.data() + morsel_begin, swips.data() + morsel_end,
                                                              [](const Swip& swip) { return !swip.is_pointer(); }) -
                                        swips.data();

                    // submit io requests before processing in-memory pages to overlap I/O with computation
                    if (swizzled_idx > morsel_begin) {
                        thread_io.batch_async_io<READ>(
                            table.segment_id, std::span{swips.begin() + morsel_begin, swips.begin() + swizzled_idx},
                            local_buffers, true);
                    }

                    TablePage* page_to_process;
                    while (swizzled_idx < morsel_end) {
                        page_to_process = swips[swizzled_idx++].get_pointer<decltype(page_to_process)>();
                        process_local_page(*page_to_process);
                    }
                    while (thread_io.has_inflight_requests()) {
                        page_to_process = thread_io.get_next_page<decltype(page_to_process)>();
                        process_local_page(*page_to_process);
                    }
                }

                // flush all
                bool last_thread{false};
                if (qthreads_done[dedicated_network_thread].val++ == qthreads_per_nthread - 1) {
                    last_thread = true;
                    // wait for other qthreads to add their active pages
                    while (added_last_page[dedicated_network_thread].val != qthreads_per_nthread - 1)
                        ;
                    for (auto dst{0u}; dst < npeers; ++dst) {
                        manager_send.enqueue_page<true>(dst, active_buffers[dst]);
                        manager_send.finished_egress();
                    }
                }
                else {
                    for (auto dst{0u}; dst < npeers; ++dst) {
                        manager_send.enqueue_page(dst, active_buffers[dst]);
                    }
                    added_last_page[dedicated_network_thread].val++;
                }

                // wait for ingress
                while (manager_recv.pending_peers() or manager_recv.pending_pages()) {
                    consume_ingress(manager_recv);
                }

                if (last_thread) {
                    nthread_continue[dedicated_network_thread] = false;
                }
                // barrier
                ::pthread_barrier_wait(&barrier_end);

                /* ----------- END ----------- */
            });
    }

    Stopwatch swatch{};
    ::pthread_barrier_wait(&barrier_start);
    swatch.start();
    ::pthread_barrier_wait(&barrier_end);
    swatch.stop();

    ::pthread_barrier_destroy(&barrier_network);
    ::pthread_barrier_destroy(&barrier_start);
    ::pthread_barrier_destroy(&barrier_end);

    DEBUGGING(print("tuples received:", tuples_received.load()));
    DEBUGGING(print("tuples sent:", tuples_sent.load()));
    DEBUGGING(print("tuples processed:", tuples_processed.load()));
    DEBUGGING(u64 pages_local =
                  (tuples_processed + ResultPage::max_tuples_per_page - 1) / ResultPage::max_tuples_per_page);
    DEBUGGING(u64 local_sz = pages_local * defaults::local_page_size);
    DEBUGGING(u64 recv_sz = pages_recv * defaults::network_page_size);

    Logger{FLAGS_print_header}
        .log("node id", node_id)
        .log("nodes", FLAGS_nodes)
        .log("traffic", "both"s)
        .log("implementation", "shuffle heterogeneous"s)
        .log("network threads", FLAGS_nthreads)
        .log("query threads", FLAGS_qthreads)
        .log("total pages", FLAGS_npages)
        .log("local page size", defaults::local_page_size)
        .log("network page size", defaults::network_page_size)
        .log("morsel size", FLAGS_morselsz)
        .log("pin", FLAGS_pin)
        .log("buffers per peer", FLAGS_bufs_per_peer)
        .log("cache (%)", FLAGS_cache)
        .log("time (ms)", swatch.time_ms)         //
        DEBUGGING(.log("recv pages", pages_recv)) //
        DEBUGGING(.log("tuple throughput (tuples/s)",
                       ((tuples_received + tuples_processed) * 1000) / swatch.time_ms))              //
        DEBUGGING(.log("local throughput (Gb/s)", (local_sz * 8 * 1000) / (1e9 * swatch.time_ms)))   //
        DEBUGGING(.log("network throughput (Gb/s)", (recv_sz * 8 * 1000) / (1e9 * swatch.time_ms))); //
}
