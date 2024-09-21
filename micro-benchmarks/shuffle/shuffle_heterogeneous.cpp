#include <gflags/gflags.h>
#include <span>
#include <thread>

#include "defaults.h"
#include "network/connection.h"
#include "network/network_manager.h"
#include "network/page_communication.h"
#include "storage/chunked_list.h"
#include "storage/page_local.h"
#include "storage/table.h"
#include "system/stopwatch.h"
#include "system/topology.h"
#include "utils/hash.h"
#include "utils/utils.h"

using namespace std::chrono_literals;

/* ----------- SCHEMA ----------- */

#define SCHEMA u64, u32, u32, std::array<char, 4>

using TablePage = PageLocal<SCHEMA>;
using ResultTuple = std::tuple<SCHEMA>;
using ResultPage = PageLocal<ResultTuple>;

/* ----------- NETWORK ----------- */

using NetworkPage = PageCommunication<ResultTuple>;
using IngressManager = ConcurrentIngressNetworkManager<NetworkPage>;
using EgressManager = ConcurrentBufferedEgressNetworkManager<NetworkPage>;

/* ----------- CMD LINE PARAMS ----------- */

DEFINE_bool(local, true, "run benchmark using loop-back interface");
DEFINE_uint32(nthreads, 2, "number of network threads to use");
DEFINE_uint32(qthreads, 3, "number of query-processing threads to use");
DEFINE_uint32(depthio, 256, "submission queue size of storage uring");
DEFINE_uint32(depthnw, 256, "submission queue size of network uring");
DEFINE_uint32(nodes, 2, "total number of num_nodes to use");
DEFINE_bool(sqpoll, false, "whether to use kernel-sided submission queue polling");
DEFINE_uint32(morselsz, 10, "number of pages to process in one morsel");
DEFINE_string(path, "data/random.tbl",
              "path to input relation (if empty random pages will be generated instead, see "
              "flag 'npages')");
DEFINE_uint32(npages, 1'000, "number of random pages to generate (only applicable if 'random' flag is set)");
DEFINE_uint32(bufs_per_peer, 2, "number of egress buffers to use per peer");
DEFINE_uint32(cache, 100, "percentage of table to cache in-memory in range [0,100] (ignored if 'random' flag is set)");
DEFINE_bool(sequential_io, true, "whether to use sequential or random I/O for cached swips");
DEFINE_bool(random, true, "whether to use randomly generated data instead of reading in a file");
DEFINE_bool(pin, false, "pin threads using balanced affinity at core granularity");
DEFINE_bool(print_header, true, "whether to print metrics header");

/* ----------- FUNCTIONS ----------- */

// balanced qthread-to-nthread mapping
std::tuple<u16, u16> find_dedicated_nthread(std::integral auto qthread_id) {
    u16 qthreads_per_nthread = FLAGS_qthreads / FLAGS_nthreads;
    auto num_fat_nthreads = FLAGS_qthreads % FLAGS_nthreads;
    auto num_fat_qthreads = (qthreads_per_nthread + 1) * num_fat_nthreads;
    bool fat_nthread = qthread_id < num_fat_qthreads;
    qthreads_per_nthread += fat_nthread;
    auto dedicated_nthread = fat_nthread ? (qthread_id / qthreads_per_nthread)
                                         : num_fat_nthreads + ((qthread_id - num_fat_qthreads) / qthreads_per_nthread);
    return {dedicated_nthread, qthreads_per_nthread};
}

void consume_ingress(IngressManager& manager_recv, ResultPage*& current_result_page,
                     PageChunkedList<ResultPage>& chunked_list_result, u64& local_tuples_received) {
    auto* network_page = manager_recv.try_dequeue_page();
    while (network_page) {
        //        for (auto i{0u}; i < network_page->get_num_tuples(); ++i) {
        //            if (current_result_page->full()) {
        //                current_result_page = chunked_list_result.get_new_page();
        //            }
        //            current_result_page->emplace_back(i, *network_page);
        //        }
        local_tuples_received += network_page->get_num_tuples();
        manager_recv.done_page(network_page);
        network_page = manager_recv.try_dequeue_page();
    }
}

void process_local_page(u32 node_id, ResultPage*& current_result_page, PageChunkedList<ResultPage>& chunked_list_result,
                        EgressManager& manager_send, std::vector<NetworkPage*>& active_buffers,
                        u64& local_tuples_processed, u64& local_tuples_sent, const TablePage& page) {
    u64 page_local_tuples_processed{0};
    u64 page_local_tuples_sent{0};
    for (auto j{0u}; j < page.num_tuples; ++j) {
        // hash tuple
        auto dst = std::hash<u64>{}(std::get<0>(page.columns)[j]) % FLAGS_nodes;

        // send or materialize into thread-local buffers
        if (dst == node_id) {
            //            if (current_result_page->full()) {
            //                current_result_page = chunked_list_result.get_new_page();
            //            }
            //            current_result_page->emplace_back_transposed(j, page);
            page_local_tuples_processed++;
        } else {
            auto actual_dst = dst - (dst > node_id);
            auto* dst_page = active_buffers[actual_dst];
            if (dst_page->full()) {
                manager_send.enqueue_page(actual_dst, dst_page);
                dst_page = manager_send.get_new_page();
                active_buffers[actual_dst] = dst_page;
            }
            if (dst_page->num_tuples > dst_page->max_num_tuples_per_page) {
                throw std::runtime_error{"egress page has too many tuples"};
            }
            dst_page->emplace_back_transposed(j, page);
            page_local_tuples_sent++;
        }
    }
    local_tuples_processed += page_local_tuples_processed;
    local_tuples_sent += page_local_tuples_sent;
}

/* ----------- MAIN ----------- */

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto subnet = FLAGS_local ? defaults::LOCAL_subnet : defaults::AWS_subnet;
    auto host_base = FLAGS_local ? defaults::LOCAL_host_base : defaults::AWS_host_base;

    auto env_var = std::getenv("NODE_ID");
    u32 node_id = std::stoul(env_var ? env_var : "0");

    println("NODE", node_id, "of", FLAGS_nodes - 1, ":");
    println("--------------");

    if (FLAGS_nthreads > FLAGS_qthreads) {
        println("invalid combination of options!");
        std::exit(0);
    }

    /* ----------- DATA LOAD ----------- */

    bool random_table = FLAGS_random;
    Table table{random_table};
    if (random_table) {
        table.prepare_random_swips(FLAGS_npages / FLAGS_nodes);
    } else {
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
        println("reading bytes:", offset_begin, "→", offset_end,
                (offset_end - offset_begin) / defaults::local_page_size, "pages");
    }

    auto& swips = table.get_swips();

    // prepare cache
    u32 num_pages_cache = random_table ? ((FLAGS_cache * swips.size()) / 100u) : FLAGS_npages;
    Cache<TablePage> cache{num_pages_cache};
    table.populate_cache(cache, num_pages_cache, FLAGS_sequential_io);

    /* ----------- THREAD SETUP ----------- */

    NodeTopology topology{static_cast<u16>(FLAGS_nthreads + FLAGS_qthreads)};
    topology.init();
    auto npeers = FLAGS_nodes - 1;

    // control atomics
    std::atomic<bool> nthread_continue{true};
    std::atomic<u64> pages_recv{0};

    // networking
    std::vector<EgressManager*> egress_managers(FLAGS_nthreads);
    std::vector<IngressManager*> ingress_managers(FLAGS_nthreads);
    ::pthread_barrier_t barrier_network{};
    ::pthread_barrier_init(&barrier_network, nullptr, FLAGS_nthreads + 1);
    std::vector<std::thread> threads_network{};
    for (auto thread_id{0u}; thread_id < FLAGS_nthreads; ++thread_id) {
        threads_network.emplace_back([=, &topology, &egress_managers, &ingress_managers, &barrier_network,
                                      &nthread_continue, &pages_recv]() {
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
            egress_managers[thread_id] = &manager_send;

            IngressManager manager_recv{static_cast<u16>(npeers), FLAGS_depthnw,
                                        npeers * FLAGS_bufs_per_peer * qthreads_per_nthread, FLAGS_sqpoll, socket_fds};
            ingress_managers[thread_id] = &manager_recv;

            for (auto dst{0u}; dst < npeers; ++dst) {
                manager_recv.post_recvs(dst);
            }

            // barrier
            ::pthread_barrier_wait(&barrier_network);

            // network loop
            while (nthread_continue) {
                manager_recv.try_drain_done();
                for (auto dst{0u}; dst < npeers; ++dst) {
                    manager_send.try_flush(dst);
                }
                for (auto dst{0u}; dst < npeers; ++dst) {
                    manager_recv.post_recvs(dst);
                }
                manager_send.try_drain_done();
            }
            manager_send.wait_all();
            // TODO barrier
            pages_recv += manager_recv.get_pages_recv();
        });
    }
    ::pthread_barrier_wait(&barrier_network);

    // query processing
    ::pthread_barrier_t barrier_start{};
    ::pthread_barrier_t barrier_end{};
    ::pthread_barrier_init(&barrier_start, nullptr, FLAGS_qthreads + 1);
    ::pthread_barrier_init(&barrier_end, nullptr, FLAGS_qthreads + 1);
    std::atomic<u64> tuples_processed{0};
    std::atomic<u64> tuples_sent{0};
    std::atomic<u64> tuples_received{0};

    // control atomics
    std::atomic<u32> current_swip{0};
    std::vector<std::atomic<u32>> qthreads_done(FLAGS_nthreads);
    std::vector<std::atomic<u32>> added_last_page(FLAGS_nthreads);
    for (auto i{0u}; i < FLAGS_nthreads; ++i) {
        qthreads_done[i] = 0;
    }

    std::vector<std::thread> threads_query;
    for (auto thread_id{0u}; thread_id < FLAGS_qthreads; ++thread_id) {
        threads_query.emplace_back([=, &topology, &current_swip, &swips, &table, &tuples_processed, &tuples_sent,
                                    &tuples_received, &pages_recv, &barrier_start, &barrier_end, &added_last_page,
                                    &egress_managers, &qthreads_done]() {
            if (FLAGS_pin) {
                topology.pin_thread(thread_id + FLAGS_nthreads);
            }

            /* -------- THREAD MAPPING -------- */

            auto [dedicated_network_thread, qthreads_per_nthread] = find_dedicated_nthread(thread_id);
            auto& manager_send = *egress_managers[dedicated_network_thread];
            auto& manager_recv = *ingress_managers[dedicated_network_thread];

            {
                std::unique_lock _{print_mtx};
                println("assigning qthread", thread_id, "to nthread", dedicated_network_thread);
            }

            /* ----------- LOCAL I/O ----------- */

            // setup local uring manager
            IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
            if (not FLAGS_path.empty()) {
                thread_io.register_files({table.get_file().get_file_descriptor()});
            }

            /* ------------ BUFFERS ------------ */

            std::vector<NetworkPage*> active_buffers(npeers);
            for (auto*& page_ptr : active_buffers) {
                page_ptr = manager_send.get_new_page();
            }
            std::vector<TablePage> local_buffers(defaults::local_io_depth);
            PageChunkedList<ResultPage> chunked_list_result{};
            auto* current_result_page = chunked_list_result.get_current_page();
            u64 local_tuples_processed{0};
            u64 local_tuples_sent{0};
            u64 local_tuples_received{0};

            // barrier
            ::pthread_barrier_wait(&barrier_start);

            /* ----------- BEGIN ----------- */

            // morsel loop
            u32 morsel_begin, morsel_end;
            while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < swips.size()) {
                morsel_end = std::min(morsel_begin + FLAGS_morselsz, static_cast<u32>(swips.size()));

                if (manager_recv.not_done()) {
                    consume_ingress(manager_recv, current_result_page, chunked_list_result, local_tuples_received);
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
                    process_local_page(node_id, current_result_page, chunked_list_result, manager_send, active_buffers,
                                       local_tuples_processed, local_tuples_sent, *page_to_process);
                }
                while (thread_io.has_inflight_requests()) {
                    page_to_process = thread_io.get_next_page<decltype(page_to_process)>();
                    process_local_page(node_id, current_result_page, chunked_list_result, manager_send, active_buffers,
                                       local_tuples_processed, local_tuples_sent, *page_to_process);
                }
            }

            // flush all
            if (qthreads_done[dedicated_network_thread]++ == qthreads_per_nthread - 1) {
                // wait for other qthreads to add their active pages
                while (added_last_page[dedicated_network_thread] != qthreads_per_nthread - 1)
                    ;
                for (auto dst{0u}; dst < npeers; ++dst) {
                    auto* page = active_buffers[dst];
                    page->set_last_page();
                    manager_send.enqueue_page(dst, page);
                    manager_send.finished_egress();
                }
            } else {
                for (auto dst{0u}; dst < npeers; ++dst) {
                    manager_send.enqueue_page(dst, active_buffers[dst]);
                }
                added_last_page[dedicated_network_thread]++;
            }

            // wait for ingress
            while (manager_recv.not_done()) {
                consume_ingress(manager_recv, current_result_page, chunked_list_result, local_tuples_received);
            }

            // barrier
            ::pthread_barrier_wait(&barrier_end);

            tuples_sent += local_tuples_sent;
            tuples_processed += local_tuples_processed;
            tuples_received += local_tuples_received;

            /* ----------- END ----------- */
        });
    }

    Stopwatch swatch{};

    ::pthread_barrier_wait(&barrier_start);
    swatch.start();
    ::pthread_barrier_wait(&barrier_end);
    nthread_continue = false;
    swatch.stop();

    for (auto& t : threads_query) {
        t.join();
    }
    for (auto& t : threads_network) {
        t.join();
    }

    ::pthread_barrier_destroy(&barrier_network);
    ::pthread_barrier_destroy(&barrier_start);
    ::pthread_barrier_destroy(&barrier_end);

    println("tuples received:", tuples_received.load());
    println("tuples sent:", tuples_sent.load());
    println("tuples processed:", tuples_processed.load());

    u64 pages_local =
        (tuples_processed + ResultPage::max_num_tuples_per_page - 1) / ResultPage::max_num_tuples_per_page;
    u64 local_sz = pages_local * defaults::local_page_size;
    u64 recv_sz = pages_recv * defaults::network_page_size;

    Logger logger{FLAGS_print_header};
    logger.log("node id", node_id);
    logger.log("nodes", FLAGS_nodes);
    logger.log("traffic", "both"s);
    logger.log("implementation", "shuffle_homogeneous"s);
    logger.log("network threads", FLAGS_nthreads);
    logger.log("query threads", FLAGS_qthreads);
    logger.log("page size", defaults::network_page_size);
    logger.log("morsel size", FLAGS_morselsz);
    logger.log("pin", FLAGS_pin);
    logger.log("buffers per peer", FLAGS_bufs_per_peer);
    logger.log("cache (%)", FLAGS_cache);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("tuple throughput (tuples/s)", ((tuples_received + tuples_processed) * 1000) / swatch.time_ms);
    logger.log("local throughput (Gb/s)", (local_sz * 8 * 1000) / (1e9 * swatch.time_ms));
    logger.log("network throughput (Gb/s)", (recv_sz * 8 * 1000) / (1e9 * swatch.time_ms));
}
