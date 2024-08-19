#include <gflags/gflags.h>
#include <span>
#include <thread>

#include "defaults.h"
#include "network/connection.h"
#include "network/network_manager.h"
#include "network/page_communication.h"
#include "storage/chunked_list.h"
#include "storage/page_local.h"
#include "storage/policy.h"
#include "storage/table.h"
#include "utils/hash.h"
#include "utils/stopwatch.h"
#include "utils/utils.h"

/* ----------- SCHEMA ----------- */

#define SCHEMA int64_t, int64_t, int32_t, std::array<unsigned char, 4>

using TablePage = PageLocal<SCHEMA>;
using ResultTuple = std::tuple<SCHEMA>;
using NetworkPage = PageCommunication<ResultTuple>;
using ResultPage = PageLocal<ResultTuple>;

/* ----------- CMD LINE PARAMS ----------- */

DEFINE_uint32(threads, 1, "number of threads to use");
DEFINE_uint32(depthio, 256, "submission queue size of storage uring");
DEFINE_uint32(depthnw, 256, "submission queue size of network uring");
DEFINE_uint32(nodes, 2, "total number of num_nodes to use");
DEFINE_uint32(buffersnw, 20, "number of communication buffer pages to use per traffic direction");
DEFINE_bool(sqpoll, false, "whether to use kernel-sided submission queue polling");
DEFINE_uint32(morselsz, 50, "number of pages to process in one morsel");
DEFINE_string(path, "data/random.tbl", "path to input relation");
DEFINE_uint32(cache, 10, "percentage of table to cache in-memory in [0,100]");
DEFINE_bool(random, false, "randomize order of cached swips");

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto env_var = std::getenv("NODE_ID");
    uint32_t node_id = std::stoul(env_var ? env_var : "0");

    /* ----------- DATA LOAD ----------- */

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
    Table table{std::move(file)};
    auto& swips = table.get_swips();

    // prepare cache
    IO_Manager io{64, false};
    auto num_pages_cache = (FLAGS_cache * swips.size()) / 100u;
    Cache<TablePage> cache{num_pages_cache};
    table.populate_cache(cache, io, num_pages_cache, FLAGS_random);

    println("NODE", node_id, "of", FLAGS_nodes - 1, ":");
    println("---------");
    println("reading bytes:", offset_begin, "→", offset_end);

    /* ----------- THREAD SETUP ----------- */

    // control atomics
    std::atomic<bool> wait{true};
    std::atomic<uint32_t> current_swip{0};
    std::atomic<uint64_t> tuples_processed{0};
    std::atomic<uint64_t> tuples_sent{0};
    std::atomic<uint64_t> tuples_received{0};

    // create threads
    std::vector<std::thread> threads{};
    for (auto thread_id{0u}; thread_id < FLAGS_threads; ++thread_id) {
        threads.emplace_back([thread_id, node_id, &wait, &current_swip, &swips, &table, &tuples_processed, &tuples_sent,
                              &tuples_received]() {
            /* ----------- NETWORK I/O ----------- */

            // setup connections to each node, forming a logical clique topology
            // note that connections need to be setup in a particular order to avoid deadlocks!
            std::vector<int> socket_fds{};

            // accept from [0, node_id)
            if (node_id) {
                Connection conn{node_id, FLAGS_threads, thread_id, node_id};
                conn.setup_ingress();
                socket_fds = std::move(conn.socket_fds);
            }

            // connect to [node_id + 1, FLAGS_nodes)
            for (auto i{node_id + 1u}; i < FLAGS_nodes; ++i) {
                auto destination_ip = std::string{defaults::subnet} + std::to_string(defaults::receiver_host_base + 0);
                Connection conn{node_id, FLAGS_threads, thread_id, destination_ip, 1};
                conn.setup_egress(i);
                socket_fds.emplace_back(conn.socket_fds[0]);
            }

            auto npeers = FLAGS_nodes - 1;
            IngressNetworkManager<NetworkPage> manager_recv{npeers, FLAGS_depthnw, FLAGS_buffersnw, FLAGS_sqpoll,
                                                            socket_fds};
            EgressNetworkManager<NetworkPage> manager_send{npeers, FLAGS_depthnw, FLAGS_buffersnw, FLAGS_sqpoll,
                                                           socket_fds};
            uint32_t peers_done = 0;

            /* ----------- LOCAL I/O ----------- */

            // setup local uring manager
            IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
            thread_io.register_files({table.get_file().get_file_descriptor()});

            /* ----------- BUFFERS ----------- */

            std::vector<TablePage> local_buffers(defaults::local_io_depth);
            PageChunkedList<ResultPage> chunked_list_result{};
            auto* current_result_page = chunked_list_result.get_current_page();

            /* ----------- LAMBDAS ----------- */

            uint64_t local_tuples_processed{0};
            uint64_t local_tuples_sent{0};
            uint64_t local_tuples_received{0};
            auto process_network_page = [&current_result_page, &chunked_list_result,
                                         &local_tuples_received](const NetworkPage& page) {
                auto page_num_tuples = page.get_num_tuples();
                for (auto i{0u}; i < page_num_tuples; ++i) {
                    if (current_result_page->full()) {
                        current_result_page = chunked_list_result.get_new_page();
                    }
                    current_result_page->emplace_back(i, page);
                }
                local_tuples_received += page_num_tuples;
                return page.is_last_page();
            };

            auto process_local_page = [node_id, &current_result_page, &chunked_list_result, &manager_send,
                                       &local_tuples_processed, &local_tuples_sent](const TablePage& page) {
                for (auto j = 0u; j < page.num_tuples; ++j) {
                    // hash tuple
                    auto dst = murmur_hash(std::get<0>(page.columns)[j]) % FLAGS_nodes;

                    // send or materialize into thread-local buffers
                    if (dst == node_id) {
                        if (current_result_page->full()) {
                            current_result_page = chunked_list_result.get_new_page();
                        }
                        current_result_page->emplace_back_transposed(j, page);
                        local_tuples_processed++;
                    } else {
                        auto actual_dst = dst - (dst > node_id);
                        auto dst_page = manager_send.get_page(actual_dst);
                        dst_page->emplace_back_transposed(j, page);
                        local_tuples_sent++;
                    }
                }
            };

            auto consume_ingress = [npeers, &peers_done, &manager_recv, &process_network_page]() {
                if (peers_done == npeers) {
                    return;
                }
                auto [network_page, peer] = manager_recv.get_page();
                while (network_page) {
                    bool last_page = process_network_page(*network_page);
                    peers_done += last_page;
                    if (!last_page) {
                        // still more pages
                        manager_recv.post_recvs(peer);
                    }
                    manager_recv.done_page(network_page);
                    std::tie(network_page, peer) = manager_recv.get_page();
                }
            };

            for (auto peer{0u}; peer < npeers; ++peer) {
                manager_recv.post_recvs(peer);
            }

            // spin barrier
            while (wait)
                ;

            /* ----------- BEGIN ----------- */

            // morsel loop
            uint32_t morsel_begin, morsel_end;
            while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < swips.size()) {
                morsel_end = std::min(morsel_begin + FLAGS_morselsz, static_cast<uint32_t>(swips.size()));

                // handle ingress communication
                consume_ingress();

                // partition swips such that unswizzled swips are at the beginning of the morsel
                auto swizzled_idx = std::stable_partition(swips.data() + morsel_begin, swips.data() + morsel_end,
                                                          [](const Swip& swip) { return !swip.is_pointer(); }) -
                                    swips.data();

                // submit io requests before processing in-memory pages to overlap I/O with computation
                thread_io.batch_async_io<READ>(table.segment_id,
                                               std::span{swips.begin() + morsel_begin, swips.begin() + swizzled_idx},
                                               local_buffers, true);

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

//            println("finished all morsels, got", local_tuples_received, "tuples during morsel work");

            manager_send.flush_all();
            while (peers_done < npeers) {
                consume_ingress();
            }
            manager_send.wait_all();
            tuples_sent += local_tuples_sent;
            tuples_processed += local_tuples_processed;
            tuples_received += local_tuples_received;

            /* ----------- END ----------- */
        });
    }

    Stopwatch swatch{};
    swatch.start();
    wait = false;
    for (auto& t : threads) {
        t.join();
    }
    swatch.stop();

    println("tuples received:", tuples_received.load());
    println("tuples sent:", tuples_sent.load());
    println("tuples processed:", tuples_processed.load());

    Logger logger{};
    logger.log("traffic", "ingress"s);
    logger.log("implementation", "shuffle_homogeneous"s);
    logger.log("threads", FLAGS_threads);
    logger.log("page size", defaults::network_page_size);
    logger.log("nodes", FLAGS_nodes);
    logger.log("morsel_size", FLAGS_morselsz);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("bandwidth (Gb/s)",
               ((tuples_received + tuples_processed) * sizeof(ResultTuple) * 8 * 1000) / (1e9 * swatch.time_ms));
}
