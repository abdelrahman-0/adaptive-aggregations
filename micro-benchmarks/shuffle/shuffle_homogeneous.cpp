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

using namespace std::chrono_literals;

/* ----------- SCHEMA ----------- */

#define SCHEMA u64, u32, u32, std::array<char, 4>

using TablePage = PageLocal<SCHEMA>;
using ResultTuple = std::tuple<SCHEMA>;
using NetworkPage = PageCommunication<ResultTuple>;
using ResultPage = PageLocal<ResultTuple>;

/* ----------- CMD LINE PARAMS ----------- */

DEFINE_bool(local, true, "run benchmark using loop-back interface");
DEFINE_uint32(threads, 1, "number of threads to use");
DEFINE_uint32(depthio, 256, "submission queue size of storage uring");
DEFINE_uint32(depthnw, 256, "submission queue size of network uring");
DEFINE_uint32(nodes, 2, "total number of num_nodes to use");
DEFINE_bool(sqpoll, false, "whether to use kernel-sided submission queue polling");
DEFINE_uint32(morselsz, 100, "number of pages to process in one morsel");
DEFINE_string(path, "data/random.tbl", "path to input relation");
DEFINE_uint32(cache, 100, "percentage of table to cache in-memory in range [0,100]");
DEFINE_bool(random, false, "randomize order of cached swips");

/* ----------- FUNCTIONS ----------- */

[[nodiscard]] u32 consume_ingress(IngressNetworkManager<NetworkPage>& manager_recv, ResultPage*& current_result_page,
                                  PageChunkedList<ResultPage>& chunked_list_result, u64& local_tuples_received) {
    u32 peers_done{0};
    auto [network_page, peer] = manager_recv.get_page();
    while (network_page) {
        //        for (auto i{0u}; i < network_page->get_num_tuples(); ++i) {
        //            if (current_result_page->full()) {
        //                current_result_page = chunked_list_result.get_new_page();
        //            }
        //            current_result_page->emplace_back(i, *network_page);
        //        }
        bool last_page = network_page->is_last_page();
        peers_done += last_page;
        local_tuples_received += network_page->get_num_tuples();
        manager_recv.done_page(network_page);
        if (!last_page) {
            // still more pages
            manager_recv.post_recvs(peer);
        }
        std::tie(network_page, peer) = manager_recv.get_page();
    }
    return peers_done;
}

void process_local_page(u32 node_id, ResultPage*& current_result_page, PageChunkedList<ResultPage>& chunked_list_result,
                        EgressNetworkManager<NetworkPage>& manager_send, u64& local_tuples_processed,
                        u64& local_tuples_sent, const TablePage& page) {
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
            auto dst_page = manager_send.get_page(actual_dst);
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
    u32 num_pages_cache = (FLAGS_cache * swips.size()) / 100u;
    Cache<TablePage> cache{num_pages_cache};
    table.populate_cache(cache, num_pages_cache, FLAGS_random);
    println("reading bytes:", offset_begin, "→", offset_end, (offset_end - offset_begin) / defaults::local_page_size,
            "pages");

    /* ----------- THREAD SETUP ----------- */

    // control atomics
    std::atomic<bool> wait{true};
    std::atomic<u32> threads_ready{0};
    std::atomic<u32> current_swip{0};
    std::atomic<u64> tuples_processed{0};
    std::atomic<u64> tuples_sent{0};
    std::atomic<u64> tuples_received{0};

    // create threads
    std::vector<std::thread> threads{};
    for (auto thread_id{0u}; thread_id < FLAGS_threads; ++thread_id) {
        threads.emplace_back([thread_id, node_id, subnet, host_base, &wait, &threads_ready, &current_swip, &swips,
                              &table, &tuples_processed, &tuples_sent, &tuples_received]() {
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
                auto destination_ip = std::string{subnet} + std::to_string(host_base + (FLAGS_local ? 0 : i));
                Connection conn{node_id, FLAGS_threads, thread_id, destination_ip, 1};
                conn.setup_egress(i);
                socket_fds.emplace_back(conn.socket_fds[0]);
            }

            auto npeers = FLAGS_nodes - 1;
            IngressNetworkManager<NetworkPage> manager_recv{npeers, FLAGS_depthnw, npeers, FLAGS_sqpoll, socket_fds};
            EgressNetworkManager<NetworkPage> manager_send{npeers, FLAGS_depthnw, npeers, FLAGS_sqpoll, socket_fds};
            u32 peers_done = 0;

            /* ----------- LOCAL I/O ----------- */

            // setup local uring manager
            IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
            thread_io.register_files({table.get_file().get_file_descriptor()});

            /* ----------- BUFFERS ----------- */

            std::vector<TablePage> local_buffers(defaults::local_io_depth);
            PageChunkedList<ResultPage> chunked_list_result{};
            auto* current_result_page = chunked_list_result.get_current_page();
            u64 local_tuples_processed{0};
            u64 local_tuples_sent{0};
            u64 local_tuples_received{0};

            for (auto peer{0u}; peer < npeers; ++peer) {
                manager_recv.post_recvs(peer);
            }

            threads_ready++;

            // spin barrier
            while (wait)
                ;

            /* ----------- BEGIN ----------- */

            // morsel loop
            u32 morsel_begin, morsel_end;
            while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < swips.size()) {
                morsel_end = std::min(morsel_begin + FLAGS_morselsz, static_cast<u32>(swips.size()));

                // handle ingress communication
                if (peers_done < npeers) {
                    peers_done +=
                        consume_ingress(manager_recv, current_result_page, chunked_list_result, local_tuples_received);
                }

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
                    process_local_page(node_id, current_result_page, chunked_list_result, manager_send,
                                       local_tuples_processed, local_tuples_sent, *page_to_process);
                }
                while (thread_io.has_inflight_requests()) {
                    page_to_process = thread_io.get_next_page<decltype(page_to_process)>();
                    process_local_page(node_id, current_result_page, chunked_list_result, manager_send,
                                       local_tuples_processed, local_tuples_sent, *page_to_process);
                }
            }

            manager_send.flush_all();
            while (peers_done < npeers) {
                peers_done +=
                    consume_ingress(manager_recv, current_result_page, chunked_list_result, local_tuples_received);
            }
            manager_send.wait_all();

            tuples_sent += local_tuples_sent;
            tuples_processed += local_tuples_processed;
            tuples_received += local_tuples_received;

            /* ----------- END ----------- */
        });
    }

    while (threads_ready != FLAGS_threads)
        ;

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
    logger.log("node id", node_id);
    logger.log("nodes", FLAGS_nodes);
    logger.log("traffic", "both"s);
    logger.log("implementation", "shuffle_homogeneous"s);
    logger.log("threads", FLAGS_threads);
    logger.log("page size", defaults::network_page_size);
    logger.log("morsel size", FLAGS_morselsz);
    logger.log("cache (%)", FLAGS_cache);
    logger.log("time (ms)", swatch.time_ms);
    logger.log("throughput (tuples/s)", ((tuples_received + tuples_processed) * 1000) / swatch.time_ms);
    logger.log("node throughput (Gb/s)", (table.get_file().get_size() * 8 * 1000) / (1e9 * swatch.time_ms));
    logger.log("total throughput (Gb/s)", (table.get_file().get_total_size() * 8 * 1000) / (1e9 * swatch.time_ms));
}
