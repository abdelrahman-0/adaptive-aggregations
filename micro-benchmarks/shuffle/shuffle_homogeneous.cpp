#include <span>
#include <thread>

#include "core/network/connection.h"
#include "core/network/network_manager.h"
#include "core/network/page_communication.h"
#include "core/storage/page_local.h"
#include "core/storage/table.h"
#include "defaults.h"
#include "system/stopwatch.h"
#include "system/topology.h"
#include "ubench/common_flags.h"
#include "ubench/debug.h"
#include "utils/hash.h"
#include "utils/utils.h"

using namespace std::chrono_literals;

/* ----------- CMD LINE PARAMS ----------- */

DEFINE_uint32(threads, 1, "number of threads to use");

/* ----------- SCHEMA ----------- */

#define SCHEMA u64, u32, u32, std::array<char, 4>

using TablePage = PageLocal<SCHEMA>;
using ResultTuple = std::tuple<SCHEMA>;
using ResultPage = PageLocal<ResultTuple>;

/* ----------- NETWORK ----------- */

using NetworkPage = PageCommunication<defaults::network_page_size, ResultTuple>;
using IngressManager = SimpleIngressNetworkManager<NetworkPage>;
using EgressManager = BufferedEgressNetworkManager<NetworkPage>;

/* ----------- FUNCTIONS ----------- */

[[nodiscard]]
u32 consume_ingress(IngressManager& manager_recv)
{
    u32 peers_done{0};
    auto [network_page, peer] = manager_recv.get_page();
    while (network_page) {
        bool last_page = network_page->is_last_page();
        peers_done += last_page;
        manager_recv.done_page(network_page);
        if (!last_page) {
            // still more pages
            manager_recv.post_recvs(peer);
        }
        std::tie(network_page, peer) = manager_recv.get_page();
    }
    return peers_done;
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

    /* ----------- DATA LOAD ----------- */

    bool random_table = FLAGS_random;
    Table table{random_table};
    if (random_table) {
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
    u32 num_pages_cache = random_table ? ((FLAGS_cache * swips.size()) / 100u) : FLAGS_npages;
    Cache<TablePage> cache{num_pages_cache};
    table.populate_cache(cache, num_pages_cache, FLAGS_sequential_io);

    /* ----------- THREAD SETUP ----------- */

    // control atomics
    NodeTopology topology{static_cast<u16>(FLAGS_threads)};
    topology.init();
    ::pthread_barrier_t barrier_start{};
    ::pthread_barrier_t barrier_end{};
    ::pthread_barrier_init(&barrier_start, nullptr, FLAGS_threads + 1);
    ::pthread_barrier_init(&barrier_end, nullptr, FLAGS_threads + 1);
    std::atomic<u32> current_swip{0};
    std::atomic<u64> tuples_processed{0};
    std::atomic<u64> tuples_sent{0};
    std::atomic<u64> tuples_received{0};
    std::atomic<u64> pages_recv{0};

    // create threads
    std::vector<std::thread> threads{};
    for (auto thread_id{0u}; thread_id < FLAGS_threads; ++thread_id) {
        threads.emplace_back([=, &topology, &current_swip, &swips, &table, &tuples_processed, &tuples_sent,
                              &tuples_received, &pages_recv, &barrier_start, &barrier_end]() {
            if (FLAGS_pin) {
                topology.pin_thread(thread_id);
            }

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
            IngressManager manager_recv{npeers, FLAGS_depthnw, npeers, FLAGS_sqpoll, socket_fds};
            EgressManager manager_send{npeers, FLAGS_depthnw, npeers * FLAGS_bufs_per_peer, FLAGS_sqpoll, socket_fds};
            u32 peers_done = 0;

            /* ----------- LOCAL I/O ----------- */

            // setup local uring manager
            IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
            if (not FLAGS_path.empty()) {
                thread_io.register_files({table.get_file().get_file_descriptor()});
            }

            /* ----------- BUFFERS ----------- */

            std::vector<TablePage> local_buffers(defaults::local_io_depth);

            for (auto peer{0u}; peer < npeers; ++peer) {
                manager_recv.post_recvs(peer);
            }

            /* ------------ LAMBDAS ------------ */

            auto process_local_page = [node_id, &manager_send](const TablePage& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    // hash tuple
                    auto tup = page.get_tuple<0, 1, 2, 3>(j);
                    auto dst = hash_key(std::get<0>(tup)) % FLAGS_nodes;
                    if (dst == node_id) {
                    }
                    else {
                        auto actual_dst = dst - (dst > node_id);
                        auto dst_page = manager_send.get_page(actual_dst);
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

                // handle ingress communication
                if (peers_done < npeers) {
                    peers_done += consume_ingress(manager_recv);
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
                    process_local_page(*page_to_process);
                }
                while (thread_io.has_inflight_requests()) {
                    page_to_process = thread_io.get_next_page<decltype(page_to_process)>();
                    process_local_page(*page_to_process);
                }
            }

            manager_send.flush_all();
            while (peers_done < npeers) {
                peers_done += consume_ingress(manager_recv);
                manager_send.try_drain_pending();
            }
            manager_send.wait_all();

            // barrier
            ::pthread_barrier_wait(&barrier_end);

            pages_recv += manager_recv.get_pages_recv();

            /* ----------- END ----------- */
        });
    }

    Stopwatch swatch{};

    ::pthread_barrier_wait(&barrier_start);
    swatch.start();
    ::pthread_barrier_wait(&barrier_end);
    swatch.stop();

    for (auto& t : threads) {
        t.join();
    }

    ::pthread_barrier_destroy(&barrier_start);
    ::pthread_barrier_destroy(&barrier_end);

    // clang-format off
    DEBUGGING(
        print("tuples received:", tuples_received.load());
        print("tuples sent:", tuples_sent.load());
        print("tuples processed:", tuples_processed.load());
        u64 pages_local = (tuples_processed + ResultPage::max_tuples_per_page - 1) / ResultPage::max_tuples_per_page;
        u64 local_sz = pages_local * defaults::local_page_size; u64 recv_sz = pages_recv * defaults::network_page_size;
    )

    Logger{FLAGS_print_header}
        .log("node id", node_id)
        .log("nodes", FLAGS_nodes)
        .log("traffic", "both"s)
        .log("implementation", "shuffle homogeneous"s)
        .log("threads", FLAGS_threads)
        .log("total pages", FLAGS_npages)
        .log("local page size", defaults::local_page_size)
        .log("network page size", defaults::network_page_size)
        .log("morsel size", FLAGS_morselsz)
        .log("pin", FLAGS_pin)
        .log("buffers per peer", FLAGS_bufs_per_peer)
        .log("cache (%)", FLAGS_cache)
        .log("time (ms)", swatch.time_ms)
        DEBUGGING(
        .log("tuple throughput (tuples/s)", ((tuples_received + tuples_processed) * 1000) / swatch.time_ms)
        .log("local throughput (Gb/s)", (local_sz * 8 * 1000) / (1e9 * swatch.time_ms))
        .log("network throughput (Gb/s)", (recv_sz * 8 * 1000) / (1e9 * swatch.time_ms))
        );
}
