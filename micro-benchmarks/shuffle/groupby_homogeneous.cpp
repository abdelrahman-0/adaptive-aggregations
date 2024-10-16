#include <gflags/gflags.h>
#include <span>
#include <thread>

#include "core/buffer/tuple_buffer.h"
#include "core/hashtable/hashtable.h"
#include "core/network/connection.h"
#include "core/network/network_manager.h"
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
DEFINE_uint32(partitions, 16, "number of hashtable partitions to use");
DEFINE_uint32(slots, 512, "number of slots to use per partition");

/* ----------- SCHEMA ----------- */

#define KEYS_AGG u64
#define KEYS_IDX 0
#define KEYS_GRP u64
#define SCHEMA KEYS_GRP, u32, u32, std::array<char, 4>

using TablePage = PageLocal<SCHEMA>;

/* ----------- GROUP BY ----------- */

using GroupAttributes = std::tuple<KEYS_GRP>;
using AggregateAttributes = std::tuple<KEYS_AGG>;
auto aggregate = [](AggregateAttributes& aggs_grp, const AggregateAttributes& aggs_tup) {
    std::get<0>(aggs_grp) += std::get<0>(aggs_tup);
};
using HashTablePreAgg = hashtable::PartitionedSaltedHashtable<GroupAttributes, AggregateAttributes, aggregate, void*>;
using BufferPage = HashTablePreAgg::PageAgg;

/* ----------- NETWORK ----------- */

using IngressManager = IngressNetworkManager<BufferPage>;
using EgressManager = EgressNetworkManager<BufferPage>;

/* ----------- MAIN ----------- */

int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    FLAGS_partitions = next_power_2(FLAGS_partitions);
    FLAGS_slots = next_power_2(FLAGS_slots);

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
        DEBUGGING(print("reading bytes:", offset_begin, "→", offset_end,
                        (offset_end - offset_begin) / defaults::local_page_size, "pages"));
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
    DEBUGGING(std::atomic<u64> tuples_processed{0});
    DEBUGGING(std::atomic<u64> tuples_sent{0});
    DEBUGGING(std::atomic<u64> tuples_received{0});
    DEBUGGING(std::atomic<u64> pages_recv{0});

    // create threads
    std::vector<std::jthread> threads{};
    for (auto thread_id{0u}; thread_id < FLAGS_threads; ++thread_id) {
        threads.emplace_back([=, &topology, &current_swip, &swips, &table, &barrier_start,
                              &barrier_end DEBUGGING(, &tuples_processed, &tuples_sent, &tuples_received,
                                                     &pages_recv)]() {
            if (FLAGS_pin) {
                topology.pin_thread(thread_id);
            }

            /* ----------- CONNECTION ----------- */

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

            /* ----------- BUFFERS ----------- */

            TupleBuffer<BufferPage> tuple_buffer;
            std::vector<TablePage> local_buffers(defaults::local_io_depth);
            u64 local_tuples_processed{0};
            u64 local_tuples_sent{0};
            u64 local_tuples_received{0};

            /* ----------- NETWORK I/O ----------- */

            auto npeers = FLAGS_nodes - 1;
            auto ingress_consumer_fn = [&tuple_buffer](BufferPage* pg) { tuple_buffer.add_page(pg); };
            IngressManager manager_recv{npeers, FLAGS_depthnw, 0, FLAGS_sqpoll, socket_fds, ingress_consumer_fn};
            EgressManager manager_send{npeers, FLAGS_depthnw, 0, FLAGS_sqpoll, socket_fds};
            u32 peers_done = 0;

            /* ----------- LOCAL I/O ----------- */

            // setup local uring manager
            IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
            if (not FLAGS_random and not FLAGS_path.empty()) {
                thread_io.register_files({table.get_file().get_file_descriptor()});
            }

            /* ------------ GROUP BY ------------ */

            u32 part_offset{0};
            std::vector<HashTablePreAgg::ConsumerFn> consumer_fns{};
            for (u32 part{0}; part < FLAGS_partitions; ++part) {
                u16 dst = (part * FLAGS_nodes) / FLAGS_partitions;
                auto parts_per_dst = (FLAGS_partitions / FLAGS_nodes) + (dst < (FLAGS_partitions % FLAGS_nodes));
                bool final_dst_partition = ((part - part_offset + 1) % parts_per_dst) == 0;
                part_offset += final_dst_partition ? parts_per_dst : 0;
                if (dst == node_id) {
                    consumer_fns.emplace_back([&tuple_buffer](BufferPage* pg, bool) {
                        if (not pg->empty()) {
                            pg->retire();
                            tuple_buffer.add_page(pg);
                        }
                    });
                }
                else {
                    auto actual_dst = dst - (dst > node_id);
                    consumer_fns.emplace_back(
                        [&manager_send, actual_dst, final_dst_partition](BufferPage* pg, bool is_last = false) {
                            if (not pg->empty() or final_dst_partition) {
                                pg->retire();
                                if (is_last and final_dst_partition) {
                                    pg->set_last_page();
                                }
                                manager_send.try_flush(actual_dst, pg);
                            }
                        });
                }
            }
            HashTablePreAgg ht{FLAGS_partitions, FLAGS_slots, consumer_fns};

            /* ------------ LAMBDAS ------------ */

            auto& ht_alloc = ht.get_alloc();
            manager_send.register_page_consumer_fn([&ht_alloc](BufferPage* pg) { ht_alloc.return_page(pg); });

            auto process_local_page = [&ht DEBUGGING(, &local_tuples_processed)](const TablePage& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    auto group = page.get_tuple<KEYS_IDX>(j);
                    auto agg = std::make_tuple<u64>(1);
                    ht.aggregate(group, agg);
                }
                DEBUGGING(local_tuples_processed += page.num_tuples);
            };

            auto consume_ingress = [&manager_recv]() { return manager_recv.consume_pages(); };

            // barrier
            ::pthread_barrier_wait(&barrier_start);

            /* ----------- BEGIN ----------- */

            // morsel loop
            u32 morsel_begin, morsel_end;
            while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < swips.size()) {
                morsel_end = std::min(morsel_begin + FLAGS_morselsz, static_cast<u32>(swips.size()));

                // handle communication
                manager_send.try_drain_pending();
                if (peers_done < npeers) {
                    peers_done += consume_ingress();
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
            ht.finalize();
            while (peers_done < npeers) {
                peers_done += consume_ingress();
                manager_send.try_drain_pending();
            }
            manager_send.wait_all();

            // barrier
            ::pthread_barrier_wait(&barrier_end);

            /* ----------- END ----------- */

            DEBUGGING(tuples_sent += local_tuples_sent);
            DEBUGGING(tuples_processed += local_tuples_processed);
            DEBUGGING(tuples_received += local_tuples_received);
            DEBUGGING(pages_recv += manager_recv.get_pages_recv());
        });
    }

    Stopwatch swatch{};

    ::pthread_barrier_wait(&barrier_start);
    swatch.start();
    ::pthread_barrier_wait(&barrier_end);
    swatch.stop();

    ::pthread_barrier_destroy(&barrier_start);
    ::pthread_barrier_destroy(&barrier_end);

    DEBUGGING(print("tuples received:", tuples_received.load())); //
    DEBUGGING(print("tuples sent:", tuples_sent.load()));         //
    DEBUGGING(u64 pages_local =
                  (tuples_processed + TablePage::max_tuples_per_page - 1) / TablePage::max_tuples_per_page); //
    DEBUGGING(u64 local_sz = pages_local * defaults::local_page_size);                                       //
    DEBUGGING(u64 recv_sz = pages_recv * defaults::network_page_size);                                       //

    Logger{FLAGS_print_header}
        .log("node id", node_id)
        .log("nodes", FLAGS_nodes)
        .log("traffic", "both"s)
        .log("implementation", "groupby homogeneous"s)
        .log("threads", FLAGS_threads)
        .log("partitions", FLAGS_partitions)
        .log("slots", FLAGS_slots)
        .log("groups", FLAGS_groups)
        .log("total pages", FLAGS_npages)
        .log("local page size", defaults::local_page_size)
        .log("tuples per local page", TablePage::max_tuples_per_page)
        .log("hashtable page size", defaults::hashtable_page_size)
        .log("tuples per network page", BufferPage::max_tuples_per_page)
        .log("morsel size", FLAGS_morselsz)
        .log("pin", FLAGS_pin)
        .log("cache (%)", FLAGS_cache)
        .log("time (ms)", swatch.time_ms)                                                            //
        DEBUGGING(.log("local tuples processed", tuples_processed))                                  //
        DEBUGGING(.log("pages received", pages_recv))                                                //
        DEBUGGING(.log("local throughput (Gb/s)", (local_sz * 8 * 1000) / (1e9 * swatch.time_ms)))   //
        DEBUGGING(.log("network throughput (Gb/s)", (recv_sz * 8 * 1000) / (1e9 * swatch.time_ms))); //
}
