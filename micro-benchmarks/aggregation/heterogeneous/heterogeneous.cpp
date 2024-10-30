#include <span>
#include <thread>

#include "bench/bench.h"
#include "bench/common_flags.h"
#include "bench/heterogeneous_thread_group.h"
#include "bench/stopwatch.h"
#include "common/alignment.h"
#include "core/buffer/page_buffer.h"
#include "core/hashtable/ht_local.h"
#include "core/network/connection.h"
#include "core/network/network_manager.h"
#include "core/network/page_communication.h"
#include "core/storage/page_local.h"
#include "core/storage/table.h"
#include "defaults.h"
#include "misc/exceptions/exceptions_misc.h"
#include "system/topology.h"
#include "utils/hash.h"
#include "utils/utils.h"

using namespace std::chrono_literals;

/* ----------- CMD LINE PARAMS ----------- */

DEFINE_uint32(nthreads, 1, "number of network threads to use");
DEFINE_uint32(qthreads, 1, "number of query-processing threads to use");
DEFINE_uint32(slots, 8192, "number of slots to use per partition");
DEFINE_uint32(bump, 1, "bumping factor to use when allocating memory for partition pages");

/* ----------- SCHEMA ----------- */

#define AGG_KEYS u64
#define GPR_KEYS_IDX 0
#define GRP_KEYS u64
#define SCHEMA GRP_KEYS, u32, u32, std::array<char, 4>

using PageTable = PageLocal<SCHEMA>;

/* ----------- GROUP BY ----------- */

using GroupAttributes = std::tuple<GRP_KEYS>;
using AggregateAttributes = std::tuple<AGG_KEYS>;
auto aggregate_fn = [](AggregateAttributes& aggs_grp, const AggregateAttributes& aggs_tup) {
    std::get<0>(aggs_grp) += std::get<0>(aggs_tup);
};
static constexpr bool is_salted = true;
using HashTablePreAgg = ht::PartitionedOpenAggregationHashtable<GroupAttributes, AggregateAttributes, aggregate_fn, void*, true, is_salted>;
using PageHashtable = HashTablePreAgg::page_t;

/* ----------- NETWORK ----------- */

using IngressManager = HeterogeneousIngressNetworkManager<PageHashtable>;
using EgressManager = HeterogeneousEgressNetworkManager<PageHashtable>;
using ThreadGroup = ubench::HeterogeneousThreadGroup<EgressManager, IngressManager, PageHashtable>;

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
    Cache<PageTable> cache{num_pages_cache};
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
    std::vector<ThreadGroup> thread_grps(FLAGS_nthreads, ThreadGroup{npeers * 10, FLAGS_maxalloc});
    ::pthread_barrier_t barrier_network{};
    ::pthread_barrier_init(&barrier_network, nullptr, FLAGS_nthreads + 1);
    std::vector<std::jthread> threads_network{};
    for (auto thread_id{0u}; thread_id < FLAGS_nthreads; ++thread_id) {
        threads_network.emplace_back(
            [=, &topology, &thread_grps, &barrier_network, &barrier_end, &nthread_continue DEBUGGING(, &pages_recv)]() {
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

                EgressManager manager_send{static_cast<u16>(npeers),
                                           FLAGS_depthnw,
                                           static_cast<u16>(FLAGS_nthreads),
                                           FLAGS_sqpoll,
                                           socket_fds,
                                           FLAGS_qthreads};
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
    DEBUGGING(std::atomic<u64> tuples_local{0});
    DEBUGGING(std::atomic<u64> tuples_sent{0});
    DEBUGGING(std::atomic<u64> tuples_received{0});

    std::vector<CachelineAlignedAtomic<u32>> qthreads_done(FLAGS_nthreads);
    std::vector<CachelineAlignedAtomic<u32>> added_last_page(FLAGS_nthreads);
    std::fill(qthreads_done.begin(), qthreads_done.end(), 0);

    std::vector<std::jthread> threads_query;
    for (u64 thread_id{0u}; thread_id < FLAGS_qthreads; ++thread_id) {
        threads_query.emplace_back(
            [=, &topology, &current_swip, &swips, &table, &barrier_start, &barrier_end, &added_last_page,
             &qthreads_done, &nthread_continue,
             &thread_grps DEBUGGING(, &tuples_local, &tuples_sent, &tuples_received, &pages_recv)]() {
                if (FLAGS_pin) {
                    topology.pin_thread(thread_id + FLAGS_nthreads);
                }

                /* -------- THREAD MAPPING -------- */

                auto [dedicated_network_thread, qthreads_per_nthread] = find_dedicated_nthread(thread_id);
                IngressManager& manager_recv = *thread_grps[dedicated_network_thread].ingress_mgr;
                EgressManager& manager_send = *thread_grps[dedicated_network_thread].egress_mgr;
                DEBUGGING(print("assigning qthread", thread_id, "to nthread", dedicated_network_thread));

                /* ----------- BUFFERS ----------- */

                PageBuffer<PageHashtable> tuple_buffer;
                std::vector<PageTable> local_buffers(defaults::local_io_depth);
                u64 local_tuples_processed{0};
                u64 local_tuples_sent{0};
                u64 local_tuples_received{0};

                /* ----------- LOCAL I/O ----------- */

                // setup local uring manager
                IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
                if (not FLAGS_path.empty()) {
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
                        consumer_fns.emplace_back([&tuple_buffer](PageHashtable* pg, bool) {
                            if (not pg->empty()) {
                                pg->retire();
                                tuple_buffer.add_page(pg);
                            }
                        });
                    }
                    else {
                        auto actual_dst = dst - (dst > node_id);
                        consumer_fns.emplace_back([&manager_send, actual_dst, final_dst_partition,
                                                   thread_id](PageHashtable* pg, bool is_last = false) {
                            if (not pg->empty() or final_dst_partition) {
                                pg->retire();
                                if (is_last and final_dst_partition) {
                                    pg->set_last_page();
                                }
                                manager_send.enqueue_page(actual_dst,
                                                          reinterpret_cast<PageHashtable*>(
                                                              (reinterpret_cast<uintptr_t>(pg) | (thread_id << 56))));
                            }
                        });
                    }
                }
                mem::BlockAllocator<PageHashtable, mem::MMapMemoryAllocator<true>, true> ht_alloc(
                    FLAGS_partitions * FLAGS_bump, FLAGS_maxalloc);
                HashTablePreAgg ht{static_cast<u32>(FLAGS_partitions), FLAGS_slots, consumer_fns, ht_alloc};

                /* ------------ LAMBDAS ------------ */
                manager_send.register_page_consumer_fn(
                    thread_id, [&ht_alloc, thread_id](PageHashtable* pg) { ht_alloc.return_page(pg); });

                auto process_local_page = [&ht DEBUGGING(, &local_tuples_processed)](const PageTable& page) {
                    for (auto j{0u}; j < page.num_tuples; ++j) {
                        auto group = page.get_tuple<GPR_KEYS_IDX>(j);
                        auto agg = std::make_tuple<AGG_KEYS>(1);
                        ht.aggregate_fn(group, agg);
                    }
                    DEBUGGING(local_tuples_processed += page.num_tuples);
                };

                auto consume_ingress = [&manager_recv, &tuple_buffer]() {
                    PageHashtable* page = manager_recv.try_dequeue_page();
                    if (page) {
                        tuple_buffer.add_page(page);
                    }
                };

                // barrier
                ::pthread_barrier_wait(&barrier_start);

                /* ----------- BEGIN ----------- */

                // morsel loop
                u32 morsel_begin, morsel_end;
                while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < swips.size()) {
                    morsel_end = std::min(morsel_begin + FLAGS_morselsz, static_cast<u32>(swips.size()));

                    if (manager_recv.pending()) {
                        consume_ingress();
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

                    PageTable* page_to_process;
                    while (swizzled_idx < morsel_end) {
                        page_to_process = swips[swizzled_idx++].get_pointer<decltype(page_to_process)>();
                        process_local_page(*page_to_process);
                    }
                    while (thread_io.has_inflight_requests()) {
                        page_to_process = thread_io.get_next_page<decltype(page_to_process)>();
                        process_local_page(*page_to_process);
                    }
                }

                bool last_thread{false};
                if (qthreads_done[dedicated_network_thread].val++ == qthreads_per_nthread - 1) {
                    last_thread = true;
                    // wait for other qthreads to add their active pages
                    while (added_last_page[dedicated_network_thread].val != qthreads_per_nthread - 1)
                        ;
                    ht.finalize(true);
                    manager_send.finished_egress();
                }
                else {
                    ht.finalize(false);
                    added_last_page[dedicated_network_thread].val++;
                }

                // wait for ingress
                while (manager_recv.pending()) {
                    consume_ingress();
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
    DEBUGGING(print("tuples processed:", tuples_local.load()));
    DEBUGGING(u64 pages_local = (tuples_local + PageHashtable::max_tuples_per_page - 1) / PageHashtable::max_tuples_per_page);
    DEBUGGING(u64 local_sz = pages_local * defaults::local_page_size);
    DEBUGGING(u64 recv_sz = pages_recv * defaults::network_page_size);
    DEBUGGING(u64 tuples_processed = tuples_received + tuples_local);

    Logger{FLAGS_print_header}
        .log("node id", node_id)
        .log("nodes", FLAGS_nodes)
        .log("traffic", "both"s)
        .log("implementation", "groupby heterogeneous"s)
        .log("hashtable", HashTablePreAgg::get_type())
        .log("network threads", FLAGS_nthreads)
        .log("query threads", FLAGS_qthreads)
        .log("partitions", FLAGS_partitions)
        .log("slots", FLAGS_slots)
        .log("groups", FLAGS_groups)
        .log("total pages", FLAGS_npages)
        .log("local page size", defaults::local_page_size)
        .log("tuples per local page", PageTable::max_tuples_per_page)
        .log("hashtable page size", defaults::hashtable_page_size)
        .log("tuples per hashtable page", PageHashtable::max_tuples_per_page)
        .log("morsel size", FLAGS_morselsz)
        .log("pin", FLAGS_pin)
        .log("cache (%)", FLAGS_cache)
        .log("time (ms)", swatch.time_ms)                                                            //
        DEBUGGING(.log("recv pages", pages_recv))                                                    //
        DEBUGGING(.log("tuple throughput (tuples/s)", (tuples_processed * 1000) / swatch.time_ms))   //
        DEBUGGING(.log("local throughput (Gb/s)", (local_sz * 8 * 1000) / (1e9 * swatch.time_ms)))   //
        DEBUGGING(.log("network throughput (Gb/s)", (recv_sz * 8 * 1000) / (1e9 * swatch.time_ms))); //
}
