#include "config.h"
/* --------------------------------------- */
int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    /* --------------------------------------- */
    auto subnet         = FLAGS_local ? defaults::LOCAL_subnet : defaults::AWS_subnet;
    auto host_base      = FLAGS_local ? defaults::LOCAL_host_base : defaults::AWS_host_base;
    auto local_node     = sys::Node{FLAGS_threads};
    u16 node_id         = local_node.get_id();
    /* --------------------------------------- */
    FLAGS_npages        = std::max(1u, FLAGS_npages);
    auto table          = Table{node_id};
    auto& swips         = table.get_swips();
    /* --------------------------------------- */
    u32 num_pages_cache = ((FLAGS_random ? 100 : FLAGS_cache) * swips.size()) / 100u;
    auto cache          = Cache<PageTable>{num_pages_cache};
    table.populate_cache(cache, num_pages_cache, FLAGS_sequential_io);
    /* --------------------------------------- */
    auto barrier_start = ::pthread_barrier_t{};
    auto barrier_end   = ::pthread_barrier_t{};
    ::pthread_barrier_init(&barrier_start, nullptr, FLAGS_threads + 1);
    ::pthread_barrier_init(&barrier_end, nullptr, FLAGS_threads + 1);
    /* --------------------------------------- */
    auto current_swip = std::atomic{0ul};
    /* --------------------------------------- */
    FLAGS_partitions  = next_power_2(FLAGS_partitions) * FLAGS_nodes;
    auto npeers       = u32{FLAGS_nodes - 1};
    DEBUGGING(auto tuples_processed = std::atomic{0ul});
    DEBUGGING(auto tuples_sent = std::atomic{0ul});
    DEBUGGING(auto tuples_received = std::atomic{0ul});
    DEBUGGING(auto pages_recv = std::atomic{0ul});
    /* --------------------------------------- */
    // create threads
    auto threads = std::vector<std::jthread>{};
    for (u16 thread_id : range(FLAGS_threads)) {
        threads.emplace_back([=, &local_node, &current_swip, &swips, &table, &barrier_start, &barrier_end DEBUGGING(, &tuples_processed, &tuples_sent, &tuples_received, &pages_recv)] {
            if (FLAGS_pin) {
                local_node.pin_thread(thread_id);
            }
            /* --------------------------------------- */
            // setup connections to each node, forming a logical clique topology
            // note that connections need to be setup in a particular order to avoid deadlocks!
            auto socket_fds = std::vector<int>{};
            /* --------------------------------------- */
            // accept from [0, node_id)
            if (node_id) {
                auto conn = Connection{node_id, FLAGS_threads, thread_id, node_id};
                conn.setup_ingress();
                socket_fds = std::move(conn.socket_fds);
            }
            /* --------------------------------------- */
            // connect to [node_id + 1, FLAGS_nodes)
            for (u16 peer : range(node_id + 1u, FLAGS_nodes)) {
                auto destination_ip = std::string{subnet} + std::to_string(host_base + (FLAGS_local ? 0 : peer));
                auto conn           = Connection{node_id, FLAGS_threads, thread_id, destination_ip, 1};
                conn.setup_egress(peer);
                socket_fds.emplace_back(conn.socket_fds[0]);
            }
            /* --------------------------------------- */
            auto io_buffers = std::vector<PageTable>(defaults::local_io_depth);
            DEBUGGING(u64 local_tuples_processed{0});
            DEBUGGING(u64 local_tuples_sent{0});
            DEBUGGING(u64 local_tuples_received{0});
            /* --------------------------------------- */
            auto recv_alloc               = BlockAlloc{npeers * 10, FLAGS_maxalloc};
            auto manager_recv             = IngressManager{npeers, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
            auto manager_send             = EgressManager{npeers, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
            u32 peers_done                = 0;
            /* --------------------------------------- */
            auto ingress_page_consumer_fn = std::function{[&peers_done, &recv_alloc, &manager_recv](const PageResult* page, u32 dst) {
                if (page->is_last_page()) {
                    // recv sketch after last page
                    peers_done++;
                }
                else {
                    manager_recv.recv(dst, recv_alloc.get_object());
                }
            }};
            manager_recv.register_consumer_fn(ingress_page_consumer_fn);
            /* --------------------------------------- */
            // setup local uring manager
            auto thread_io = IO_Manager{FLAGS_depthio, FLAGS_sqpoll};
            if (not FLAGS_random and not FLAGS_path.empty()) {
                thread_io.register_files({table.get_file().get_file_descriptor()});
            }
            /* --------------------------------------- */
            // dependency injection
            u32 part_offset   = 0;
            auto eviction_fns = std::vector<BufferLocal::EvictionFn>(FLAGS_partitions);
            for (u64 part_no : range(FLAGS_partitions)) {
                u16 dst                   = (part_no * FLAGS_nodes) / FLAGS_partitions;
                auto parts_per_dst        = (FLAGS_partitions / FLAGS_nodes) + (dst < (FLAGS_partitions % FLAGS_nodes));
                bool final_dst_partition  = ((part_no - part_offset + 1) % parts_per_dst) == 0;
                part_offset              += final_dst_partition ? parts_per_dst : 0;
                auto actual_dst           = dst - (dst > node_id);
                if (dst == node_id) {
                    eviction_fns[part_no] = [](PageResult*, const bool) {};
                }
                else {
                    eviction_fns[part_no] = [actual_dst, final_dst_partition, &manager_send](PageResult* page, const bool is_last = false) {
                        if (not page->empty() or final_dst_partition) {
                            page->retire();
                            if (is_last and final_dst_partition) {
                                page->set_last_page();
                            }
                            manager_send.send(actual_dst, page);
                        }
                    };
                }
            }
            /* --------------------------------------- */
            auto block_alloc                      = BlockAlloc{FLAGS_partitions * FLAGS_bump, FLAGS_maxalloc};
            auto partition_buffer                 = BufferLocal{FLAGS_partitions, block_alloc, eviction_fns};
            auto inserter_loc                     = InserterLocal{FLAGS_partitions, partition_buffer};
            /* --------------------------------------- */
            std::function egress_page_consumer_fn = [&block_alloc](PageResult* pg) -> void { block_alloc.return_object(pg); };
            manager_send.register_consumer_fn(egress_page_consumer_fn);
            /* --------------------------------------- */
            auto insert_into_buffer = [&inserter_loc DEBUGGING(, &local_tuples_processed)](const PageTable& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    inserter_loc.insert(page.get_tuple<TUPLE_IDXS>(j), page.get_tuple<KEY_IDXS>(j));
                }
                DEBUGGING(local_tuples_processed += page.num_tuples);
            };
            std::function process_local_page = insert_into_buffer;
            /* --------------------------------------- */
            // barrier
            ::pthread_barrier_wait(&barrier_start);
            for (u16 dst : range(npeers)) {
                manager_recv.recv(dst, recv_alloc.get_object());
            }
            /* --------------------------------------- */
            u64 morsel_begin, morsel_end;
            auto nswips       = swips.size();
            auto* swips_begin = swips.data();
            while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < swips.size()) {
                morsel_end = std::min(morsel_begin + FLAGS_morselsz, nswips);
                // handle communication
                manager_send.try_drain_pending();
                if (peers_done < npeers) {
                    manager_recv.consume_done();
                }
                // partition swips such that unswizzled swips are at the beginning of the morsel
                auto swizzled_idx = std::stable_partition(swips_begin + morsel_begin, swips_begin + morsel_end, [](const Swip& swip) { return !swip.is_pointer(); }) - swips_begin;
                // submit io requests before processing in-memory pages to overlap I/O with computation
                thread_io.batch_async_io<READ>(table.segment_id, std::span{swips_begin + morsel_begin, swips_begin + swizzled_idx}, io_buffers, true);
                while (swizzled_idx < morsel_end) {
                    process_local_page(*swips[swizzled_idx++].get_pointer<PageTable>());
                }
                while (thread_io.has_inflight_requests()) {
                    process_local_page(*thread_io.get_next_page<PageTable>());
                }
            }
            /* --------------------------------------- */
            partition_buffer.finalize();
            while (peers_done < npeers) {
                manager_recv.consume_done();
                manager_send.try_drain_pending();
            }
            manager_send.wait_all();
            // barrier
            ::pthread_barrier_wait(&barrier_end);
            /* --------------------------------------- */
            DEBUGGING(tuples_sent += local_tuples_sent);
            DEBUGGING(tuples_processed += local_tuples_processed);
            DEBUGGING(tuples_received += local_tuples_received);
            DEBUGGING(pages_recv += manager_recv.get_pages_recv());
        });
    }
    /* --------------------------------------- */
    Stopwatch swatch{};
    ::pthread_barrier_wait(&barrier_start);
    swatch.start();
    ::pthread_barrier_wait(&barrier_end);
    swatch.stop();
    /* --------------------------------------- */
    ::pthread_barrier_destroy(&barrier_start);
    ::pthread_barrier_destroy(&barrier_end);
    /* --------------------------------------- */
    DEBUGGING(print("tuples received:", tuples_received.load()));                                                          //
    DEBUGGING(print("tuples sent:", tuples_sent.load()));                                                                  //
    DEBUGGING(u64 pages_local = (tuples_processed + PageTable::max_tuples_per_page - 1) / PageTable::max_tuples_per_page); //
    DEBUGGING(u64 local_sz = pages_local * defaults::local_page_size);                                                     //
    DEBUGGING(u64 recv_sz = pages_recv * defaults::network_page_size);                                                     //

    Logger{FLAGS_print_header, FLAGS_csv}
        .log("node id", node_id)
        .log("nodes", FLAGS_nodes)
        .log("traffic", "both"s)
        .log("operator", "shuffle"s)
        .log("implementation", "homogeneous"s)
        .log("allocator", MemAlloc::get_type())
        .log("schema", get_schema_str<TABLE_SCHEMA>())
        .log("result type indexes", TUPLE_IDXS)
        .log("key type indexes", KEY_IDXS)
        .log("page size (local)", defaults::local_page_size)
        .log("max tuples per page (local)", PageTable::max_tuples_per_page)
        .log("page size (network)", defaults::network_page_size)
        .log("max tuples per page (network)", PageResult::max_tuples_per_page)
        .log("cache (%)", FLAGS_cache)
        .log("pin", FLAGS_pin)
        .log("morsel size", FLAGS_morselsz)
        .log("total pages", FLAGS_npages)
        .log("partitions", FLAGS_partitions)
        .log("threads", FLAGS_threads)
        .log("time (ms)", swatch.time_ms)                                                            //
        DEBUGGING(.log("local tuples processed", tuples_processed))                                  //
        DEBUGGING(.log("pages received", pages_recv))                                                //
        DEBUGGING(.log("local throughput (Gb/s)", (local_sz * 8 * 1000) / (1e9 * swatch.time_ms)))   //
        DEBUGGING(.log("network throughput (Gb/s)", (recv_sz * 8 * 1000) / (1e9 * swatch.time_ms))); //
}
