#include "definitions.h"

int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    /* --------------------------------------- */
    adapt::Configuration config{FLAGS_config};
    sys::Node node{FLAGS_nthreads + FLAGS_qthreads};
    u16 node_id         = node.get_id();
    /* --------------------------------------- */
    FLAGS_npages        = std::max(1u, FLAGS_npages);
    auto table          = Table{FLAGS_npages / FLAGS_nodes};
    auto& swips         = table.get_swips();
    /* --------------------------------------- */
    u32 num_pages_cache = ((FLAGS_random ? 100 : FLAGS_cache) * swips.size()) / 100u;
    auto cache          = Cache<PageTable>{num_pages_cache};
    table.populate_cache(cache, num_pages_cache, FLAGS_sequential_io);
    /* --------------------------------------- */
    ::pthread_barrier_t barrier_network_setup{};
    ::pthread_barrier_t barrier_start{};
    ::pthread_barrier_t barrier_end{};
    ::pthread_barrier_init(&barrier_network_setup, nullptr, FLAGS_nthreads + 1);
    ::pthread_barrier_init(&barrier_start, nullptr, FLAGS_qthreads + 1);
    ::pthread_barrier_init(&barrier_end, nullptr, FLAGS_nthreads + FLAGS_qthreads + 1);
    /* --------------------------------------- */
    FLAGS_partitions = next_power_2(FLAGS_partitions) * FLAGS_nodes;
    auto npeers      = u32{FLAGS_nodes - 1};
    DEBUGGING(auto tuples_processed = std::atomic{0ul});
    DEBUGGING(auto tuples_sent = std::atomic{0ul});
    DEBUGGING(auto tuples_received = std::atomic{0ul});
    DEBUGGING(auto pages_recv = std::atomic{0ul});
    /* --------------------------------------- */
    tbb::concurrent_vector<u64> times_preagg(FLAGS_qthreads);
    // networking
    std::vector thread_grps(FLAGS_nthreads, QueryThreadGroup{});
    std::vector<std::jthread> threads_network{};
    for (u16 nthread_id : range(FLAGS_nthreads)) {
        threads_network.emplace_back(
            [=, &node, &thread_grps, &barrier_network_setup, &barrier_end DEBUGGING(, &pages_recv)]
            {
                if (FLAGS_pin) {
                    node.pin_thread(nthread_id);
                }
                /* --------------------------------------- */
                // setup connections to each node, forming a logical clique topology
                // note that connections need to be setup in a particular order to avoid deadlocks!
                std::vector<int> socket_fds;
                /* --------------------------------------- */
                // accept from [0, node_id)
                if (node_id) {
                    auto port  = std::to_string(std::stoi(config.get_worker_info(node_id).port) + node_id * FLAGS_nthreads + nthread_id);
                    socket_fds = Connection::setup_ingress(port, node_id);
                }
                /* --------------------------------------- */
                // connect to [node_id + 1, FLAGS_nodes)
                for (node_t peer_id : range(node_id + 1u, FLAGS_nodes)) {
                    auto [ip, port_base] = config.get_worker_info(peer_id);
                    auto port            = std::to_string(std::stoi(port_base) + peer_id * FLAGS_nthreads + nthread_id);
                    socket_fds.emplace_back(Connection::setup_egress(node_id, ip, port));
                }
                /* --------------------------------------- */
                auto qthreads_per_nthread = (FLAGS_qthreads / FLAGS_nthreads) + (nthread_id < (FLAGS_qthreads % FLAGS_nthreads));
                /* --------------------------------------- */
                EgressManager manager_send{npeers, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
                IngressManager manager_recv{npeers, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
                BlockAllocIngress alloc_ingress{npeers * 10, FLAGS_maxalloc * qthreads_per_nthread};
                BlockAllocEgress alloc_egress{npeers * 10, FLAGS_maxalloc * qthreads_per_nthread};
                thread_grps[nthread_id].egress_mgr     = &manager_send;
                thread_grps[nthread_id].ingress_mgr    = &manager_recv;
                thread_grps[nthread_id].alloc_ingress  = &alloc_ingress;
                thread_grps[nthread_id].alloc_egress   = &alloc_egress;
                /* --------------------------------------- */
                auto& peers_done                       = thread_grps[nthread_id].peers_done;
                std::function ingress_page_consumer_fn = [&alloc_ingress, &manager_recv, &peers_done](PageResult* page, u32 dst)
                {
                    if (page->is_primary_bit_set()) {
                        ++peers_done;
                    } else {
                        manager_recv.recv(dst, alloc_ingress.get_object());
                    }
                };
                manager_recv.register_consumer_fn(ingress_page_consumer_fn);
                /* --------------------------------------- */
                std::function egress_page_consumer_fn = [nthread_id, &thread_grps](PageResult* page) { thread_grps[nthread_id].alloc_egress->return_object(page); };
                manager_send.register_consumer_fn(egress_page_consumer_fn);
                /* --------------------------------------- */
                // barrier
                ::pthread_barrier_wait(&barrier_network_setup);
                for (u16 dst : range(npeers)) {
                    manager_recv.recv(dst, alloc_ingress.get_object());
                }
                /* --------------------------------------- */
                // network loop
                while (peers_done < npeers) {
                    manager_recv.consume_done();
                    manager_send.try_flush_all();
                    manager_send.try_drain_pending();
                }
                thread_grps[nthread_id].all_peers_done = true;
                thread_grps[nthread_id].all_peers_done.notify_all();
                manager_send.wait_all();
                ::pthread_barrier_wait(&barrier_end);
                DEBUGGING(pages_recv += manager_recv.get_pages_recv());
            });
    }
    ::pthread_barrier_wait(&barrier_network_setup);

    // query processing
    std::atomic<u32> current_swip{0};
    DEBUGGING(std::atomic<u64> tuples_local{0});

    std::vector<std::jthread> threads_query;
    for (u16 qthread_id : range(FLAGS_qthreads)) {
        threads_query.emplace_back(
            [=, &node, &current_swip, &swips, &table, &barrier_start, &barrier_end, &times_preagg, &thread_grps DEBUGGING(, &tuples_processed, &tuples_local, &pages_recv)]()
            {
                if (FLAGS_pin) {
                    node.pin_thread(qthread_id + FLAGS_nthreads);
                }
                /* --------------------------------------- */
                auto [dedicated_network_thread, qthreads_per_nthread, qthread_local_id] = find_dedicated_nthread(qthread_id);
                EgressManager& manager_send                                             = *thread_grps[dedicated_network_thread].egress_mgr;
                DEBUGGING(print("assigning qthread", qthread_id, "to nthread", dedicated_network_thread));
                /* --------------------------------------- */
                std::vector<PageTable> io_buffers(defaults::local_io_depth);
                DEBUGGING(u64 local_tuples_processed{0});
                /* --------------------------------------- */
                // setup local uring manager
                IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
                if (not FLAGS_path.empty()) {
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
                    } else {
                        eviction_fns[part_no] = [actual_dst, final_dst_partition, &manager_send](PageResult* page, const bool is_last = false)
                        {
                            if (not page->empty() or final_dst_partition) {
                                page->retire();
                                if (is_last and final_dst_partition) {
                                    page->set_primary_bit();
                                }
                                manager_send.send(actual_dst, page);
                            }
                        };
                    }
                }
                /* --------------------------------------- */
                auto partition_buffer   = BufferLocal{FLAGS_partitions, *thread_grps[dedicated_network_thread].alloc_egress, eviction_fns};
                auto inserter           = InserterLocal{FLAGS_partitions, partition_buffer};
                /* --------------------------------------- */
                auto insert_into_buffer = [&inserter DEBUGGING(, &local_tuples_processed)](const PageTable& page)
                {
                    for (auto j{0u}; j < page.num_tuples; ++j) {
                        inserter.insert(page.get_tuple<TUPLE_IDXS>(j), page.get_tuple<KEY_IDXS>(j));
                    }
                    DEBUGGING(local_tuples_processed += page.num_tuples);
                };
                std::function process_local_page = insert_into_buffer;
                /* --------------------------------------- */
                // barrier
                ::pthread_barrier_wait(&barrier_start);
                Stopwatch swatch_preagg{};
                swatch_preagg.start();
                /* --------------------------------------- */
                // morsel loop
                u64 morsel_begin, morsel_end;
                const u64 nswips  = swips.size();
                auto* swips_begin = swips.data();
                while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < swips.size()) {
                    morsel_end        = std::min(morsel_begin + FLAGS_morselsz, nswips);
                    // partition swips such that unswizzled swips are at the beginning of the morsel
                    auto swizzled_idx = std::stable_partition(swips_begin + morsel_begin, swips_begin + morsel_end, [](const Swip& swip) { return !swip.is_pointer(); }) - swips_begin;
                    // submit I/O requests before processing in-memory pages to overlap I/O with computation
                    thread_io.batch_async_io<READ>(table.segment_id, std::span{swips_begin + morsel_begin, swips_begin + swizzled_idx}, io_buffers, true);
                    while (swizzled_idx < morsel_end) {
                        process_local_page(*swips[swizzled_idx++].get_pointer<PageTable>());
                    }
                }
                if (qthread_local_id == 0) {
                    // wait for other qthreads to add their active pages
                    // wait for other qthreads to finalize their buffers (unless there is only one qthread in this group)
                    thread_grps[dedicated_network_thread].all_qthreads_added_last_page.wait(qthreads_per_nthread == 1);
                    partition_buffer.finalize(true);
                    manager_send.finished_egress();
                } else {
                    partition_buffer.finalize(false);
                    if (++thread_grps[dedicated_network_thread].qthreads_added_last_page == qthreads_per_nthread - 1) {
                        thread_grps[dedicated_network_thread].all_qthreads_added_last_page = true;
                        thread_grps[dedicated_network_thread].all_qthreads_added_last_page.notify_one();
                    }
                }
                /* --------------------------------------- */
                // wait for ingress
                thread_grps[dedicated_network_thread].all_peers_done.wait(false);
                swatch_preagg.stop();
                times_preagg[qthread_id] = swatch_preagg.time_ms;
                // barrier
                ::pthread_barrier_wait(&barrier_end);
                /* ----------- END ----------- */
                DEBUGGING(tuples_processed += local_tuples_processed);
            });
    }
    /* --------------------------------------- */
    Stopwatch swatch{};
    ::pthread_barrier_wait(&barrier_start);
    swatch.start();
    ::pthread_barrier_wait(&barrier_end);
    swatch.stop();
    /* --------------------------------------- */
    ::pthread_barrier_destroy(&barrier_network_setup);
    ::pthread_barrier_destroy(&barrier_start);
    ::pthread_barrier_destroy(&barrier_end);
    /* --------------------------------------- */
    DEBUGGING(u64 pages_local = (tuples_processed + PageTable::max_tuples_per_page - 1) / PageTable::max_tuples_per_page); //
    DEBUGGING(u64 local_sz = pages_local * defaults::local_page_size);
    DEBUGGING(u64 recv_sz = pages_recv * defaults::network_page_size);
    /* --------------------------------------- */
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
        .log("nthreads", FLAGS_nthreads)
        .log("qthreads", FLAGS_qthreads)
        .log("time (ms)", swatch.time_ms)                                                            //
        DEBUGGING(.log("local tuples processed", tuples_processed))                                  //
        DEBUGGING(.log("pages received", pages_recv))                                                //
        DEBUGGING(.log("local throughput (Gb/s)", (local_sz * 8 * 1000) / (1e9 * swatch.time_ms)))   //
        DEBUGGING(.log("network throughput (Gb/s)", (recv_sz * 8 * 1000) / (1e9 * swatch.time_ms))); //
}
