#include "config.h"

/* ----------- MAIN ----------- */

int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto subnet    = FLAGS_local ? defaults::LOCAL_subnet : defaults::AWS_subnet;
    auto host_base = FLAGS_local ? defaults::LOCAL_host_base : defaults::AWS_host_base;

    if (FLAGS_nthreads > FLAGS_qthreads) {
        throw InvalidOptionError{"Number of query threads must not be less than number of network threads"};
    }

    sys::Node local_node{FLAGS_nthreads + FLAGS_qthreads};

    /* ----------- DATA LOAD ----------- */

    u16 node_id = local_node.get_id();
    Table table{node_id};
    auto& swips         = table.get_swips();

    // prepare cache
    u32 num_pages_cache = ((FLAGS_random ? 100 : FLAGS_cache) * swips.size()) / 100u;
    Cache<PageTable> cache{num_pages_cache};
    table.populate_cache(cache, num_pages_cache, FLAGS_sequential_io);

    /* ----------- THREAD SETUP ----------- */

    auto npeers      = FLAGS_nodes - 1;
    FLAGS_partitions = next_power_2(FLAGS_partitions);
    FLAGS_slots      = next_power_2(FLAGS_slots);

    DEBUGGING(std::atomic<u64> pages_recv{0});
    // barriers
    ::pthread_barrier_t barrier_network_setup{};
    ::pthread_barrier_t barrier_start{};
    ::pthread_barrier_t barrier_preaggregation{};
    ::pthread_barrier_t barrier_end{};
    ::pthread_barrier_init(&barrier_network_setup, nullptr, FLAGS_nthreads + 1);
    ::pthread_barrier_init(&barrier_start, nullptr, FLAGS_qthreads + 1);
    ::pthread_barrier_init(&barrier_preaggregation, nullptr, FLAGS_qthreads);
    ::pthread_barrier_init(&barrier_end, nullptr, FLAGS_nthreads + FLAGS_qthreads + 1);
    std::atomic<bool> global_ht_construction_complete{false};
    StorageGlobal storage_glob{FLAGS_consumepart ? FLAGS_partitions : 1};
    Sketch sketch_glob;
    HashtableGlobal ht_glob;

    tbb::concurrent_vector<u64> times_preagg(FLAGS_qthreads);

    // networking
    std::vector<QueryThreadGroup> thread_grps(FLAGS_nthreads, QueryThreadGroup{npeers});
    std::vector<std::jthread> threads_network{};
    for (u16 nthread_id : range(FLAGS_nthreads)) {
        threads_network.emplace_back([=, &local_node, &thread_grps, &storage_glob, &sketch_glob, &barrier_network_setup, &barrier_end DEBUGGING(, &pages_recv)]() {
            if (FLAGS_pin) {
                local_node.pin_thread(nthread_id);
            }

            /* ----------- NETWORK I/O ----------- */

            // setup connections to each node, forming a logical clique topology
            // note that connections need to be setup in a particular order to avoid deadlocks!
            std::vector<int> socket_fds{};

            // accept from [0, node_id)
            if (node_id) {
                Connection conn{node_id, FLAGS_nthreads, nthread_id, node_id};
                conn.setup_ingress();
                socket_fds = std::move(conn.socket_fds);
            }

            // connect to [node_id + 1, FLAGS_nodes)
            for (auto i{node_id + 1u}; i < FLAGS_nodes; ++i) {
                auto destination_ip = std::string{subnet} + std::to_string(host_base + (FLAGS_local ? 0 : i));
                Connection conn{node_id, FLAGS_nthreads, nthread_id, destination_ip, 1};
                conn.setup_egress(i);
                socket_fds.emplace_back(conn.socket_fds[0]);
            }

            auto qthreads_per_nthread = (FLAGS_qthreads / FLAGS_nthreads) + (nthread_id < (FLAGS_qthreads % FLAGS_nthreads));

            EgressManager manager_send{npeers, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
            IngressManager manager_recv{npeers, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
            BlockAllocIngress alloc_ingress{npeers * 10, FLAGS_maxalloc * qthreads_per_nthread};
            BlockAllocEgress alloc_egress{npeers * 10, FLAGS_maxalloc * qthreads_per_nthread};
            thread_grps[nthread_id].egress_mgr                             = &manager_send;
            thread_grps[nthread_id].ingress_mgr                            = &manager_recv;
            thread_grps[nthread_id].alloc_ingress                          = &alloc_ingress;
            thread_grps[nthread_id].alloc_egress                           = &alloc_egress;

            const auto& sketches_ingress                                   = thread_grps[nthread_id].sketches_ingress;
            std::function<void(PageResult*, u32)> ingress_page_consumer_fn = [&alloc_ingress, &storage_glob, &sketches_ingress, &manager_recv](PageResult* page, u32 dst) {
                if (page->is_last_page()) {
                    // recv sketch after last page
                    manager_recv.post_recvs(dst, sketches_ingress.data() + dst);
                    if (page->empty()) {
                        return;
                    }
                }
                else {
                    manager_recv.post_recvs(dst, alloc_ingress.get_page());
                }
                storage_glob.add_page(page, page->get_part_no());
            };

            auto& peers_done                         = thread_grps[nthread_id].peers_done;
            std::function ingress_sketch_consumer_fn = [&sketch_glob, &peers_done](const Sketch* sketch, u32) {
                sketch_glob.merge_concurrent(*sketch);
                ++peers_done;
            };
            manager_recv.register_consumer_fn(ingress_page_consumer_fn);
            manager_recv.register_consumer_fn(ingress_sketch_consumer_fn);

            std::function<void(PageResult*)> egress_page_consumer_fn = [nthread_id, &thread_grps](PageResult* page) { thread_grps[nthread_id].alloc_egress->return_page(page); };
            manager_send.register_consumer_fn(egress_page_consumer_fn);

            // barrier
            ::pthread_barrier_wait(&barrier_network_setup);

            for (u16 dst : range(npeers)) {
                manager_recv.post_recvs(dst, alloc_ingress.get_page());
            }

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
    std::atomic<u64> pages_pre_agg{0};
    DEBUGGING(std::atomic<u64> tuples_local{0});
    DEBUGGING(std::atomic<u64> tuples_sent{0});
    DEBUGGING(std::atomic<u64> tuples_received{0});

    std::vector<std::jthread> threads_query;
    for (u16 qthread_id : range(FLAGS_qthreads)) {
        threads_query.emplace_back([=, &local_node, &barrier_preaggregation, &current_swip, &swips, &table, &barrier_start, &barrier_end, &times_preagg, &thread_grps, &storage_glob,
                                    &sketch_glob, &pages_pre_agg, &global_ht_construction_complete, &ht_glob DEBUGGING(, &tuples_local, &tuples_sent, &tuples_received, &pages_recv)]() {
            if (FLAGS_pin) {
                local_node.pin_thread(qthread_id + FLAGS_nthreads);
            }

            /* -------- THREAD MAPPING -------- */

            auto [dedicated_network_thread, qthreads_per_nthread, qthread_local_id] = find_dedicated_nthread(qthread_id);
            IngressManager& manager_recv                                            = *thread_grps[dedicated_network_thread].ingress_mgr;
            EgressManager& manager_send                                             = *thread_grps[dedicated_network_thread].egress_mgr;
            DEBUGGING(print("assigning qthread", qthread_id, "to nthread", dedicated_network_thread));

            /* ----------- BUFFERS ----------- */

            std::vector<PageTable> io_buffers(defaults::local_io_depth);
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
            std::vector<BufferLocal::EvictionFn> eviction_fns(FLAGS_partitions);
            for (u64 part_no : range(FLAGS_partitions)) {
                u16 dst                   = (part_no * FLAGS_nodes) / FLAGS_partitions;
                auto parts_per_dst        = (FLAGS_partitions / FLAGS_nodes) + (dst < (FLAGS_partitions % FLAGS_nodes));
                bool final_dst_partition  = ((part_no - part_offset + 1) % parts_per_dst) == 0;
                part_offset              += final_dst_partition ? parts_per_dst : 0;
                // partition number is local to the recipient node
                auto final_part_no        = FLAGS_consumepart ? part_no - (dst * parts_per_dst) : 0;
                if (dst == node_id) {
                    eviction_fns[part_no] = [final_part_no, &storage_glob](PageResult* page, bool) {
                        if (not page->empty()) {
                            page->retire();
                            storage_glob.add_page(page, final_part_no);
                        }
                    };
                }
                else {
                    auto actual_dst       = dst - (dst > node_id);
                    eviction_fns[part_no] = [final_part_no, actual_dst, final_dst_partition, &manager_send](PageResult* page, bool is_last = false) {
                        if (not page->empty() or final_dst_partition) {
                            page->set_part_no(final_part_no);
                            page->retire();
                            if (is_last and final_dst_partition) {
                                page->set_last_page();
                            }
                            manager_send.send(actual_dst, page);
                        }
                    };
                }
            }

            BufferLocal partition_buffer{FLAGS_partitions, *thread_grps[dedicated_network_thread].alloc_egress, eviction_fns};
            auto partition_groups = FLAGS_nodes;
            InserterLocal inserter_loc{FLAGS_partitions, partition_buffer, partition_groups};
            HashtableLocal ht_loc{FLAGS_partitions, FLAGS_slots, FLAGS_thresh, partition_buffer, inserter_loc};

            /* ------------ LAMBDAS ------------ */

            auto insert_into_ht = [&ht_loc DEBUGGING(, &local_tuples_processed)](const PageTable& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    auto group = page.get_tuple<GPR_KEYS_IDX>(j);
                    auto agg   = std::make_tuple<AGG_KEYS>(AGG_VALS);
                    ht_loc.insert(group, agg);
                }
                DEBUGGING(local_tuples_processed += page.num_tuples);
            };

            auto insert_into_buffer = [&inserter_loc DEBUGGING(, &local_tuples_processed)](const PageTable& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    auto group = page.get_tuple<GPR_KEYS_IDX>(j);
                    auto agg   = std::make_tuple<AGG_KEYS>(AGG_VALS);
                    inserter_loc.insert(group, agg);
                }
                DEBUGGING(local_tuples_processed += page.num_tuples);
            };

            std::function<void(const PageTable&)> process_local_page = insert_into_ht;

            auto process_page_glob                                   = [&ht_glob](PageResult& page) {
                for (auto j{0u}; j < page.get_num_tuples(); ++j) {
                    ht_glob.insert(page.get_tuple_ref(j));
                }
            };

            // barrier
            ::pthread_barrier_wait(&barrier_start);
            Stopwatch swatch_preagg{};
            swatch_preagg.start();

            /* ----------- BEGIN ----------- */

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
                while (thread_io.has_inflight_requests()) {
                    process_local_page(*thread_io.get_next_page<PageTable>());
                }

                if (FLAGS_adapre and ht_loc.is_useless()) {
                    // turn off pre-aggregation
                    FLAGS_adapre       = false;
                    process_local_page = insert_into_buffer;
                }
            }
            for (u32 part_grp : range(partition_groups)) {
                if (part_grp == node_id) {
                    // merge local sketch
                    sketch_glob.merge_concurrent(inserter_loc.get_sketch(part_grp));
                }
                else {
                    // send remote sketches
                    auto actual_dst = part_grp - (part_grp > node_id);
                    thread_grps[dedicated_network_thread].sketches_egress[actual_dst].merge_concurrent(inserter_loc.get_sketch(part_grp));
                }
            }

            if (qthread_local_id == 0) {
                // wait for other qthreads to add their active pages
                // wait for other qthreads to finalize their buffers (unless there is only one qthread in this group)
                thread_grps[dedicated_network_thread].all_qthreads_added_last_page.wait(qthreads_per_nthread == 1);
                partition_buffer.finalize(true);
                for (u16 j{0}; j < npeers; ++j) {
                    manager_send.send(j, &(thread_grps[dedicated_network_thread].sketches_egress[j]));
                }
                manager_send.finished_egress();
            }
            else {
                partition_buffer.finalize(false);
                ++thread_grps[dedicated_network_thread].qthreads_added_last_page;
                if (++thread_grps[dedicated_network_thread].qthreads_added_last_page != qthreads_per_nthread - 1) {
                    thread_grps[dedicated_network_thread].all_qthreads_added_last_page = true;
                    thread_grps[dedicated_network_thread].all_qthreads_added_last_page.notify_one();
                }
            }

            // wait for ingress
            thread_grps[dedicated_network_thread].all_peers_done.wait(false);

            swatch_preagg.stop();
            // barrier
            ::pthread_barrier_wait(&barrier_preaggregation);

            if (qthread_id == 0) {
                // thread 0 initializes global ht
                ht_glob.initialize(next_power_2(static_cast<u64>(FLAGS_htfactor * sketch_glob.get_estimate())));
                // reset morsel
                current_swip                    = 0;
                global_ht_construction_complete = true;
                global_ht_construction_complete.notify_all();
            }
            else {
                global_ht_construction_complete.wait(false);
            }

            if (FLAGS_consumepart) {
                // consume partitions
                const auto npartitions = storage_glob.partition_pages.size();
                while ((morsel_begin = current_swip.fetch_add(1)) < npartitions) {
                    for (auto* page : storage_glob.partition_pages[morsel_begin]) {
                        process_page_glob(*page);
                    }
                }
            }
            else {
                // consume pages from partition 0 (only partition)
                const u64 npages = storage_glob.partition_pages[0].size();
                while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < npages) {
                    morsel_end = std::min(morsel_begin + FLAGS_morselsz, npages);
                    while (morsel_begin < morsel_end) {
                        process_page_glob(*storage_glob.partition_pages[0][morsel_begin++]);
                    }
                }
            }

            times_preagg[qthread_id] = swatch_preagg.time_ms;
            ::pthread_barrier_wait(&barrier_end);

            /* ----------- END ----------- */

            if (qthread_id == 0) {
                if (FLAGS_consumepart) {
                    std::for_each(storage_glob.partition_pages.begin(), storage_glob.partition_pages.end(), [&pages_pre_agg](auto&& part_pgs) { pages_pre_agg += part_pgs.size(); });
                }
                else {
                    pages_pre_agg = storage_glob.partition_pages[0].size();
                }
            }
        });
    }

    Stopwatch swatch{};
    ::pthread_barrier_wait(&barrier_start);
    swatch.start();
    ::pthread_barrier_wait(&barrier_end);
    swatch.stop();

    ::pthread_barrier_destroy(&barrier_network_setup);
    ::pthread_barrier_destroy(&barrier_start);
    ::pthread_barrier_destroy(&barrier_preaggregation);
    ::pthread_barrier_destroy(&barrier_end);

    DEBUGGING(print("tuples received:", tuples_received.load()));      //
    DEBUGGING(print("tuples sent:", tuples_sent.load()));              //
    DEBUGGING(u64 recv_sz = pages_recv * defaults::network_page_size); //

    Logger{FLAGS_print_header, FLAGS_csv}
        .log("node id", node_id)
        .log("nodes", FLAGS_nodes)
        .log("traffic", "both"s)
        .log("operator", "aggregation"s)
        .log("implementation", "homogeneous"s)
        .log("allocator", MemAlloc::get_type())
        .log("schema", get_schema_str<TABLE_SCHEMA>())
        .log("group keys", GPR_KEYS_IDX)
        .log("aggregation keys", get_schema_str<AGG_KEYS>())
        .log("page size (local)", defaults::local_page_size)
        .log("max tuples per page (local)", PageTable::max_tuples_per_page)
        .log("page size (hashtable)", defaults::hashtable_page_size)
        .log("max tuples per page (hashtable)", PageResult::max_tuples_per_page)
        .log("hashtable (local)", HashtableLocal::get_type())
        .log("hashtable (global)", HashtableGlobal::get_type())
        .log("sketch (local)", Sketch::get_type())
        .log("sketch (global)", Sketch::get_type())
        .log("consume partitions", FLAGS_consumepart)
        .log("adaptive pre-aggregation", FLAGS_adapre)
        .log("threshold pre-aggregation", FLAGS_thresh)
        .log("cache (%)", FLAGS_cache)
        .log("pin", FLAGS_pin)
        .log("morsel size", FLAGS_morselsz)
        .log("total pages", FLAGS_npages)
        .log("ht factor", FLAGS_htfactor)
        .log("partitions", FLAGS_partitions)
        .log("slots", FLAGS_slots)
        .log("nthreads", FLAGS_nthreads)
        .log("qthreads", FLAGS_qthreads)
        .log("groups seed", FLAGS_seed)
        .log("groups total (actual)", FLAGS_groups)
        .log("groups node (estimate)", sketch_glob.get_estimate())
        .log("pages pre-agg", pages_pre_agg)
        .log("mean pre-agg time (ms)", std::reduce(times_preagg.begin(), times_preagg.end()) * 1.0 / times_preagg.size())
        .log("time (ms)", swatch.time_ms)                                                            //
        DEBUGGING(.log("pages received", pages_recv))                                                //
        DEBUGGING(.log("network throughput (Gb/s)", (recv_sz * 8 * 1000) / (1e9 * swatch.time_ms))); //

    print("global ht size", ht_glob.size_mask + 1);
    u64 count{0};
    u64 inserts{0};
    for (u64 i : range(ht_glob.size_mask + 1)) {
        if (auto slot = ht_glob.slots[i].load()) {
            auto slot_count  = std::get<0>(reinterpret_cast<HashtableGlobal::slot_idx_raw_t>(reinterpret_cast<uintptr_t>(slot) >> 16)->get_aggregates());
            count           += slot_count;
            ++inserts;
        }
    }
    print("INSERTS:", inserts);
    print("COUNT:", count);
}
