#include "config.h"

int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    FLAGS_partitions = next_power_2(FLAGS_partitions);
    FLAGS_slots = next_power_2(FLAGS_slots);

    auto subnet = FLAGS_local ? defaults::LOCAL_subnet : defaults::AWS_subnet;
    auto host_base = FLAGS_local ? defaults::LOCAL_host_base : defaults::AWS_host_base;

    sys::Node local_node{FLAGS_threads};

    /* ----------- DATA LOAD ----------- */

    auto node_id = local_node.get_id();
    Table table{FLAGS_random};
    if (FLAGS_random) {
        table.prepare_random_swips(FLAGS_npages / FLAGS_nodes);
    }
    else {
        // prepare local IO at node offset (adjusted for page boundaries)
        File file{FLAGS_path, FileMode::READ};
        auto offset_begin = (((file.get_total_size() / FLAGS_nodes) * node_id) / defaults::local_page_size) * defaults::local_page_size;
        auto offset_end = (((file.get_total_size() / FLAGS_nodes) * (node_id + 1)) / defaults::local_page_size) * defaults::local_page_size;
        if (node_id == FLAGS_nodes - 1) {
            offset_end = file.get_total_size();
        }
        file.set_offset(offset_begin, offset_end);
        table.bind_file(std::move(file));
        table.prepare_file_swips();
        DEBUGGING(print("reading bytes:", offset_begin, "→", offset_end, (offset_end - offset_begin) / defaults::local_page_size, "pages"));
    }

    auto& swips = table.get_swips();

    // prepare cache
    u32 num_pages_cache = ((FLAGS_random ? 100 : FLAGS_cache) * swips.size()) / 100u;
    Cache<PageTable> cache{num_pages_cache};
    table.populate_cache(cache, num_pages_cache, FLAGS_sequential_io);
    DEBUGGING(print("finished populating cache"));

    /* ----------- THREAD SETUP ----------- */

    // control atomics
    ::pthread_barrier_t barrier_start{};
    ::pthread_barrier_t barrier_preagg{};
    ::pthread_barrier_t barrier_end{};
    ::pthread_barrier_init(&barrier_start, nullptr, FLAGS_threads + 1);
    ::pthread_barrier_init(&barrier_preagg, nullptr, FLAGS_threads);
    ::pthread_barrier_init(&barrier_end, nullptr, FLAGS_threads + 1);
    std::atomic<u64> current_swip{0};
    std::atomic<bool> global_ht_construction_complete{false};
    std::atomic<u64> pages_pre_agg{0};
    StorageGlobal storage_glob{FLAGS_consumepart ? FLAGS_partitions : 1};
    SketchGlobal sketch_glob;
    HashtableGlobal ht_glob;
    DEBUGGING(std::atomic<u64> tuples_processed{0});
    DEBUGGING(std::atomic<u64> tuples_sent{0});
    DEBUGGING(std::atomic<u64> tuples_received{0});
    DEBUGGING(std::atomic<u64> pages_recv{0});

    tbb::concurrent_vector<u64> times_preagg;
    times_preagg.resize(FLAGS_threads);

    FLAGS_partitions *= FLAGS_nodes;

    // create threads
    std::vector<std::jthread> threads{};
    for (u16 thread_id : range(FLAGS_threads)) {
        threads.emplace_back([=, &local_node, &current_swip, &swips, &table, &storage_glob, &barrier_start, &barrier_preagg, &barrier_end, &ht_glob, &sketch_glob,
                              &global_ht_construction_complete, &times_preagg,
                              &pages_pre_agg DEBUGGING(, &tuples_processed, &tuples_sent, &tuples_received, &pages_recv)] {
            if (FLAGS_pin) {
                local_node.pin_thread(thread_id);
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
            for (u16 peer : range(node_id + 1u, FLAGS_nodes)) {
                auto destination_ip = std::string{subnet} + std::to_string(host_base + (FLAGS_local ? 0 : peer));
                Connection conn{node_id, FLAGS_threads, thread_id, destination_ip, 1};
                conn.setup_egress(peer);
                socket_fds.emplace_back(conn.socket_fds[0]);
            }

            /* ----------- BUFFERS ----------- */

            std::vector<PageTable> io_buffers(defaults::local_io_depth);
            DEBUGGING(u64 local_tuples_processed{0});
            DEBUGGING(u64 local_tuples_sent{0});
            DEBUGGING(u64 local_tuples_received{0});

            /* ----------- NETWORK I/O ----------- */

            auto npeers = FLAGS_nodes - 1;

            BlockAlloc recv_alloc{npeers * 10, FLAGS_maxalloc};
            IngressManager manager_recv{npeers, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
            EgressManager manager_send{npeers, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
            std::vector<SketchLocal> remote_sketches(npeers);
            u32 peers_done = 0;

            std::function<void(PageBuffer*, u32)> ingress_page_consumer_fn = [&peers_done, &recv_alloc, &storage_glob, &remote_sketches, &manager_recv](PageBuffer* page,
                                                                                                                                                        u32 dst) {
                if (page->is_last_page()) {
                    // recv sketch after last page
                    manager_recv.post_recvs(dst, remote_sketches.data() + dst);
                }
                else {
                    manager_recv.post_recvs(dst, recv_alloc.get_page());
                }
                if (not page->empty()) {
                    storage_glob.add_page(page, page->get_part_no());
                }
            };
            std::function<void(SketchLocal*, u32)> ingress_sketch_consumer_fn = [&sketch_glob, &peers_done](SketchLocal* sketch, u32) {
                sketch_glob.merge_concurrent(*sketch);
                peers_done++;
            };
            manager_recv.register_consumer_fn(ingress_page_consumer_fn);
            manager_recv.register_consumer_fn(ingress_sketch_consumer_fn);

            /* ----------- LOCAL I/O ----------- */

            // setup local uring manager
            IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
            if (not FLAGS_random and not FLAGS_path.empty()) {
                thread_io.register_files({table.get_file().get_file_descriptor()});
            }

            /* ------------ GROUP BY ------------ */

            u32 part_offset{0};
            std::vector<BufferLocal::EvictionFn> eviction_fns(FLAGS_partitions);
            for (u64 part_no : range(FLAGS_partitions)) {
                u16 dst = (part_no * FLAGS_nodes) / FLAGS_partitions;
                auto parts_per_dst = (FLAGS_partitions / FLAGS_nodes) + (dst < (FLAGS_partitions % FLAGS_nodes));
                bool final_dst_partition = ((part_no - part_offset + 1) % parts_per_dst) == 0;
                part_offset += final_dst_partition ? parts_per_dst : 0;
                auto final_part_no = FLAGS_consumepart ? part_no - (dst * parts_per_dst) : 0;
                if (dst == node_id) {
                    eviction_fns[part_no] = [final_part_no, &storage_glob](PageBuffer* page, bool) {
                        if (not page->empty()) {
                            page->retire();
                            storage_glob.add_page(page, final_part_no);
                        }
                    };
                }
                else {
                    auto actual_dst = dst - (dst > node_id);
                    eviction_fns[part_no] = [final_part_no, actual_dst, final_dst_partition, &manager_send](PageBuffer* page, bool is_last = false) {
                        if (not page->empty() or final_dst_partition) {
                            page->retire();
                            page->set_part_no(final_part_no);
                            if (is_last and final_dst_partition) {
                                page->set_last_page();
                            }
                            manager_send.try_flush(actual_dst, page);
                        }
                    };
                }
            }
            BlockAlloc block_alloc(FLAGS_partitions * FLAGS_bump, FLAGS_maxalloc);
            BufferLocal partition_buffer{FLAGS_partitions, block_alloc, eviction_fns};
            auto partition_groups = FLAGS_nodes;
            InserterLocal inserter_loc{FLAGS_partitions, partition_buffer, partition_groups};
            HashtableLocal ht_loc{FLAGS_partitions, FLAGS_slots, FLAGS_thresh, partition_buffer, inserter_loc};

            /* ------------ LAMBDAS ------------ */

            std::function<void(PageBuffer*)> egress_page_consumer_fn = [&block_alloc](PageBuffer* pg) -> void { block_alloc.return_page(pg); };
            manager_send.register_consumer_fn(egress_page_consumer_fn);

            auto insert_into_ht = [&ht_loc DEBUGGING(, &local_tuples_processed)](const PageTable& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    auto group = page.get_tuple<GPR_KEYS_IDX>(j);
                    auto agg = std::make_tuple<AGG_KEYS>(AGG_VALS);
                    ht_loc.insert(group, agg);
                }
                DEBUGGING(local_tuples_processed += page.num_tuples);
            };

            auto insert_into_buffer = [&inserter_loc DEBUGGING(, &local_tuples_processed)](const PageTable& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    auto group = page.get_tuple<GPR_KEYS_IDX>(j);
                    auto agg = std::make_tuple<AGG_KEYS>(AGG_VALS);
                    inserter_loc.insert(group, agg);
                }
                DEBUGGING(local_tuples_processed += page.num_tuples);
            };

            std::function<void(const PageTable&)> process_local_page = insert_into_ht;

            auto consume_ingress = [&manager_recv]() { return manager_recv.consume_pages(); };

            auto process_page_glob = [&ht_glob](PageBuffer& page) {
                for (auto j{0u}; j < page.get_num_tuples(); ++j) {
                    ht_glob.insert(page.get_tuple_ref(j));
                }
            };

            // barrier
            ::pthread_barrier_wait(&barrier_start);
            Stopwatch swatch_preagg{};
            swatch_preagg.start();

            /* ----------- BEGIN ----------- */

            for (u16 dst : range(npeers)) {
                manager_recv.post_recvs(dst, recv_alloc.get_page());
            }
            // morsel loop
            u64 morsel_begin, morsel_end;
            const u64 nswips = swips.size();
            auto* swips_begin = swips.data();
            while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < swips.size()) {
                morsel_end = std::min(morsel_begin + FLAGS_morselsz, nswips);

                // handle communication
                manager_send.try_drain_pending();
                if (peers_done < npeers) {
                    consume_ingress();
                }

                // partition swips such that unswizzled swips are at the beginning of the morsel
                auto swizzled_idx =
                    std::stable_partition(swips_begin + morsel_begin, swips_begin + morsel_end, [](const Swip& swip) { return !swip.is_pointer(); }) - swips_begin;

                // submit io requests before processing in-memory pages to overlap I/O with computation
                thread_io.batch_async_io<READ>(table.segment_id, std::span{swips_begin + morsel_begin, swips_begin + swizzled_idx}, io_buffers, true);

                while (swizzled_idx < morsel_end) {
                    process_local_page(*swips[swizzled_idx++].get_pointer<PageTable>());
                }
                while (thread_io.has_inflight_requests()) {
                    process_local_page(*thread_io.get_next_page<PageTable>());
                }

                if (do_adaptive_preagg and FLAGS_adapre and ht_loc.is_useless()) {
                    // turn off pre-aggregation
                    FLAGS_adapre = false;
                    process_local_page = insert_into_buffer;
                }
            }
            partition_buffer.finalize();
            for (u32 part_grp : range(partition_groups)) {
                if (part_grp == node_id) {
                    // merge local sketch
                    sketch_glob.merge_concurrent(inserter_loc.get_sketch(part_grp));
                }
                else {
                    // send remote sketches
                    auto actual_dst = part_grp - (part_grp > node_id);
                    manager_send.try_flush(actual_dst, &inserter_loc.get_sketch(part_grp));
                }
            }
            while (peers_done < npeers) {
                consume_ingress();
                manager_send.try_drain_pending();
            }
            manager_send.wait_all();

            swatch_preagg.stop();
            ::pthread_barrier_wait(&barrier_preagg);

            if (thread_id == 0) {
                // thread 0 initializes global ht
                ht_glob.initialize(next_power_2(static_cast<u64>(FLAGS_htfactor * sketch_glob.get_estimate())));
                // reset morsel
                current_swip = 0;
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

            times_preagg[thread_id] = swatch_preagg.time_ms;
            // barrier
            ::pthread_barrier_wait(&barrier_end);

            /* ----------- END ----------- */

            if (thread_id == 0) {
                if (FLAGS_consumepart) {
                    std::for_each(storage_glob.partition_pages.begin(), storage_glob.partition_pages.end(),
                                  [&pages_pre_agg](auto&& part_pgs) { pages_pre_agg += part_pgs.size(); });
                }
                else {
                    pages_pre_agg = storage_glob.partition_pages[0].size();
                }
            }

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

    DEBUGGING(print("tuples received:", tuples_received.load()));                                                          //
    DEBUGGING(print("tuples sent:", tuples_sent.load()));                                                                  //
    DEBUGGING(u64 pages_local = (tuples_processed + PageTable::max_tuples_per_page - 1) / PageTable::max_tuples_per_page); //
    DEBUGGING(u64 local_sz = pages_local * defaults::local_page_size);                                                     //
    DEBUGGING(u64 recv_sz = pages_recv * defaults::network_page_size);                                                     //

    Logger{FLAGS_print_header, FLAGS_csv}
        .log("node id", node_id)
        .log("nodes", FLAGS_nodes)
        .log("traffic", "both"s)
        .log("operator", "aggregation"s)
        .log("implementation", "homogeneous"s)
        .log("allocator", MemAlloc::get_type())
        .log("schema", get_schema_str<SCHEMA>())
        .log("group keys", get_schema_str<GRP_KEYS>())
        .log("aggregation keys", get_schema_str<AGG_KEYS>())
        .log("page size (local)", defaults::local_page_size)
        .log("max tuples per page (local)", PageTable::max_tuples_per_page)
        .log("page size (hashtable)", defaults::hashtable_page_size)
        .log("max tuples per page (hashtable)", PageBuffer::max_tuples_per_page)
        .log("hashtable (local)", HashtableLocal::get_type())
        .log("hashtable (global)", HashtableGlobal::get_type())
        .log("sketch (local)", SketchLocal::get_type())
        .log("sketch (global)", SketchGlobal::get_type())
        .log("consume partitions", FLAGS_consumepart)
        .log("adaptive pre-aggregation", do_adaptive_preagg)
        .log("threshold pre-aggregation", FLAGS_thresh)
        .log("cache (%)", FLAGS_cache)
        .log("pin", FLAGS_pin)
        .log("morsel size", FLAGS_morselsz)
        .log("total pages", FLAGS_npages)
        .log("ht factor", FLAGS_htfactor)
        .log("partitions", FLAGS_partitions)
        .log("slots", FLAGS_slots)
        .log("threads", FLAGS_threads)
        .log("groups seed", FLAGS_seed)
        .log("groups total (actual)", FLAGS_groups)
        .log("groups node (estimate)", sketch_glob.get_estimate())
        .log("pages pre-agg", pages_pre_agg)
        .log("mean pre-agg time (ms)", std::reduce(times_preagg.begin(), times_preagg.end()) * 1.0 / times_preagg.size())
        .log("time (ms)", swatch.time_ms)                                                            //
        DEBUGGING(.log("local tuples processed", tuples_processed))                                  //
        DEBUGGING(.log("pages received", pages_recv))                                                //
        DEBUGGING(.log("local throughput (Gb/s)", (local_sz * 8 * 1000) / (1e9 * swatch.time_ms)))   //
        DEBUGGING(.log("network throughput (Gb/s)", (recv_sz * 8 * 1000) / (1e9 * swatch.time_ms))); //

    print("global ht size", ht_glob.size_mask + 1);
    u64 count{0};
    u64 inserts{0};
    for (u64 i : range(ht_glob.size_mask + 1)) {
        auto slot = ht_glob.slots[i].load();
        if (slot) {
            auto slot_count = std::get<0>(reinterpret_cast<HashtableGlobal::slot_idx_raw_t>(reinterpret_cast<uintptr_t>(slot) >> 16)->get_aggregates());
            count += slot_count;
            inserts++;
        }
    }
    print("INSERTS:", inserts);
    print("COUNT:", count);
}
