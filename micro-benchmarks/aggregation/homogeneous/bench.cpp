#include "definitions.h"
/* --------------------------------------- */
int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    /* --------------------------------------- */
    auto config         = adapt::Configuration{FLAGS_config};
    auto local_node     = sys::Node{FLAGS_threads};
    node_t node_id      = local_node.get_id();
    /* --------------------------------------- */
    auto table          = Table{FLAGS_npages / FLAGS_nodes};
    auto& swips         = table.get_swips();
    /* --------------------------------------- */
    u32 num_pages_cache = ((FLAGS_random ? 100 : FLAGS_cache) * swips.size()) / 100u;
    auto cache          = Cache<PageTable>{num_pages_cache};
    table.populate_cache(cache, num_pages_cache, FLAGS_sequential_io);
    /* --------------------------------------- */
    auto current_swip                                   = std::atomic{0ul};
    auto pages_pre_agg                                  = std::atomic{0ul};
    auto times_preagg                                   = tbb::concurrent_vector<u64>(FLAGS_threads);
    /* --------------------------------------- */
    FLAGS_slots                                         = next_power_2(FLAGS_slots);
    FLAGS_partitions                                    = next_power_2(FLAGS_partitions);
    FLAGS_partgrpsz                                     = next_power_2(FLAGS_partgrpsz);
    auto partgrpsz_shift                                = __builtin_ctz(FLAGS_partgrpsz);
    auto npartgrps                                      = FLAGS_partitions >> partgrpsz_shift;
    auto npartgrps_shift                                = __builtin_ctz(npartgrps);
    auto [min_grps_per_dst, num_workers_with_extra_grp] = std::ldiv(npartgrps, FLAGS_nodes);
    /* --------------------------------------- */
    auto storage_glob                                   = StorageGlobal{FLAGS_consumepart ? (((npartgrps / FLAGS_nodes) + (node_id < (npartgrps % FLAGS_nodes))) << partgrpsz_shift) : 1};
    auto sketch_glob                                    = Sketch{};
    auto ht_glob                                        = HashtableGlobal{};
    auto npeers                                         = u32{FLAGS_nodes - 1};
    DEBUGGING(auto tuples_processed = std::atomic{0ul});
    DEBUGGING(auto tuples_sent = std::atomic{0ul});
    DEBUGGING(auto tuples_received = std::atomic{0ul});
    DEBUGGING(auto pages_recv = std::atomic{0ul});
    /* --------------------------------------- */
    auto barrier_query  = std::barrier{FLAGS_threads + 1};
    auto barrier_preagg = std::barrier{FLAGS_threads, [&] {
                                           ht_glob.initialize(next_power_2(static_cast<u64>(FLAGS_htfactor * sketch_glob.get_estimate())));
                                           // reset morsel
                                           current_swip = 0;
                                       }};
    /* --------------------------------------- */
    // create threads
    auto threads        = std::vector<std::jthread>{};
    for (u16 thread_id : range(FLAGS_threads)) {
        threads.emplace_back([=, &local_node, &current_swip, &swips, &table, &storage_glob, &barrier_query, &barrier_preagg, &ht_glob, &sketch_glob, &times_preagg,
                              &pages_pre_agg DEBUGGING(, &tuples_processed, &tuples_sent, &tuples_received, &pages_recv)] {
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
                auto port  = std::to_string(std::stoi(config.get_worker_info(node_id).port) + node_id * FLAGS_threads + thread_id);
                socket_fds = Connection::setup_ingress(port, node_id);
            }
            /* --------------------------------------- */
            // connect to [node_id + 1, FLAGS_nodes)
            for (node_t peer_id : range(node_id + 1u, FLAGS_nodes)) {
                auto [ip, port_base] = config.get_worker_info(peer_id);
                auto port            = std::to_string(std::stoi(port_base) + peer_id * FLAGS_threads + thread_id);
                socket_fds.emplace_back(Connection::setup_egress(node_id, ip, port));
            }
            /* --------------------------------------- */
            auto io_buffers = std::vector<PageTable>(defaults::local_io_depth);
            DEBUGGING(u64 local_tuples_processed{0});
            DEBUGGING(u64 local_tuples_sent{0});
            DEBUGGING(u64 local_tuples_received{0});
            /* --------------------------------------- */
            auto recv_alloc                 = BlockAlloc{1, npeers * 10, FLAGS_maxalloc};
            auto manager_recv               = IngressManager{npeers, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
            auto manager_send               = EgressManager{npeers, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
            auto remote_sketches_ingress    = std::vector<Sketch>(npeers);
            auto remote_sketches_egress     = std::vector<Sketch>(npeers);
            u32 peers_done                  = 0;
            /* --------------------------------------- */
            auto ingress_page_consumer_fn   = std::function{[&recv_alloc, &storage_glob, &remote_sketches_ingress, &manager_recv](PageResult* page, u32 dst) {
                if (page->is_primary_bit_set()) {
                    // recv sketch after last page
                    manager_recv.recv(dst, remote_sketches_ingress.data() + dst);
                    if (page->empty()) {
                        return;
                    }
                }
                else {
                    manager_recv.recv(dst, recv_alloc.get_object());
                }
                storage_glob.add_page(page, page->get_part_no());
            }};
            auto ingress_sketch_consumer_fn = std::function{[&sketch_glob, &peers_done](const Sketch* sketch, u32) {
                sketch_glob.merge_concurrent(*sketch);
                peers_done++;
            }};
            manager_recv.register_consumer_fn(ingress_page_consumer_fn);
            manager_recv.register_consumer_fn(ingress_sketch_consumer_fn);
            /* --------------------------------------- */
            // setup local uring manager
            auto thread_io = IO_Manager{FLAGS_depthio, FLAGS_sqpoll};
            if (not FLAGS_random and not FLAGS_path.empty()) {
                thread_io.register_files({table.get_file().get_file_descriptor()});
            }
            /* --------------------------------------- */
            // dependency injection
            auto eviction_fns = std::vector<BufferLocal::EvictionFn>(FLAGS_partitions);
            for (u64 grp_no : range(npartgrps)) {
                node_t dst              = (grp_no * FLAGS_nodes) >> npartgrps_shift;
                bool has_extra_grp      = dst < num_workers_with_extra_grp;
                auto grps_per_dst       = min_grps_per_dst + has_extra_grp;
                auto grp_offset         = (grps_per_dst * dst) + (has_extra_grp ? 0 : num_workers_with_extra_grp);
                bool is_final_grp       = (grp_no - grp_offset) == (grps_per_dst - 1);
                auto part_no_grp_offset = grp_no << partgrpsz_shift;
                for (u64 part_no : range(FLAGS_partgrpsz)) {
                    auto part_no_local  = FLAGS_consumepart ? (grp_no - grp_offset) * FLAGS_partgrpsz + part_no : 0;
                    auto part_no_global = part_no_grp_offset + part_no;
                    if (dst == node_id) {
                        eviction_fns[part_no_global] = [=, &storage_glob](PageResult* page, bool) {
                            if (not page->empty()) {
                                page->retire();
                                storage_glob.add_page(page, part_no_local);
                            }
                        };
                    }
                    else {
                        bool is_final_partition      = is_final_grp and (part_no == FLAGS_partgrpsz - 1);
                        auto actual_dst              = dst - (dst > node_id);
                        eviction_fns[part_no_global] = [=, &manager_send](PageResult* page, bool is_last = false) {
                            bool last_page_to_send = is_last and is_final_partition;
                            if (not page->empty() or last_page_to_send) {
                                page->retire();
                                page->set_part_no(part_no_local);
                                if (last_page_to_send) {
                                    page->set_primary_bit();
                                }
                                manager_send.send(actual_dst, page);
                            }
                        };
                    }
                }
            }
            /* --------------------------------------- */
            auto block_alloc                      = BlockAlloc{FLAGS_partitions, FLAGS_bump, FLAGS_maxalloc};
            auto partition_buffer                 = BufferLocal{FLAGS_partitions, block_alloc, eviction_fns};
            auto inserter_loc                     = InserterLocal{FLAGS_partitions, partition_buffer, npartgrps};
            auto ht_loc                           = HashtableLocal{FLAGS_partitions, FLAGS_slots, FLAGS_thresh, partition_buffer, inserter_loc};
            /* --------------------------------------- */
            std::function egress_page_consumer_fn = [&block_alloc](PageResult* pg) -> void { block_alloc.return_object(pg); };
            manager_send.register_consumer_fn(egress_page_consumer_fn);
            /* --------------------------------------- */
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
            std::function process_local_page = insert_into_ht;
            /* --------------------------------------- */
            auto process_page_glob           = [&ht_glob](PageResult& page) {
                for (auto j{0u}; j < page.get_num_tuples(); ++j) {
                    ht_glob.insert(page.get_tuple_ref(j));
                }
            };
            /* --------------------------------------- */
            // barrier
            barrier_query.arrive_and_wait();
            Stopwatch swatch_preagg{};
            swatch_preagg.start();
            for (node_t dst : range(npeers)) {
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
                if (FLAGS_cart and ht_loc.is_useless()) {
                    // turn off pre-aggregation
                    FLAGS_cart         = false;
                    process_local_page = insert_into_buffer;
                }
            }
            /* --------------------------------------- */
            partition_buffer.finalize(true);
            for (u32 grp_no : range(npartgrps)) {
                node_t dst = (grp_no * FLAGS_nodes) >> npartgrps_shift;
                if (dst == node_id) {
                    // merge local sketch
                    sketch_glob.merge_concurrent(inserter_loc.get_sketch(grp_no));
                }
                else {
                    // send remote sketches
                    auto actual_dst = dst - (dst > node_id);
                    remote_sketches_egress[actual_dst].merge(inserter_loc.get_sketch(grp_no));
                }
            }
            for (node_t peer_id : range(npeers)) {
                manager_send.send(peer_id, &remote_sketches_egress[peer_id]);
            }
            while (peers_done < npeers) {
                manager_recv.consume_done();
                manager_send.try_drain_pending();
            }
            manager_send.wait_all();
            swatch_preagg.stop();
            // need to wait for all threads to add their sketch contribution
            barrier_preagg.arrive_and_wait();
            /* --------------------------------------- */
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
            /* --------------------------------------- */
            times_preagg[thread_id] = swatch_preagg.time_ms;
            // barrier
            barrier_query.arrive_and_wait();
            /* --------------------------------------- */
            if (thread_id == 0) {
                if (FLAGS_consumepart) {
                    std::ranges::for_each(storage_glob.partition_pages.begin(), storage_glob.partition_pages.end(), [&pages_pre_agg](auto&& part_pgs) { pages_pre_agg += part_pgs.size(); });
                }
                else {
                    pages_pre_agg = storage_glob.partition_pages[0].size();
                }
            }
            /* --------------------------------------- */
            DEBUGGING(tuples_sent += local_tuples_sent);
            DEBUGGING(tuples_processed += local_tuples_processed);
            DEBUGGING(tuples_received += local_tuples_received);
            DEBUGGING(pages_recv += manager_recv.get_pages_recv());
        });
    }
    /* --------------------------------------- */
    Stopwatch swatch{};
    barrier_query.arrive_and_wait();
    swatch.start();
    barrier_query.arrive_and_wait();
    swatch.stop();
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
        .log("operator", "aggregation"s)
        .log("implementation", "homogeneous"s)
        .log("allocator", MemAlloc::get_type())
        .log("schema", get_schema_str<TABLE_SCHEMA>())
        .log("group keys", get_schema_str<GRP_KEYS>())
        .log("aggregation keys", get_schema_str<AGG_KEYS>())
        .log("page size (local)", defaults::local_page_size)
        .log("max tuples per page (local)", PageTable::max_tuples_per_page)
        .log("page size (hashtable)", defaults::hashtable_page_size)
        .log("max tuples per page (hashtable)", PageResult::max_tuples_per_page)
        .log("hashtable (local)", HashtableLocal::get_type())
        .log("hashtable (global)", HashtableGlobal::get_type())
        .log("sketch", Sketch::get_type())
        .log("block allocator", "partition-unaware"s)
#if defined(ENABLE_CART)
        .log("cart enabled", true)
#else
        .log("cart enabled", false)
#endif
#if defined(ENABLE_PREAGG)
        .log("pre-aggregation enabled", true)
#else
        .log("pre-aggregation enabled", false)
#endif
        .log("cart status", FLAGS_cart)
        .log("threshold pre-aggregation", FLAGS_thresh)
        .log("cache (%)", FLAGS_cache)
        .log("pin", FLAGS_pin)
        .log("morsel size", FLAGS_morselsz)
        .log("total pages", FLAGS_npages)
        .log("ht factor", FLAGS_htfactor)
        .log("partitions", FLAGS_partitions)
        .log("partition group size", FLAGS_partgrpsz)
        .log("slots", FLAGS_slots)
        .log("threads", FLAGS_threads)
        .log("groups seed", FLAGS_seed)
        .log("groups pool (actual)", FLAGS_groups)
        .log("groups node (estimate)", sketch_glob.get_estimate())
        .log("pages pre-agg", pages_pre_agg)
        .log("mean pre-agg time (ms)", std::reduce(times_preagg.begin(), times_preagg.end()) * 1.0 / times_preagg.size())
        .log("time (ms)", swatch.time_ms)                                                            //
        DEBUGGING(.log("local tuples processed", tuples_processed))                                  //
        DEBUGGING(.log("pages received", pages_recv))                                                //
        DEBUGGING(.log("local throughput (Gb/s)", (local_sz * 8 * 1000) / (1e9 * swatch.time_ms)))   //
        DEBUGGING(.log("network throughput (Gb/s)", (recv_sz * 8 * 1000) / (1e9 * swatch.time_ms))); //

    u64 count{0};
    u64 inserts{0};
    for (u64 i : range(ht_glob.size_mask + 1)) {
        if (auto slot = ht_glob.slots[i].load()) {
            auto slot_count  = std::get<0>(reinterpret_cast<HashtableGlobal::slot_idx_raw_t>(reinterpret_cast<uintptr_t>(slot) >> 16)->get_aggregates());
            count           += slot_count;
            inserts++;
        }
    }
    print("INSERTS:", inserts);
    print("COUNT:", count);
}
