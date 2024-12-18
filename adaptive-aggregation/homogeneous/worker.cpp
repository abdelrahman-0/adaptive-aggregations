#include "definitions.h"

int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    adapt::Configuration config{FLAGS_config};
    sys::Node node{FLAGS_threads};

    // generate data
    u16 node_id     = node.get_id();
    auto table      = Table{FLAGS_npages};
    auto& swips     = table.get_swips();
    auto cache      = Cache<PageTable>{swips.size()};
    auto npeers_max = u32{FLAGS_nodes - 1};
    table.populate_cache(cache, swips.size(), FLAGS_sequential_io);
    /* --------------------------------------- */
    FLAGS_slots          = next_power_2(FLAGS_slots);
    FLAGS_partitions     = next_power_2(FLAGS_partitions);
    FLAGS_partgrpsz      = next_power_2(FLAGS_partgrpsz);
    auto partgrpsz_shift = __builtin_ctz(FLAGS_partgrpsz);
    auto npartgrps       = FLAGS_partitions >> partgrpsz_shift;
    auto npartgrps_shift = __builtin_ctz(npartgrps);
    /* --------------------------------------- */
    auto current_swip    = std::atomic{0ul};
    auto times_preagg    = tbb::concurrent_vector<u64>(FLAGS_threads);
    auto storage_glob    = StorageGlobal{FLAGS_partitions};
    auto sketch_glob     = Sketch{};
    auto ht_glob         = HashtableGlobal{};
    // worker state
    auto task_metrics    = adapt::TaskMetrics{node_id, sizeof(Groups) + sizeof(Aggregates), npartgrps};
    auto task_scheduler  = adapt::TaskScheduler{FLAGS_morselsz, FLAGS_sla, task_metrics};
    /* --------------------------------------- */
    // monitor thread connects to coordinator
    std::latch coordinator_latch{1};
    auto monitor_thread = std::jthread{[node_id, &config, &coordinator_latch, &task_scheduler]() {
        auto [ip, port] = config.get_coordinator_info();
        auto conn_fds   = Connection::setup_egress(node_id, ip, port, 1);
        coordinator_latch.count_down();
        auto egress_network_manager  = network::HomogeneousEgressNetworkManager<adapt::StateMessage>{1, FLAGS_depthnw, FLAGS_sqpoll, conn_fds};
        auto ingress_network_manager = network::HomogeneousIngressNetworkManager<adapt::StateMessage>{1, FLAGS_depthnw, FLAGS_sqpoll, conn_fds};
        adapt::WorkerMonitor monitor{egress_network_manager, ingress_network_manager, task_scheduler};
        monitor.monitor_query();
        Connection::close_connections(conn_fds);
    }};
    coordinator_latch.wait();
    /* --------------------------------------- */
    // track if worker received any tasks
    bool is_active_worker = false;
    auto barrier_query    = std::barrier{FLAGS_threads + 1};
    auto barrier_task     = std::barrier{FLAGS_threads, [&is_active_worker, &task_scheduler, &current_swip] {
                                         is_active_worker |= task_scheduler.dequeue_task();
                                         current_swip      = 0;
                                     }};
    auto barrier_preagg   = std::barrier{FLAGS_threads, [&ht_glob, &sketch_glob, &current_swip] {
                                           ht_glob.initialize(next_power_2(static_cast<u64>(FLAGS_htfactor * sketch_glob.get_estimate())));
                                           current_swip = 0;
                                       }};
    // instantiate query threads
    std::vector<std::jthread> threads;
    for (u16 thread_id : range(FLAGS_threads)) {
        threads.emplace_back([=, &is_active_worker, &node, &config, &storage_glob, &sketch_glob, &ht_glob, &times_preagg, &task_metrics, &task_scheduler, &barrier_query, &barrier_preagg,
                              &barrier_task, &current_swip]() {
            // open max nodes connections, use config for ips and ports
            if (FLAGS_pin) {
                node.pin_thread(thread_id);
            }
            /* --------------------------------------- */
            // setup connections to each node, forming a logical clique topology
            // note that connections need to be setup in a particular order to avoid deadlocks!
            auto socket_fds = std::vector<int>{};
            /* --------------------------------------- */
            // accept from [0, node_id)
            if (node_id) {
                int port_base = std::stoi(config.get_worker_info(node_id).port);
                socket_fds    = Connection::setup_ingress(std::to_string(port_base + node_id * FLAGS_threads + thread_id), node_id);
            }
            /* --------------------------------------- */
            // connect to [node_id + 1, FLAGS_nodes)
            for (u16 peer : range(node_id + 1u, FLAGS_nodes)) {
                auto [ip, port_base] = config.get_worker_info(peer);
                auto port            = std::to_string(std::stoi(port_base) + peer * FLAGS_threads + thread_id);
                socket_fds.emplace_back(Connection::setup_egress(node_id, ip, port));
            }
            /* --------------------------------------- */
            auto page_alloc_ingress      = BlockAlloc{npeers_max * 10, FLAGS_maxalloc};
            auto manager_recv            = IngressManager{npeers_max, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
            auto manager_send            = EgressManager{npeers_max, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
            auto remote_sketches_ingress = std::vector<Sketch>(npeers_max);
            auto remote_sketches_egress  = std::vector<Sketch>(npeers_max);
            auto storage_loc             = StorageLocal{FLAGS_partitions};
            node_t sketches_seen         = 0;
            node_t peers_ready           = 0;
            /* --------------------------------------- */
            auto ingress_page_consumer_fn =
                std::function{[&page_alloc_ingress, &storage_loc, &storage_glob, &remote_sketches_ingress, &manager_recv, &peers_ready](PageResult* page, u32 dst) {
                    if (page->is_secondary_bit_set()) {
                        // phase 1
                        if (page->is_primary_bit_set()) {
                            manager_recv.recv(dst, remote_sketches_ingress.data() + dst);
                        }
                        else {
                            manager_recv.recv(dst, page_alloc_ingress.get_object());
                        }
                        if (not page->empty()) {
                            // contains local part_no
                            storage_glob.add_page(page, page->get_part_no());
                        }
                    }
                    else {
                        // phase 0
                        manager_recv.recv(dst, page_alloc_ingress.get_object());
                        peers_ready += page->is_primary_bit_set();
                        if (not page->empty()) {
                            // contains global part_no
                            storage_loc.add_page(page, page->get_part_no());
                        }
                    }
                }};
            auto ingress_sketch_consumer_fn = std::function{[&sketch_glob, &sketches_seen](const Sketch* sketch, u32) {
                sketch_glob.merge_concurrent(*sketch);
                sketches_seen++;
            }};
            manager_recv.register_consumer_fn(ingress_page_consumer_fn);
            manager_recv.register_consumer_fn(ingress_sketch_consumer_fn);
            /* --------------------------------------- */
            // dependency injection
            std::vector<node_t> partitions_begin;
            std::vector<node_t> partitions_end;
            auto eviction_fns = std::vector<BufferLocal::EvictionFn>(FLAGS_partitions);
            for (auto part_no_glob : range(FLAGS_partitions)) {
                auto grp_no                = part_no_glob >> partgrpsz_shift;
                // calculate destination on each eviction
                eviction_fns[part_no_glob] = [=, &task_scheduler, &storage_loc, &manager_send, &partitions_end, &task_metrics](PageResult* page, bool is_last) {
                    // defer last_page
                    auto current_workers = task_scheduler.nworkers.load();
                    node_t dst           = (grp_no * current_workers) >> npartgrps_shift;
                    page->retire();
                    page->set_part_no(part_no_glob);
                    auto actual_dst        = dst - (dst > node_id);
                    auto last_page_to_send = is_last and (part_no_glob == partitions_end[dst]);
                    if (dst == node_id) {
                        if (not page->empty()) {
                            // add to local storage since number of workers might change
                            storage_loc.add_page(page, part_no_glob);
                        }
                    }
                    else if (not page->empty() or last_page_to_send) {
                        if (last_page_to_send) {
                            page->set_primary_bit();
                        }
                        manager_send.send(actual_dst, page);
                    }
                    task_metrics.tuples_produced += PageResult::max_tuples_per_page;
                };
            }

            /* --------------------------------------- */
            auto page_alloc_egress = BlockAlloc{FLAGS_partitions * FLAGS_bump, FLAGS_maxalloc};
            auto partition_buffer  = BufferLocal{FLAGS_partitions, page_alloc_egress, eviction_fns};
            auto inserter_loc      = InserterLocal{FLAGS_partitions, partition_buffer, npartgrps};
            auto ht_loc            = HashtableLocal{FLAGS_partitions, FLAGS_slots, FLAGS_thresh, partition_buffer, inserter_loc};
            /* --------------------------------------- */
            auto insert_into_ht    = [&ht_loc](const PageTable& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    auto group = page.get_tuple<GPR_KEYS_IDX>(j);
                    auto agg   = std::make_tuple<AGG_KEYS>(AGG_VALS);
                    ht_loc.insert(group, agg);
                }
            };
            auto insert_into_buffer = [&inserter_loc](const PageTable& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    auto group = page.get_tuple<GPR_KEYS_IDX>(j);
                    auto agg   = std::make_tuple<AGG_KEYS>(AGG_VALS);
                    inserter_loc.insert(group, agg);
                }
            };
            std::function process_local_page = insert_into_ht;
            /* --------------------------------------- */
            auto process_page_glob           = [&ht_glob](PageResult& page) {
                for (auto j{0u}; j < page.get_num_tuples(); ++j) {
                    ht_glob.insert(page.get_tuple_ref(j));
                }
            };
            barrier_query.arrive_and_wait();
            Stopwatch swatch_preagg{};
            swatch_preagg.start();
            for (node_t dst : range(npeers_max)) {
                manager_recv.recv(dst, page_alloc_ingress.get_object());
            }
            // wait for query end
            while (not task_scheduler.is_done()) {
                // one task per iteration
                barrier_task.arrive_and_wait();
                while (adapt::Task morsel = task_scheduler.get_next_morsel()) {
                    while (morsel.start < morsel.end) {
                        process_local_page(*swips[morsel.start++].get_pointer<PageTable>());
                    }
                    if (FLAGS_adapre and ht_loc.is_useless()) {
                        // turn off pre-aggregation
                        FLAGS_adapre       = false;
                        process_local_page = insert_into_buffer;
                    }
                    for (u32 grp_id{0}; grp_id < npartgrps; ++grp_id) {
                        task_metrics.merge_sketch(grp_id, inserter_loc.get_sketch(grp_id));
                    }
                }
                manager_recv.consume_done();
                manager_send.try_drain_pending();
            }

            node_t nworkers_final, npeers_final;
            u64 first_partition, npartitions_node, groups_per_worker, extra_groups;
            ldiv_t div_result;
            if (not is_active_worker) {
                goto finished_query_processing;
            }
            // finalize buffer
            nworkers_final = task_scheduler.nworkers.load();
            npeers_final   = nworkers_final - 1;

            // compute last partitions
            partitions_begin.reserve(nworkers_final);
            partitions_end.reserve(nworkers_final);
            div_result        = std::ldiv(npartgrps, nworkers_final);
            groups_per_worker = div_result.quot;
            extra_groups      = div_result.rem;
            for (node_t worker_no : range(nworkers_final)) {
                bool has_extra_grp = worker_no < extra_groups;
                u16 first_group    = worker_no * groups_per_worker + (has_extra_grp ? worker_no : extra_groups);
                auto first_part_no = (first_group) << partgrpsz_shift;
                auto last_part_no  = ((first_group + groups_per_worker + has_extra_grp) << partgrpsz_shift) - 1;
                partitions_begin.push_back(first_part_no);
                partitions_end.push_back(last_part_no);
            }

            partition_buffer.finalize(true);
            while (peers_ready < npeers_final) {
                // ping pong
                manager_recv.consume_done();
                manager_send.try_drain_pending();
            }

            // loop through partitions and send out what not mine (and set secondary bit and primary bit), and add to storage_glob what is mine
            for (node_t part_no_glob : range(FLAGS_partitions)) {
                auto grp_no         = part_no_glob >> partgrpsz_shift;
                auto dst            = (grp_no * nworkers_final) >> npartgrps_shift;
                bool sent_last_page = false;
                auto actual_dst     = dst - (dst > node_id);
                bool is_last_part   = part_no_glob == partitions_end[dst];
                for (auto& part_page : storage_loc.partition_pages[part_no_glob]) {
                    if (dst == node_id) {
                        storage_glob.add_page(part_page, part_no_glob);
                    }
                    else {
                        part_page->set_secondary_bit();
                        if (is_last_part) {
                            part_page->set_primary_bit();
                            sent_last_page = true;
                        }
                        manager_send.send(actual_dst, part_page);
                    }
                }
                if (dst != node_id and is_last_part and not sent_last_page) {
                    // send empty last page
                    auto* empty_page = page_alloc_egress.get_object();
                    empty_page->set_secondary_bit();
                    empty_page->set_primary_bit();
                    manager_send.send(actual_dst, empty_page);
                }
                manager_recv.consume_done();
                manager_send.try_drain_pending();
            }
            // merge and send sketches
            for (u32 grp_no : range(npartgrps)) {
                node_t dst = (grp_no * nworkers_final) >> npartgrps_shift;
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
            for (node_t peer_id : range(npeers_final)) {
                manager_send.send(peer_id, &remote_sketches_egress[peer_id]);
            }
            // send and recv sketch
            while (sketches_seen < npeers_final) {
                // ping pong
                manager_recv.consume_done();
                manager_send.try_drain_pending();
            }

            // wait all
            manager_send.wait_all();

            swatch_preagg.stop();
            barrier_preagg.arrive_and_wait();

            // build global ht
            first_partition  = partitions_begin[node_id];
            npartitions_node = partitions_end[node_id] - first_partition + 1;
            u64 swip_offset;
            while ((swip_offset = current_swip.fetch_add(1)) < npartitions_node) {
                for (auto* page : storage_glob.partition_pages[first_partition + swip_offset]) {
                    process_page_glob(*page);
                }
            }
        finished_query_processing:;
            times_preagg[thread_id] = swatch_preagg.time_ms;
            barrier_query.arrive_and_wait();
        });
    }

    Stopwatch swatch{};
    barrier_query.arrive_and_wait();
    swatch.start();
    barrier_query.arrive_and_wait();
    swatch.stop();

    Logger{FLAGS_print_header, FLAGS_csv}
        .log("node id", node_id)
        .log("nodes", FLAGS_nodes)
        .log("traffic", "both"s)
        .log("operator", "aggregation"s)
        .log("implementation", "adaptive homogeneous"s)
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
        .log("consume partitions", FLAGS_consumepart)
        .log("adaptive pre-aggregation", FLAGS_adapre)
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
        .log("SLA", FLAGS_sla)
        .log("groups seed", FLAGS_seed)
        .log("groups total (actual)", FLAGS_groups)
        .log("groups node (estimate)", sketch_glob.get_estimate())
        .log("mean pre-agg time (ms)", static_cast<u64>(std::reduce(times_preagg.begin(), times_preagg.end()) * 1.0 / times_preagg.size()))
        .log("time (ms)", swatch.time_ms);

    if (is_active_worker) {
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
}
