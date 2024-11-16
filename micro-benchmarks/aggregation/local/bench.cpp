#include "config.h"

int main(int argc, char* argv[])
{
    LIKWID_MARKER_INIT;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    FLAGS_partitions = next_power_2(FLAGS_partitions);
    FLAGS_slots = next_power_2(FLAGS_slots);

    sys::Node local_node{FLAGS_threads};

    /* ----------- DATA LOAD ----------- */

    // TODO cleanup -> refactor
    auto node_id = local_node.get_id();
    Table table{FLAGS_random};
    if (FLAGS_random) {
        table.prepare_random_swips(FLAGS_npages);
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
    u32 num_pages_cache = FLAGS_random ? ((FLAGS_cache * swips.size()) / 100u) : FLAGS_npages;
    Cache<PageTable> cache{num_pages_cache};
    table.populate_cache(cache, num_pages_cache, FLAGS_sequential_io);
    DEBUGGING(print("finished populating cache"));

    /* ----------- SETUP ----------- */

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

    tbb::concurrent_vector<u64> times_preagg;
    times_preagg.resize(FLAGS_threads);

    // create threads
    std::vector<std::jthread> threads{};
    for (u32 thread_id : range(FLAGS_threads)) {
        threads.emplace_back([=, &local_node, &current_swip, &swips, &table, &storage_glob, &barrier_start, &barrier_preagg, &barrier_end, &ht_glob, &sketch_glob,
                              &global_ht_construction_complete, &times_preagg, &pages_pre_agg DEBUGGING(, &tuples_processed)]() {
            if (FLAGS_pin) {
                local_node.pin_thread(thread_id);
            }
            LIKWID_MARKER_THREADINIT;
            LIKWID_MARKER_REGISTER("pre-aggregation");
            LIKWID_MARKER_REGISTER("concurrent aggregation");

            /* ----------- BUFFERS ----------- */

            //            StorageLocal storage_local;
            std::vector<PageTable> io_buffers(defaults::local_io_depth);
            DEBUGGING(u64 local_tuples_processed{0});

            /* ----------- LOCAL I/O ----------- */

            // setup local uring manager
            IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
            if (not FLAGS_random and not FLAGS_path.empty()) {
                thread_io.register_files({table.get_file().get_file_descriptor()});
            }

            /* ------------ GROUP BY ------------ */

            std::vector<BufferLocal::EvictionFn> eviction_fns(FLAGS_partitions);
            for (u32 part_no : range(FLAGS_partitions)) {
                auto final_part_no = FLAGS_consumepart ? part_no : 0;
                eviction_fns[part_no] = [final_part_no, &storage_glob](PageBuffer* page, bool) {
                    if (not page->empty()) {
                        page->retire();
                        storage_glob.add_page(page, final_part_no);
                    }
                };
            }

            BlockAlloc block_alloc(FLAGS_partitions * FLAGS_bump, FLAGS_maxalloc);
            BufferLocal partition_buffer{FLAGS_partitions, block_alloc, eviction_fns};
            InserterLocal inserter_loc{FLAGS_partitions, FLAGS_slots, 1u, partition_buffer};
            HashtableLocal ht_loc{FLAGS_partitions, FLAGS_slots, partition_buffer, inserter_loc};

            /* ------------ AGGREGATION LAMBDAS ------------ */

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

            auto process_page_glob = [&ht_glob](PageBuffer& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    ht_glob.insert(page.get_tuple_ref(j));
                }
            };

            // barrier
            ::pthread_barrier_wait(&barrier_start);
            LIKWID_MARKER_START("pre-aggregation");
            Stopwatch swatch_preagg{};
            swatch_preagg.start();

            /* ----------- BEGIN ----------- */

            // morsel loop
            u64 morsel_begin, morsel_end;
            const u64 nswips = swips.size();
            auto* swips_begin = swips.data();
            while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < nswips) {
                morsel_end = std::min(morsel_begin + FLAGS_morselsz, nswips);

                // partition swips such that unswizzled swips are at the beginning of the morsel
                auto swizzled_idx =
                    std::stable_partition(swips_begin + morsel_begin, swips_begin + morsel_end, [](const Swip& swip) { return !swip.is_pointer(); }) - swips_begin;

                // submit io requests before processing in-memory pages to overlap I/O with computation
                thread_io.batch_async_io<READ>(table.segment_id, std::span{swips_begin + morsel_begin, swips_begin + swizzled_idx}, io_buffers, true);

                // process swizzled pages
                while (swizzled_idx < morsel_end) {
                    process_local_page(*swips[swizzled_idx++].get_pointer<PageTable>());
                }

                // process unswizzled pages
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
            sketch_glob.merge_concurrent(inserter_loc.get_sketch(0));

            swatch_preagg.stop();
            LIKWID_MARKER_STOP("pre-aggregation");
            // barrier
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

            LIKWID_MARKER_START("concurrent aggregation");
            if (FLAGS_consumepart) {
                while ((morsel_begin = current_swip.fetch_add(1)) < FLAGS_partitions) {
                    for (auto* page : storage_glob.partition_pages[morsel_begin]) {
                        process_page_glob(*page);
                    }
                }
            }
            else {
                const u64 npages = storage_glob.partition_pages[0].size();
                while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < npages) {
                    morsel_end = std::min(morsel_begin + FLAGS_morselsz, npages);
                    while (morsel_begin < morsel_end) {
                        process_page_glob(*storage_glob.partition_pages[0][morsel_begin++]);
                    }
                }
            }

            times_preagg[thread_id] = swatch_preagg.time_ms;
            LIKWID_MARKER_STOP("concurrent aggregation");
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

            DEBUGGING(tuples_processed += local_tuples_processed);
        });
    }

    Stopwatch swatch{};
    {
        ::pthread_barrier_wait(&barrier_start);
        swatch.start();
        ::pthread_barrier_wait(&barrier_end);
        swatch.stop();
    }

    ::pthread_barrier_destroy(&barrier_start);
    ::pthread_barrier_destroy(&barrier_preagg);
    ::pthread_barrier_destroy(&barrier_end);

    Logger{FLAGS_print_header, FLAGS_csv}
        .log("traffic", "both"s)
        .log("operator", "aggregation"s)
        .log("implementation", "local"s)
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
        .log("threshold pre-aggregation", threshold_preagg)
        .log("cache (%)", FLAGS_cache)
        .log("pin", FLAGS_pin)
        .log("morsel size", FLAGS_morselsz)
        .log("total pages", FLAGS_npages)
        .log("ht factor", FLAGS_htfactor)
        .log("partitions", FLAGS_partitions)
        .log("slots", FLAGS_slots)
        .log("threads", FLAGS_threads)
        .log("groups total (actual)", FLAGS_groups)
        .log("groups node (estimate)", sketch_glob.get_estimate())
        .log("pages pre-agg", pages_pre_agg)
        .log("mean pre-agg time (ms)", std::reduce(times_preagg.begin(), times_preagg.end()) * 1.0 / times_preagg.size())
        .log("time (ms)", swatch.time_ms)                            //
        DEBUGGING(.log("local tuples processed", tuples_processed)); //

    LIKWID_MARKER_CLOSE;
}
