#include "definitions.h"

int main(int argc, char* argv[])
{
    // LIKWID_MARKER_INIT;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    sys::Node local_node{FLAGS_threads};

    /* ----------- DATA LOAD ----------- */

    Table table{FLAGS_npages};
    auto& swips         = table.get_swips();

    // prepare cache
    u32 num_pages_cache = ((FLAGS_random ? 100 : FLAGS_cache) * swips.size()) / 100u;
    Cache<PageTable> cache{num_pages_cache};
    table.populate_cache(cache, num_pages_cache);
    /* --------------------------------------- */
    FLAGS_partitions = next_power_2(FLAGS_partitions);
    FLAGS_slots      = next_power_2(FLAGS_slots);
    std::atomic<u64> current_swip{0};
    std::atomic<u64> pages_pre_agg{0};
    std::atomic<u64> tuple_count{0};
    std::atomic<u64> groups_inserted{0};
    std::atomic global_ht_construction_complete{false};
#if defined(ENABLE_RADIX)
    StorageGlobal storage_glob{FLAGS_partitions};
#else
    StorageGlobal storage_glob{1};
#endif
    Sketch sketch_glob;
    HashtableGlobal ht_glob;
    /* --------------------------------------- */
    // control atomics
    auto barrier_query  = std::barrier{FLAGS_threads + 1};
    auto barrier_preagg = std::barrier{FLAGS_threads, [&ht_glob, &sketch_glob, &current_swip] {
                                           u64 ht_size = next_power_2(static_cast<u64>(FLAGS_htfactor * sketch_glob.get_estimate()));
                                           ht_glob.initialize(ht_size);
                                           // reset morsel
                                           current_swip = 0;
                                       }};
    tbb::concurrent_vector<u64> times_preagg(FLAGS_threads);
    /* --------------------------------------- */
    // create threads
    std::vector<std::jthread> threads{};
    for (u32 thread_id : range(FLAGS_threads)) {
        threads.emplace_back([=, &local_node, &current_swip, &swips, &table, &storage_glob, &barrier_query, &barrier_preagg, &ht_glob, &sketch_glob, &global_ht_construction_complete,
                              &times_preagg, &pages_pre_agg, &tuple_count, &groups_inserted] {
            if (FLAGS_pin) {
                local_node.pin_thread(thread_id);
            }
            // LIKWID_MARKER_THREADINIT;
            // LIKWID_MARKER_REGISTER("pre-aggregation");
            // LIKWID_MARKER_REGISTER("concurrent aggregation");

            /* ----------- BUFFERS ----------- */

            //            StorageLocal storage_local;
            std::vector<PageTable> io_buffers(defaults::local_io_depth);

            /* ----------- LOCAL I/O ----------- */

            // setup local uring manager
            IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
            if (not FLAGS_random and not FLAGS_path.empty()) {
                thread_io.register_files({table.get_file().get_file_descriptor()});
            }

            /* ------------ GROUP BY ------------ */

            std::vector<BufferLocal::EvictionFn> eviction_fns(FLAGS_partitions);
            for (u32 part_no : range(FLAGS_partitions)) {
#if defined(ENABLE_RADIX)
                auto final_part_no = part_no;
#else
                auto final_part_no = 0;
#endif
                eviction_fns[part_no] = [final_part_no, &storage_glob](PageResult* page, bool) {
                    page->retire();
                    if (not page->empty()) {
                        storage_glob.add_page(page, final_part_no);
                    }
                };
            }

            BlockAlloc block_alloc(FLAGS_partitions, FLAGS_bump, FLAGS_maxalloc);
            BufferLocal partition_buffer{FLAGS_partitions, block_alloc, eviction_fns};
            InserterLocal inserter_loc{FLAGS_partitions, partition_buffer};
            HashtableLocal ht_loc{FLAGS_partitions, FLAGS_slots, FLAGS_thresh, partition_buffer, inserter_loc};

            /* ------------ AGGREGATION LAMBDAS ------------ */

            auto insert_into_ht = [&ht_loc](const PageTable& page) {
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
#if defined(ENABLE_RADIX)
                    inserter_loc.insert(group, agg);
#else
                    inserter_loc.insert<true>(group, agg);
#endif
                }
            };
#if defined(ENABLE_PREAGG)
#if not defined(ENABLE_CART)
            const
#endif
                std::function process_local_page = insert_into_ht;
#else
            const std::function process_local_page = insert_into_buffer;
#endif
            auto process_page_glob = [&ht_glob](PageResult& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    ht_glob.insert(page.get_tuple_ref(j));
                }
            };

            // barrier
            Stopwatch swatch_preagg{};
            barrier_query.arrive_and_wait();
            // LIKWID_MARKER_START("pre-aggregation");
            swatch_preagg.start();
            {
#if defined(ENABLE_PERFEVENT)
                PerfEventBlock perf;
#endif
                /* ----------- BEGIN ----------- */

                // morsel loop
                u64 morsel_begin, morsel_end;
                const u64 nswips  = swips.size();
                auto* swips_begin = swips.data();
                while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < nswips) {
                    morsel_end        = std::min(morsel_begin + FLAGS_morselsz, nswips);

                    // partition swips such that unswizzled swips are at the beginning of the morsel
                    auto swizzled_idx = std::stable_partition(swips_begin + morsel_begin, swips_begin + morsel_end, [](const Swip& swip) { return !swip.is_pointer(); }) - swips_begin;

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
#if defined(ENABLE_CART)
                    if (FLAGS_cart and ht_loc.is_useless()) {
                        // turn off pre-aggregation
                        FLAGS_cart         = false;
                        process_local_page = insert_into_buffer;
                    }
#endif
                }
                partition_buffer.finalize();
                sketch_glob.merge_concurrent(inserter_loc.get_sketch());

                swatch_preagg.stop();
                // LIKWID_MARKER_STOP("pre-aggregation");
                // barrier
                barrier_preagg.arrive_and_wait();

                // LIKWID_MARKER_START("concurrent aggregation");
#if defined(ENABLE_RADIX)
                while ((morsel_begin = current_swip.fetch_add(1)) < FLAGS_partitions) {
                    for (auto* page : storage_glob.partition_pages[morsel_begin]) {
                        process_page_glob(*page);
                    }
                }
#else
                const u64 npages = storage_glob.partition_pages[0].size();
                while ((morsel_begin = current_swip.fetch_add(100)) < npages) {
                    morsel_end = std::min(morsel_begin + FLAGS_morselsz, npages);
                    while (morsel_begin < morsel_end) {
                        process_page_glob(*storage_glob.partition_pages[0][morsel_begin++]);
                    }
                }
#endif

                times_preagg[thread_id] = swatch_preagg.time_ms;
                // LIKWID_MARKER_STOP("concurrent aggregation");
                // barrier
                barrier_query.arrive_and_wait();
            }

            /* ----------- END ----------- */
            if (thread_id == 0) {
#if defined(ENABLE_RADIX)
                std::for_each(storage_glob.partition_pages.begin(), storage_glob.partition_pages.end(), [&pages_pre_agg](auto&& part_pgs) { pages_pre_agg += part_pgs.size(); });
#else
                pages_pre_agg = storage_glob.partition_pages[0].size();
#endif

#if defined(GLOBAL_UNCHAINED_HT)
                for (u64 i : range(ht_glob.size_mask + 1)) {
                    if (auto slot = ht_glob.slots[i].load()) {
                        auto slot_count = std::get<0>(reinterpret_cast<HashtableGlobal::slot_idx_raw_t>(reinterpret_cast<uintptr_t>(slot) >> (is_ht_glob_salted ? 16 : 0))->get_aggregates());
                        tuple_count += slot_count;
                        ++groups_inserted;
                    }
                }
#else
                for (u64 i : range(ht_glob.size_mask + 1)) {
                    auto slot = ht_glob.slots[i].load();
                    while (slot) {
                        auto slot_count  = std::get<0>(slot->get_aggregates());
                        tuple_count     += slot_count;
                        ++groups_inserted;
                        slot = slot->get_next();
                    }
                }
#endif
            }
            barrier_query.arrive_and_wait();
        });
    }

    Stopwatch swatch{};
    barrier_query.arrive_and_wait();
    swatch.start();
    barrier_query.arrive_and_wait();
    swatch.stop();
    barrier_query.arrive_and_wait();

    print("GROUPS:", groups_inserted.load());
    print("TUPLES:", tuple_count.load());

    Logger{FLAGS_print_header, FLAGS_csv}
        .log("traffic", "both"s)
        .log("operator", "aggregation"s)
        .log("implementation", "local"s)
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
#if defined(ENABLE_RADIX)
        .log("consume partitions", true)
#else
        .log("consume partitions", false)
#endif
#if defined(ENABLE_PART_BLOCK)
        .log("block allocator", "partition-aware"s)
#else
        .log("block allocator", "partition-unaware"s)
#endif
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
        .log("slots", FLAGS_slots)
        .log("threads", FLAGS_threads)
        .log("groups pool (actual)", FLAGS_groups)
        .log("groups node (actual)", groups_inserted)
        .log("groups node (estimate)", sketch_glob.get_estimate())
        .log("pages pre-agg", pages_pre_agg)
        .log("mean pre-agg time (ms)", std::reduce(times_preagg.begin(), times_preagg.end()) * 1.0 / FLAGS_threads)
        .log("time (ms)", swatch.time_ms);

    // LIKWID_MARKER_CLOSE;
}
