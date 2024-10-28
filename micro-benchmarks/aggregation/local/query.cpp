#include <span>
#include <thread>

#include "config.h"

int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    FLAGS_partitions = next_power_2(FLAGS_partitions);
    FLAGS_slots = next_power_2(FLAGS_slots);

    sys::Node local_node{FLAGS_threads};

    /* ----------- DATA LOAD ----------- */

    // TODO cleanup -> refactor
    auto npeers = sys::Node::get_npeers();
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
    u32 num_pages_cache = FLAGS_random ? ((FLAGS_cache * swips.size()) / 100u) : FLAGS_npages;
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
    StorageGlobal storage_global;
    HashtableGlobal ht_global{FLAGS_partitions, FLAGS_slots};
    DEBUGGING(std::atomic<u64> tuples_processed{0});

    // create threads
    std::vector<std::jthread> threads{};
    for (auto thread_id{0u}; thread_id < FLAGS_threads; ++thread_id) {
        threads.emplace_back([=, &local_node, &current_swip, &swips, &table, &storage_global, &barrier_start, &barrier_preagg, &barrier_end,
                              &ht_global, &global_ht_construction_complete DEBUGGING(, &tuples_processed)]() {
            if (FLAGS_pin) {
                local_node.pin_thread(thread_id);
            }

            /* ----------- BUFFERS ----------- */

            StorageLocal storage_local;
            std::vector<PageTable> io_buffers(defaults::local_io_depth);
            DEBUGGING(u64 local_tuples_processed{0});

            /* ----------- LOCAL I/O ----------- */

            // setup local uring manager
            IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
            if (not FLAGS_random and not FLAGS_path.empty()) {
                thread_io.register_files({table.get_file().get_file_descriptor()});
            }

            /* ------------ GROUP BY ------------ */

            std::vector<Buffer::ConsumerFn> consumer_fns(FLAGS_partitions);
            std::fill(consumer_fns.begin(), consumer_fns.end(), [&storage_global](PageHashtable* pg, bool) {
                if (not pg->empty()) {
                    pg->retire();
                    storage_global.add_page(pg);
                }
            });
            BlockAlloc block_alloc(FLAGS_partitions * FLAGS_bump, FLAGS_maxalloc);
            Buffer partition_buffer{FLAGS_partitions, block_alloc, consumer_fns};
            HashtableLocal ht{FLAGS_partitions, FLAGS_slots, partition_buffer};

            /* ------------ AGGREGATION LAMBDAS ------------ */

            auto process_page_local = [&ht DEBUGGING(, &local_tuples_processed)](const PageTable& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    auto group = page.get_tuple<GPR_KEYS_IDX>(j);
                    auto agg = std::make_tuple<AGG_KEYS>(AGG_VAL);
                    ht.aggregate(group, agg);
                }
                DEBUGGING(local_tuples_processed += page.num_tuples);
            };

            auto process_page_global = [&ht_global](PageHashtable& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    ht_global.aggregate(page.get_tuple_ref(j));
                }
            };

            // barrier
            ::pthread_barrier_wait(&barrier_start);

            /* ----------- BEGIN ----------- */

            // morsel loop
            u64 morsel_begin, morsel_end;
            const u64 nswips = swips.size();
            auto* swips_begin = swips.data();
            while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < nswips) {
                morsel_end = std::min(morsel_begin + FLAGS_morselsz, nswips);

                // partition swips such that unswizzled swips are at the beginning of the morsel
                auto swizzled_idx =
                    std::stable_partition(swips_begin + morsel_begin, swips_begin + morsel_end, [](const Swip& swip) { return !swip.is_pointer(); }) -
                    swips.data();

                // submit io requests before processing in-memory pages to overlap I/O with computation
                thread_io.batch_async_io<READ>(table.segment_id, std::span{swips_begin + morsel_begin, swips_begin + swizzled_idx}, io_buffers, true);

                // process swizzled pages
                while (swizzled_idx < morsel_end) {
                    process_page_local(*swips[swizzled_idx++].get_pointer<PageTable>());
                }

                // process unswizzled pages
                while (thread_io.has_inflight_requests()) {
                    process_page_local(*thread_io.get_next_page<PageTable>());
                }
            }
            partition_buffer.finalize();

            // barrier
            ::pthread_barrier_wait(&barrier_preagg); // TODO relax this barrier? (90% threads done? -> scheduler)
//            // TODO measure both pre-aggregation time and global time
//            if (thread_id == 0) {
//                ht_global.initialize(FLAGS_partitions, next_power_2(static_cast<u64>(1.7 * storage_global.num_tuples)));
//                // reset morsel
//                current_swip = 0;
//                global_ht_construction_complete = true;
//                global_ht_construction_complete.notify_all();
//            }
//            else {
//                global_ht_construction_complete.wait(false);
//            }
//
//            const u64 npages = storage_global.pages.size();
//            while ((morsel_begin = current_swip.fetch_add(1)) < npages) {
//                morsel_end = std::min(morsel_begin + 1, npages);
//                while (morsel_begin < morsel_end) {
//                    process_page_global(*storage_global.pages[morsel_begin++]);
//                }
//            }

            // barrier
            ::pthread_barrier_wait(&barrier_end);

            /* ----------- END ----------- */

            DEBUGGING(tuples_processed += local_tuples_processed);
        });
    }

    Stopwatch swatch{};
    ::pthread_barrier_wait(&barrier_start);
    swatch.start();
    ::pthread_barrier_wait(&barrier_end);
    swatch.stop();

    ::pthread_barrier_destroy(&barrier_start);
    ::pthread_barrier_destroy(&barrier_preagg);
    ::pthread_barrier_destroy(&barrier_end);

    Logger{FLAGS_print_header}
        .log("node id", node_id)
        .log("nodes", FLAGS_nodes)
        .log("traffic", "both"s)
        .log("operator", "aggregation"s)
        .log("implementation", "local"s)
        .log("hashtable local", HashtableLocal::get_type())
        .log("threads", FLAGS_threads)
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
        .log("time (ms)", swatch.time_ms)                            //
        DEBUGGING(.log("local tuples processed", tuples_processed)); //
}
