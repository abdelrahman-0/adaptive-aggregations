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
    bool random_table = FLAGS_random;
    Table table{random_table};
    if (random_table) {
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
        DEBUGGING(print("reading bytes:", offset_begin, "→", offset_end,
                        (offset_end - offset_begin) / defaults::local_page_size, "pages"));
    }

    auto& swips = table.get_swips();

    // prepare cache
    u32 num_pages_cache = random_table ? ((FLAGS_cache * swips.size()) / 100u) : FLAGS_npages;
    Cache<TablePage> cache{num_pages_cache};
    table.populate_cache(cache, num_pages_cache, FLAGS_sequential_io);

    /* ----------- THREAD SETUP ----------- */

    // control atomics
    ::pthread_barrier_t barrier_start{};
    ::pthread_barrier_t barrier_preagg{};
    ::pthread_barrier_t barrier_end{};
    ::pthread_barrier_init(&barrier_start, nullptr, FLAGS_threads + 1);
    ::pthread_barrier_init(&barrier_preagg, nullptr, FLAGS_threads);
    ::pthread_barrier_init(&barrier_end, nullptr, FLAGS_threads + 1);
    std::atomic<u64> current_swip{0};
    DEBUGGING(std::atomic<u64> tuples_processed{0});

    // create threads
    std::vector<std::jthread> threads{};
    for (auto thread_id{0u}; thread_id < FLAGS_threads; ++thread_id) {
        threads.emplace_back([=, &local_node, &current_swip, &swips, &table, &barrier_start, &barrier_preagg,
                              &barrier_end DEBUGGING(, &tuples_processed)]() {
            if (FLAGS_pin) {
                local_node.pin_thread(thread_id);
            }

            /* ----------- BUFFERS ----------- */

            PageBuffer<BufferPage> tuple_buffer;
            std::vector<TablePage> io_buffers(defaults::local_io_depth);
            DEBUGGING(u64 local_tuples_processed{0});

            /* ----------- LOCAL I/O ----------- */

            // setup local uring manager
            IO_Manager thread_io{FLAGS_depthio, FLAGS_sqpoll};
            if (not FLAGS_random and not FLAGS_path.empty()) {
                thread_io.register_files({table.get_file().get_file_descriptor()});
            }

            /* ------------ GROUP BY ------------ */

            std::vector<std::function<void(BufferPage*, bool)>> consumer_fns(FLAGS_partitions);
            std::fill(consumer_fns.begin(), consumer_fns.end(), [&tuple_buffer](BufferPage* pg, bool) {
                if (not pg->empty()) {
                    pg->retire();
                    tuple_buffer.add_page(pg);
                }
            });
            BlockAlloc block_alloc(FLAGS_partitions * FLAGS_bump, FLAGS_maxalloc);
            Buffer partition_buffer{FLAGS_partitions, block_alloc};
            HashtableLocal ht{FLAGS_partitions, FLAGS_slots, partition_buffer};

            /* ------------ LAMBDAS ------------ */

            auto process_local_page = [&ht DEBUGGING(, &local_tuples_processed)](const TablePage& page) {
                for (auto j{0u}; j < page.num_tuples; ++j) {
                    auto group = page.get_tuple<KEYS_IDX>(j);
                    auto agg = std::make_tuple<u64>(1);
                    ht.aggregate(group, agg);
                }
                DEBUGGING(local_tuples_processed += page.num_tuples);
            };

            // barrier
            ::pthread_barrier_wait(&barrier_start);

            /* ----------- BEGIN ----------- */

            // morsel loop
            u64 morsel_begin, morsel_end;
            while ((morsel_begin = current_swip.fetch_add(FLAGS_morselsz)) < swips.size()) {
                morsel_end = std::min(morsel_begin + FLAGS_morselsz, swips.size());

                // partition swips such that unswizzled swips are at the beginning of the morsel
                auto swizzled_idx = std::stable_partition(swips.data() + morsel_begin, swips.data() + morsel_end,
                                                          [](const Swip& swip) { return !swip.is_pointer(); }) -
                                    swips.data();

                // submit io requests before processing in-memory pages to overlap I/O with computation
                thread_io.batch_async_io<READ>(table.segment_id,
                                               std::span{swips.begin() + morsel_begin, swips.begin() + swizzled_idx},
                                               io_buffers, true);

                // process swizzled pages
                while (swizzled_idx < morsel_end) {
                    process_local_page(*swips[swizzled_idx++].get_pointer<TablePage>());
                }

                // process unswizzled pages
                while (thread_io.has_inflight_requests()) {
                    process_local_page(*thread_io.get_next_page<TablePage>());
                }
            }

            // barrier
            ::pthread_barrier_wait(&barrier_preagg);
            partition_buffer.finalize();
            // TODO global HT
            // TODO measure both pre-aggregation time and global time
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
        .log("tuples per local page", TablePage::max_tuples_per_page)
        .log("hashtable page size", defaults::hashtable_page_size)
        .log("tuples per hashtable page", BufferPage::max_tuples_per_page)
        .log("morsel size", FLAGS_morselsz)
        .log("pin", FLAGS_pin)
        .log("cache (%)", FLAGS_cache)
        .log("time (ms)", swatch.time_ms)                            //
        DEBUGGING(.log("local tuples processed", tuples_processed)); //
}
