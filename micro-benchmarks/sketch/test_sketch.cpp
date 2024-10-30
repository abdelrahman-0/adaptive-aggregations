#include "config.h"

int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    using entry_t = u64;

    union_sketches_t sketch_glob{};
    // control atomics
    ::pthread_barrier_t barrier_start{};
    ::pthread_barrier_t barrier_end{};
    ::pthread_barrier_init(&barrier_start, nullptr, FLAGS_threads + 1);
    ::pthread_barrier_init(&barrier_end, nullptr, FLAGS_threads + 1);

    // total number of groups should be constant
    auto total_grps = FLAGS_groups;
    FLAGS_groups /= FLAGS_threads;

    entry_t step = std::numeric_limits<entry_t>::max() / FLAGS_threads;

    // create threads
    std::vector<std::jthread> threads;
    for (u32 thread_id : range(FLAGS_threads)) {
        threads.emplace_back([=, &barrier_start, &barrier_end, &sketch_glob]() {
            std::vector<entry_t> values(FLAGS_n / FLAGS_threads);
            // populate vector with random values
            librand::random_iterable(values, thread_id * step, (thread_id + 1) * step - 1);

            sketch_t sketch_loc{};
            ::pthread_barrier_wait(&barrier_start);
            /////////////////////////
            for (auto v : values) {
                sketch_loc.update(v);
            }
            /////////////////////////
            sketch_glob.merge_concurrent(sketch_loc);
            ::pthread_barrier_wait(&barrier_end);
        });
    }

    // measure time
    Stopwatch swatch{};
    ::pthread_barrier_wait(&barrier_start);
    swatch.start();
    ::pthread_barrier_wait(&barrier_end);
    auto estimate = sketch_glob.get_estimate();
    swatch.stop();

    // destroy barriers
    ::pthread_barrier_destroy(&barrier_start);
    ::pthread_barrier_destroy(&barrier_end);

    Logger{FLAGS_print_header}
        .log("type", sketch_t::get_type())
        .log("total", FLAGS_n)
        .log("requested", total_grps)
        .log("estimate", estimate)
        .log("threads", FLAGS_threads)
        .log("time (ms)", swatch.time_ms);
}
