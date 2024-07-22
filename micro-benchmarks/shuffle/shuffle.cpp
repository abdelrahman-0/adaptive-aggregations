#include <gflags/gflags.h>
#include <tbb/enumerable_thread_specific.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>
#include <thread>

#include "defaults.h"
#include "network/connection.h"
#include "network/network_manager.h"
#include "storage/chunked_list.h"
#include "storage/policy.h"
#include "storage/table.h"
#include "utils/hash.h"
#include "utils/stopwatch.h"
#include "utils/utils.h"

// table schema
#define SCHEMA int64_t, int64_t, int32_t, std::array<unsigned char, 4>

using TablePage = PageLocal<SCHEMA>;
using ResultTuple = std::tuple<SCHEMA>;
using ResultPage = PageLocal<ResultTuple>;
using NetworkPage = PageCommunication<ResultTuple>;

// cmd line params
DEFINE_string(path, "data/random.tbl", "path to input relation");
DEFINE_double(cache, 1.0, "percentage of table to cache in-memory in [0.0,1.0]");
DEFINE_int32(nodes, 2, "total number of nodes to use (additional nodes are only used if policy spills)");
DEFINE_bool(random, false, "randomize order of cached swips");

// auto g = tbb::global_control(tbb::global_control::max_allowed_parallelism, 1);

struct TLS {
    IO_Manager io{};
    NetworkManager network;
    std::vector<TablePage> buffer{defaults::local_io_depth};
    ChunkedList<ResultPage> result{};

    explicit TLS(const Connection& conn) : network(conn) {}
};

int main() {

    using namespace std::chrono_literals;

    // setup conn_egress for spilling
    auto num_egress = FLAGS_nodes - 1;
    Connection conn_egress{num_egress};
    conn_egress.setup_egress();

    // prepare local IO
    Table table{File{FLAGS_path, FileMode::READ}};
    auto& swips = table.get_swips();
    IO_Manager io{};

    // prepare cache
    auto num_pages_cache = static_cast<std::size_t>(FLAGS_cache * swips.size());
    assert(num_pages_cache >= 0 and num_pages_cache <= swips.size());
    Cache cache{num_pages_cache};
    table.populate_cache(cache, io, num_pages_cache, FLAGS_random);

    // TLS resources
    {
        tbb::enumerable_thread_specific<TLS> tls{conn_egress};
        // multi-threaded processing
        Stopwatch _{};

        tbb::parallel_for(
            tbb::blocked_range<std::size_t>{0, swips.size()},
            [&](const tbb::blocked_range<std::size_t>& range) {
                auto middle = std::stable_partition(&swips[range.begin()], &swips[range.end()],
                                                    [](const Swip& swip) { return !swip.is_pointer(); });
                auto middle_idx = middle - &*swips.begin();

                auto& thread_io = tls.local().io;
                auto& thread_result = tls.local().result;
                auto& thread_network = tls.local().network.traffic;
                auto& thread_buffer = tls.local().buffer;

                ResultPage* current_result_page{nullptr};
                if (thread_result.current_page_full()) {
                    current_result_page = thread_result.get_new_page();
                } else {
                    current_result_page = thread_result.get_current_page();
                }

                // submit io requests
                for (auto i = range.begin(); i < middle_idx; ++i) {
                    table.read_page(thread_io, swips[i].get_page_index(),
                                    reinterpret_cast<std::byte*>(thread_buffer.data() + i - range.begin()));
                }

                // policy check (could be once-per-morsel, once-per-page or once-per-tuple)
                bool spill = PolicyAlwaysSpill::spill();

                auto processed_in_memory = middle_idx;
                bool wait_for_cqes = false;
                while (processed_in_memory < range.end() || (wait_for_cqes = thread_io.has_inflight_requests())) {
                    TablePage* page_to_process{nullptr};
                    if (wait_for_cqes) {
                        // process completion events
                        page_to_process = thread_io.get_next_cqe_data<decltype(page_to_process)>();
                    } else {
                        // process in-memory pages
                        page_to_process = swips[processed_in_memory++].get_pointer<decltype(page_to_process)>();
                    }
                    if (page_to_process) {
                        if (!spill) {
                            // local
                            for (auto j = 0u; j < page_to_process->num_tuples; ++j) {
                                // materialize into thread-local pages
                                if (current_result_page->is_full()) {
                                    current_result_page = thread_result.get_new_page();
                                }
                                current_result_page->emplace_back_transposed(j, *page_to_process);
                            }
                        } else {
                            for (auto j = 0u; j < page_to_process->num_tuples; ++j) {
                                // spill
                                auto dst = murmur_hash(std::get<0>(page_to_process->columns)[j]) % FLAGS_nodes;

                                // send or materialize into thread-local buffers
                                // if (dst == num_egress) {
                                if (false) {
                                    // last partition is local
                                    if (current_result_page->is_full()) {
                                        current_result_page = thread_result.get_new_page();
                                    }
                                    current_result_page->emplace_back_transposed(j, *page_to_process);
                                } else {
                                    // send to receiver via NetworkManager
                                    auto dst_page = thread_network.get_page<NetworkPage>(0);
                                    dst_page->emplace_back_transposed(j, *page_to_process);
                                }
                            }
                        }
                    }
                }
            },
            defaults::shuffle_partitioner);
        // drain comm buffers
        if (num_egress) {
            for (auto& thread_tls : tls) {
                thread_tls.network.traffic.flush_all<NetworkPage>();
            }
        }
    }
    println("Sent", sent_pages, "pages");
    println("Sent", sent_tuples, "tuples");
    println("Sent", sent_bytes, "bytes");
}
