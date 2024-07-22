#include <gflags/gflags.h>

#include "defaults.h"
#include "storage/file.h"
#include "storage/io_manager.h"
#include "storage/page.h"
#include "utils/stopwatch.h"
#include "utils/utils.h"

DEFINE_string(path, "data/random.tbl", "path to output destination");
DEFINE_int64(num_tuples, 10'000'000, "number of tuples to generate");

template <typename... Attributes>
void generate_data() {
    File file{FLAGS_path, FileMode::WRITE};
    IO_Manager io{};
    PageLocal<Attributes...> p{};
    p.print_info();
    auto offset = 0u;
    {
        Stopwatch _{};
        auto remaining_tuples = FLAGS_num_tuples;
        while (remaining_tuples > 0) {
            p.clear();
            p.fill_random();
            p.num_tuples = remaining_tuples >= p.max_num_tuples_per_page ? p.max_num_tuples_per_page : remaining_tuples;
//            p.print_contents();
            io.block_io<false>(file.get_file_descriptor(), offset, p.as_bytes());
            // synchronous write
            while (io.has_inflight_requests()) {
                if (io.peek()) {
                    io.seen();
                }
            }
            offset += defaults::local_page_size;
            remaining_tuples -= p.max_num_tuples_per_page;
        }
    }
    println("wrote ", offset / defaults::local_page_size, " pages to file: ", FLAGS_path);
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_path.empty()) {
        println("example usage: ./generate_data --path=path/to/output --num_tuples=10000");
        exit(1);
    }
    generate_data<int64_t, int64_t, int32_t, std::array<unsigned char, 4>>();
    //    generate_data<std::tuple<int64_t, int64_t, int32_t, std::array<unsigned char, 4>>>();
}
