#include <gflags/gflags.h>

#include "defaults.h"
#include "performance/stopwatch.h"
#include "storage/file.h"
#include "storage/io_manager.h"
#include "storage/page_local.h"
#include "utils/utils.h"

#define SCHEMA uint64_t, uint32_t, uint32_t, std::array<char, 4>

DEFINE_string(path, "data/random.tbl", "path to output destination");
DEFINE_int64(ntuples, 10'000'000, "number of tuples to generate");


template <typename... Attributes>
void generate_data() {
    File file{FLAGS_path, FileMode::WRITE};
    IO_Manager io{64, false};
    PageLocal<Attributes...> p{};
    p.print_info();
    auto offset = 0u;
    {
        Stopwatch _{};
        auto remaining_tuples = FLAGS_ntuples;
        while (remaining_tuples > 0) {
            p.clear();
            p.fill_random();
            p.num_tuples = remaining_tuples >= p.max_tuples_per_page ? p.max_tuples_per_page : remaining_tuples;
//            p.print_contents();
            io.sync_io<WRITE>(file.get_file_descriptor(), offset, p);
            offset += defaults::local_page_size;
            remaining_tuples -= p.max_tuples_per_page;
        }
    }
    print("wrote ", offset / defaults::local_page_size, " pages to file: ", FLAGS_path);
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_path.empty()) {
        print("example usage: ./generate_data --path=path/to/output --num_tuples=10000");
        exit(1);
    }
    print("generating", FLAGS_ntuples, "random tuples");
    generate_data<SCHEMA>();
}
