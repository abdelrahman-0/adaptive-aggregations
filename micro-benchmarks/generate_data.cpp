#include <typeinfo>
#include "utils/stopwatch.h"
#include <boost/core/demangle.hpp>
#include "storage/page.h"
#include "utils/utils.h"
#include <gflags/gflags.h>

#include "storage/io_manager.h"

DEFINE_string(path, "", "path to output destination");
DEFINE_int64(num_tuples, 100, "number of tuples to generate");

template <typename... Attributes>
void generate_data(){
    Page<Attributes...> p{};
    println("size of page:", sizeof(p), "( max tuples:", p.max_num_tuples_per_page, ")");
    println("columns:", boost::core::demangle(typeid(Attributes).name())...);
    {
        Stopwatch _{};
        // TODO count tuples properly
        auto i = 0u;
        for(; i < FLAGS_num_tuples; i+=p.max_num_tuples_per_page){
            p.clear();
            p.fill_random();
            p.num_tuples = p.max_num_tuples_per_page;
            p.print_contents();
        }

    }
    println("output path:", FLAGS_path);
}

int main(int argc, char* argv[]){
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_path.empty()) {
        println("example usage: ./generate_data --path=path/to/output --num_tuples=10000");
        exit(1);
    }
    generate_data<int64_t, int64_t, int32_t, unsigned char[4]>();
}
