#pragma once

#include <cstdint>
#include <atomic>
#include <string>
#include <vector>

#include "file.h"

static std::atomic<uint64_t> global_segment_id = 0;

class Table{
private:
    std::vector<Swips>;
    File file_tbl;
public:
    uint64_t segment_id;

    Table(const std::string& path, FileMode mode) : file_tbl({path, mode}) {
        segment_id = global_segment_id.fetch_add(1);
    }
    ~Table() = default;
};