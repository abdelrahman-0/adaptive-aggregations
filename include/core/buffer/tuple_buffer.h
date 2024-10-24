#pragma once

#include <tbb/concurrent_vector.h>
#include <vector>

#include "defaults.h"

template <typename BufferPage, bool is_concurrent = false>
struct PageBuffer {
    u64 num_tuples{0};
    std::conditional_t<is_concurrent, tbb::concurrent_vector<BufferPage*>, std::vector<BufferPage*>> pages;

    void add_page(BufferPage* page)
    {
        num_tuples += page->num_tuples;
        pages.push_back(page);
    }

    void print_pages() const
    {
        for (auto* p : pages) {
            p->print_page();
        }
    }

    auto begin() { return pages.begin(); }

    auto cbegin() const { return pages.cbegin(); }

    auto end() { return pages.end(); }

    auto cend() const { return pages.cend(); }
};
