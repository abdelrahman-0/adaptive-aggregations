#pragma once

#include <vector>

#include "defaults.h"

template <typename BufferPage>
struct TupleBuffer {
    u64 num_tuples{0};
    std::vector<BufferPage*> pages;

    void add_page(BufferPage* page)
    {
        num_tuples += page->num_tuples;
        pages.push_back(page);
    }

    auto begin() { return pages.begin(); }

    auto cbegin() const { return pages.cbegin(); }

    auto end() { return pages.end(); }

    auto cend() const { return pages.cend(); }
};