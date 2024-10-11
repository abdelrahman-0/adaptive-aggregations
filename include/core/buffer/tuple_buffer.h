#pragma once

#include <vector>

template <typename BufferPage>
struct TupleBuffer {
    std::vector<BufferPage*> pages;

    void add_page(BufferPage* page) { pages.push_back(page); }

    auto begin() { return pages.begin(); }

    auto cbegin() const { return pages.cbegin(); }

    auto end() { return pages.end(); }

    auto cend() const { return pages.cend(); }
};