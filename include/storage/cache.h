#pragma once

#include <array>
#include <atomic>
#include <type_traits>

#include "defaults.h"
#include "page.h"
#include "utils/custom_concepts.h"
#include "utils/hash.h"

static constexpr std::size_t pointer_tag_mask =
    static_cast<std::size_t>(-1) >> 1;

struct Swip {

  private:
    using PageIdx = std::size_t;
    uintptr_t val{0};

  public:
    Swip() = default;

    explicit Swip(const PageIdx idx) : val(idx | (~pointer_tag_mask)) {}

    explicit Swip(custom_concepts::is_pointer_type auto ptr) : val(ptr) {}

    Swip& operator=(PageIdx idx) {
        val = idx | (~pointer_tag_mask);
        return *this;
    }

    [[nodiscard]] inline bool is_pointer() const {
        return ~(val | pointer_tag_mask);
    }

    inline void set_pointer(custom_concepts::is_pointer_type auto ptr) {
        val = reinterpret_cast<uintptr_t>(ptr);
    }

    inline void set_page_index(const PageIdx idx) {
        val = idx | (~pointer_tag_mask);
    }

    [[nodiscard]] inline PageIdx get_page_index() const {
        return val & pointer_tag_mask;
    }

    [[nodiscard]] inline PageIdx get_byte_offset() const {
        return (val & pointer_tag_mask) * defaults::local_page_size;
    }

    template <custom_concepts::is_pointer_type T>
    [[nodiscard]] inline auto get_pointer() const {
        return reinterpret_cast<T>(val);
    }
};

static constexpr std::size_t cache_size = 1 << 10;

struct Cache {
    std::vector<std::byte> pages{};

    explicit Cache(std::size_t num_pages_cache) {
        pages.resize(num_pages_cache * defaults::local_page_size);
    };

    ~Cache() = default;

    [[nodiscard]] auto* get_page_ptr(std::size_t idx) {
        return &pages[idx * defaults::local_page_size];
    }
};
