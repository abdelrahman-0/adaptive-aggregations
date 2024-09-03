#pragma once

#include <array>
#include <atomic>
#include <type_traits>

#include "common/page.h"
#include "defaults.h"
#include "utils/custom_concepts.h"
#include "utils/hash.h"

static constexpr std::size_t highest_bit_mask = static_cast<std::size_t>(~0) >> 1;

struct Swip {

  private:
    using PageIdx = std::size_t;
    uintptr_t val{0};

  public:
    Swip() = default;

    explicit Swip(const PageIdx idx) : val(idx | (~highest_bit_mask)) {}

    explicit Swip(custom_concepts::pointer_type auto ptr) : val(ptr) {}

    Swip& operator=(PageIdx idx) {
        val = idx | (~highest_bit_mask);
        return *this;
    }

    [[nodiscard]] inline bool is_page_idx() const { return val & ~highest_bit_mask; }

    [[nodiscard]] inline bool is_pointer() const { return not is_page_idx(); }

    inline void set_pointer(custom_concepts::pointer_type auto ptr) { val = reinterpret_cast<uintptr_t>(ptr); }

    inline void set_page_index(const PageIdx idx) { val = idx | (~highest_bit_mask); }

    [[nodiscard]] inline PageIdx get_page_index() const { return val & highest_bit_mask; }

    [[nodiscard]] inline PageIdx get_byte_offset() const {
        return (val & highest_bit_mask) * defaults::local_page_size;
    }

    template <custom_concepts::pointer_type T>
    [[nodiscard]] inline auto get_pointer() const {
        return reinterpret_cast<T>(val);
    }

    std::ostream& operator<<(std::ostream& out) const {
        out << reinterpret_cast<uint64_t>(val);
        return out;
    }

    Swip& operator++() {
        set_page_index(get_page_index() + 1);
        return *this;
    }
};

template <custom_concepts::is_page CachePage>
struct Cache {
    std::vector<CachePage> pages;

    explicit Cache(std::size_t num_pages_cache) { pages.resize(num_pages_cache); };

    ~Cache() = default;

    [[nodiscard]] auto& get_page(std::size_t idx) { return pages[idx]; }
};
