#pragma once

#include <array>
#include <atomic>
#include <type_traits>

#include "core/page.h"
#include "defaults.h"
#include "misc/concepts_traits/concepts_common.h"
#include "utils/hash.h"

static constexpr std::size_t highest_bit_mask = static_cast<std::size_t>(~0) >> 1;

struct Swip {

  private:
    using PageIdx = std::size_t;
    uintptr_t val{0};

  public:
    Swip() = default;

    explicit Swip(const PageIdx idx) : val(idx | (~highest_bit_mask)) {}

    explicit Swip(concepts::is_pointer auto ptr) : val(ptr) {}

    Swip& operator=(PageIdx idx)
    {
        val = idx | (~highest_bit_mask);
        return *this;
    }

    [[nodiscard]]
    ALWAYS_INLINE bool is_page_idx() const
    {
        return val & ~highest_bit_mask;
    }

    [[nodiscard]]
    ALWAYS_INLINE bool is_pointer() const
    {
        return not is_page_idx();
    }

    ALWAYS_INLINE void set_pointer(concepts::is_pointer auto ptr) { val = reinterpret_cast<uintptr_t>(ptr); }

    ALWAYS_INLINE void set_page_index(const PageIdx idx) { val = idx | (~highest_bit_mask); }

    [[nodiscard]]
    ALWAYS_INLINE PageIdx get_page_index() const
    {
        return val & highest_bit_mask;
    }

    [[nodiscard]]
    inline PageIdx get_byte_offset() const
    {
        return (val & highest_bit_mask) * defaults::local_page_size;
    }

    template <typename T>
    [[nodiscard]]
    ALWAYS_INLINE auto get_pointer() const
    {
        return reinterpret_cast<T*>(val);
    }

    std::ostream& operator<<(std::ostream& out) const
    {
        out << reinterpret_cast<uint64_t>(val);
        return out;
    }

    Swip& operator++()
    {
        set_page_index(get_page_index() + 1);
        return *this;
    }
};

template <concepts::is_page CachePage>
struct Cache {
    std::vector<CachePage> pages;

    explicit Cache(std::size_t num_pages_cache) { pages.resize(num_pages_cache); };

    ~Cache() = default;

    [[nodiscard]]
    auto& get_page(std::size_t idx)
    {
        return pages[idx];
    }
};
