#pragma once

#include "core/network/page_communication.h"
#include "core/page.h"
#include "defaults.h"
#include "misc/concepts_traits/concepts_hashtable.h"
#include "utils/hash.h"

namespace ht {

enum IDX_MODE : u8 {
    DIRECT = 0,
    INDIRECT_16,
    INDIRECT_32,
    INDIRECT_64,
};

template <typename... Attributes>
struct Entry {
    std::tuple<Attributes...> val;
};

template <concepts::is_slot Next>
struct Chained {
    Next next;
};

// next pointer first, then entry
template <concepts::is_slot Next, typename... Attributes>
struct ChainedEntry : public Chained<Next>, public Entry<Attributes...> {};

// entry first, then next pointer
template <concepts::is_slot Next, typename... Attributes>
struct EntryChained : public Entry<Attributes...>, public Chained<Next> {};

// forward declaration
template <IDX_MODE mode, typename GroupAttributes, typename AggregateAttributes, bool is_chained, bool next_first>
struct EntryAggregation;

template <IDX_MODE mode, typename GroupAttributes, typename AggregateAttributes, bool is_chained, bool next_first>
using entry_agg_idx_type = std::conditional_t<mode == DIRECT, EntryAggregation<mode, GroupAttributes, AggregateAttributes, is_chained, next_first>*,
                                              std::conditional_t<mode == INDIRECT_16, u16, std::conditional_t<mode == INDIRECT_32, u32, u64>>>;

template <IDX_MODE mode, typename GroupAttributes, typename AggregateAttributes, bool is_chained, bool next_first>
using BaseEntryAggregation = std::conditional_t<
    is_chained,
    std::conditional_t<
        next_first,
        ChainedEntry<entry_agg_idx_type<mode, GroupAttributes, AggregateAttributes, is_chained, next_first>, GroupAttributes, AggregateAttributes>,
        EntryChained<entry_agg_idx_type<mode, GroupAttributes, AggregateAttributes, is_chained, next_first>, GroupAttributes, AggregateAttributes>>,
    Entry<GroupAttributes, AggregateAttributes>>;

template <IDX_MODE mode, typename GroupAttributes, typename AggregateAttributes, bool is_chained, bool next_first = true>
struct EntryAggregation : public BaseEntryAggregation<mode, GroupAttributes, AggregateAttributes, is_chained, next_first> {
    using base_t = BaseEntryAggregation<mode, GroupAttributes, AggregateAttributes, is_chained, next_first>;

    ALWAYS_INLINE GroupAttributes& get_group()
    {
        return std::get<0>(base_t::val);
    }

    ALWAYS_INLINE AggregateAttributes& get_aggregates()
    {
        return std::get<1>(base_t::val);
    }

    ALWAYS_INLINE auto& get_next()
    requires(is_chained)
    {
        return base_t::next;
    }
};

template <IDX_MODE mode, typename GroupAttributes, typename AggregateAttributes, bool is_chained, bool use_ptr = true, bool next_first = true>
requires(type_traits::is_tuple_v<GroupAttributes> and type_traits::is_tuple_v<AggregateAttributes>)
struct PageAggregation : public PageCommunication<defaults::hashtable_page_size,
                                                  EntryAggregation<mode, GroupAttributes, AggregateAttributes, is_chained, next_first>, use_ptr> {
    using entry_t = EntryAggregation<mode, GroupAttributes, AggregateAttributes, is_chained, next_first>;
    using base_t = PageCommunication<defaults::hashtable_page_size, entry_t, use_ptr>;
    using base_t::columns;
    using base_t::emplace_back;
    using base_t::get_tuple_ref;
    using base_t::num_tuples;
    using idx_t = entry_agg_idx_type<mode, GroupAttributes, AggregateAttributes, is_chained, next_first>;

    static constexpr idx_t EMPTY_SLOT = 0;

    ALWAYS_INLINE GroupAttributes& get_group(std::integral auto idx)
    {
        return get_tuple_ref(idx).get_group();
    }

    ALWAYS_INLINE GroupAttributes& get_group(concepts::is_pointer auto tuple_ptr) // TODO remove cast and auto param
    {
        return reinterpret_cast<entry_t*>(tuple_ptr)->get_group();
    }

    ALWAYS_INLINE AggregateAttributes& get_aggregates(std::integral auto idx)
    {
        return get_tuple_ref(idx).get_aggregates();
    }

    ALWAYS_INLINE AggregateAttributes& get_aggregates(concepts::is_pointer auto tuple_ptr) // TODO remove cast and auto param
    {
        return reinterpret_cast<entry_t*>(tuple_ptr)->get_aggregates();
    }

    ALWAYS_INLINE auto& get_next(std::integral auto idx)
    requires(is_chained)
    {
        return get_tuple_ref(idx).get_next();
    }

    ALWAYS_INLINE auto& get_next(concepts::is_pointer auto tuple_ptr) // TODO remove cast and auto param
    requires(is_chained)
    {
        return reinterpret_cast<entry_t*>(tuple_ptr)->get_next();
    }

    ALWAYS_INLINE auto emplace_back_grp(GroupAttributes key, AggregateAttributes value, idx_t offset = EMPTY_SLOT)
    requires(is_chained)
    {
        return emplace_back(entry_t{offset, std::make_tuple(key, value)});
    }

    ALWAYS_INLINE auto emplace_back_grp(GroupAttributes key, AggregateAttributes value)
    requires(not is_chained)
    {
        return emplace_back(entry_t{std::make_tuple(key, value)});
    }
};

} // namespace ht
