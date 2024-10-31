#pragma once

#include "core/network/page_communication.h"
#include "core/page.h"
#include "defaults.h"
#include "ht_utils.h"
#include "misc/concepts_traits/concepts_hashtable.h"
#include "utils/hash.h"

namespace ht {

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
template <typename GroupAttributes, typename AggregateAttributes, IDX_MODE mode, bool is_chained, bool next_first>
struct EntryAggregation;

template <typename GroupAttributes, typename AggregateAttributes, IDX_MODE mode, bool is_chained, bool next_first>
using agg_entry_idx_t = std::conditional_t<mode == DIRECT, EntryAggregation<GroupAttributes, AggregateAttributes, mode, is_chained, next_first>*,
                                           std::conditional_t<mode == INDIRECT_16, u16, std::conditional_t<mode == INDIRECT_32, u32, u64>>>;

template <typename GroupAttributes, typename AggregateAttributes, IDX_MODE mode, bool is_chained, bool next_first>
using BaseEntryAggregation = std::conditional_t<
    is_chained,
    std::conditional_t<
        next_first,
        ChainedEntry<agg_entry_idx_t<GroupAttributes, AggregateAttributes, mode, is_chained, next_first>, GroupAttributes, AggregateAttributes>,
        EntryChained<agg_entry_idx_t<GroupAttributes, AggregateAttributes, mode, is_chained, next_first>, GroupAttributes, AggregateAttributes>>,
    Entry<GroupAttributes, AggregateAttributes>>;

template <typename GroupAttributes, typename AggregateAttributes, IDX_MODE mode, bool is_chained, bool next_first = true>
struct EntryAggregation : public BaseEntryAggregation<GroupAttributes, AggregateAttributes, mode, is_chained, next_first> {
    using base_t = BaseEntryAggregation<GroupAttributes, AggregateAttributes, mode, is_chained, next_first>;

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

template <typename GroupAttributes, typename AggregateAttributes, IDX_MODE mode, bool is_chained, bool use_ptr = true, bool next_first = true>
requires(type_traits::is_tuple_v<GroupAttributes> and type_traits::is_tuple_v<AggregateAttributes>)
struct PageAggregation : public PageCommunication<defaults::hashtable_page_size,
                                                  EntryAggregation<GroupAttributes, AggregateAttributes, mode, is_chained, next_first>, use_ptr> {
    using entry_t = EntryAggregation<GroupAttributes, AggregateAttributes, mode, is_chained, next_first>;
    using base_t = PageCommunication<defaults::hashtable_page_size, entry_t, use_ptr>;
    using base_t::columns;
    using base_t::emplace_back;
    using base_t::get_attribute_ref;
    using base_t::num_tuples;
    using idx_t = agg_entry_idx_t<GroupAttributes, AggregateAttributes, mode, is_chained, next_first>;

    // TODO -1 for not DIRECT
    static constexpr idx_t EMPTY_SLOT = 0;

    ALWAYS_INLINE GroupAttributes& get_group(std::unsigned_integral auto idx)
    {
        return get_attribute_ref(idx).get_group();
    }

    ALWAYS_INLINE GroupAttributes& get_group(entry_t* tuple_ptr)
    {
        return tuple_ptr->get_group();
    }

    ALWAYS_INLINE AggregateAttributes& get_aggregates(std::unsigned_integral auto idx)
    {
        return get_attribute_ref(idx).get_aggregates();
    }

    ALWAYS_INLINE AggregateAttributes& get_aggregates(entry_t* tuple_ptr)
    {
        return tuple_ptr->get_aggregates();
    }

    ALWAYS_INLINE auto& get_next(std::unsigned_integral auto idx)
    requires(is_chained)
    {
        return get_attribute_ref(idx).get_next();
    }

    ALWAYS_INLINE auto& get_next(entry_t* tuple_ptr)
    requires(is_chained)
    {
        return tuple_ptr->get_next();
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
