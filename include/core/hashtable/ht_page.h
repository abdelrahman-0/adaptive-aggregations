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
template <typename GroupAttributes, typename AggregateAttributes, IDX_MODE mode, bool next_first>
struct EntryAggregation;

template <typename GroupAttributes, typename AggregateAttributes, IDX_MODE mode, bool next_first>
using agg_entry_idx_t = std::conditional_t<mode == DIRECT, EntryAggregation<GroupAttributes, AggregateAttributes, mode, next_first>*,
                                           std::conditional_t<mode == INDIRECT_16, u16, std::conditional_t<mode == INDIRECT_32, u32, u64>>>;

template <typename GroupAttributes, typename AggregateAttributes, IDX_MODE entry_mode, IDX_MODE slots_mode, bool next_first>
using agg_slot_idx_t = std::conditional_t<slots_mode == DIRECT, EntryAggregation<GroupAttributes, AggregateAttributes, entry_mode, next_first>*,
                                          std::conditional_t<entry_mode == NO_IDX, agg_entry_idx_t<GroupAttributes, AggregateAttributes, slots_mode, next_first>,
                                                             agg_entry_idx_t<GroupAttributes, AggregateAttributes, entry_mode, next_first>>>;

template <typename GroupAttributes, typename AggregateAttributes, IDX_MODE mode, bool next_first>
using BaseEntryAggregation =
    std::conditional_t<mode != NO_IDX,
                       std::conditional_t<next_first, ChainedEntry<agg_entry_idx_t<GroupAttributes, AggregateAttributes, mode, next_first>, GroupAttributes, AggregateAttributes>,
                                          EntryChained<agg_entry_idx_t<GroupAttributes, AggregateAttributes, mode, next_first>, GroupAttributes, AggregateAttributes>>,
                       Entry<GroupAttributes, AggregateAttributes>>;

// TODO add const ref returns when possible (decltype(auto)?)
template <typename GroupAttributes, typename AggregateAttributes, IDX_MODE mode, bool next_first>
struct EntryAggregation : BaseEntryAggregation<GroupAttributes, AggregateAttributes, mode, next_first> {
    static constexpr bool is_chained = mode != NO_IDX;
    using base_t                     = BaseEntryAggregation<GroupAttributes, AggregateAttributes, mode, next_first>;

    ALWAYS_INLINE decltype(auto) get_group()
    {
        return std::get<0>(base_t::val);
    }

    ALWAYS_INLINE decltype(auto) get_aggregates()
    {
        return std::get<1>(base_t::val);
    }

    ALWAYS_INLINE decltype(auto) get_next()
    requires(is_chained)
    {
        return base_t::next;
    }
};

template <typename GroupAttributes, typename AggregateAttributes, IDX_MODE mode, bool use_ptr, bool next_first>
requires(type_traits::is_tuple_v<GroupAttributes> and type_traits::is_tuple_v<AggregateAttributes>)
struct PageAggregation : PageCommunication<defaults::hashtable_page_size, EntryAggregation<GroupAttributes, AggregateAttributes, mode, next_first>, use_ptr> {
    using key_t   = GroupAttributes;
    using value_t = AggregateAttributes;
    using entry_t = EntryAggregation<GroupAttributes, AggregateAttributes, mode, next_first>;
    using base_t  = PageCommunication<defaults::hashtable_page_size, entry_t, use_ptr>;
    using base_t::columns;
    using base_t::emplace_back;
    using base_t::get_num_tuples;
    using base_t::get_tuple_ref;
    using base_t::num_tuples;
    using idx_t                          = agg_entry_idx_t<GroupAttributes, AggregateAttributes, mode, next_first>;
    static constexpr u8 part_no_shift    = 32;
    // two highest bits are used by communication page (see base class)
    static constexpr u64 num_tuples_mask = 0xC0000000FFFFFFFF;

    ALWAYS_INLINE decltype(auto) get_group(std::unsigned_integral auto idx)
    {
        return get_tuple_ref(idx).get_group();
    }

    ALWAYS_INLINE decltype(auto) get_group(entry_t* tuple_ptr)
    {
        return tuple_ptr->get_group();
    }

    ALWAYS_INLINE decltype(auto) get_aggregates(std::unsigned_integral auto idx)
    {
        return get_tuple_ref(idx).get_aggregates();
    }

    ALWAYS_INLINE decltype(auto) get_aggregates(entry_t* tuple_ptr)
    {
        return tuple_ptr->get_aggregates();
    }

    ALWAYS_INLINE decltype(auto) get_next(std::unsigned_integral auto idx)
    requires(entry_t::is_chained)
    {
        return get_tuple_ref(idx).get_next();
    }

    ALWAYS_INLINE decltype(auto) get_next(entry_t* tuple_ptr)
    requires(entry_t::is_chained)
    {
        return tuple_ptr->get_next();
    }

    [[maybe_unused]]
    ALWAYS_INLINE auto emplace_back_grp(GroupAttributes key, AggregateAttributes value, idx_t offset = 0)
    requires(entry_t::is_chained)
    {
        return emplace_back(entry_t{offset, std::make_tuple(key, value)});
    }

    [[maybe_unused]]
    ALWAYS_INLINE auto emplace_back_grp(GroupAttributes key, AggregateAttributes value)
    requires(not entry_t::is_chained)
    {
        return emplace_back(entry_t{std::make_tuple(key, value)});
    }

    void set_part_no(u64 part_no)
    {
        num_tuples |= (part_no << part_no_shift);
    }

    [[nodiscard]]
    u32 get_part_no() const
    {
        return ((num_tuples & ~num_tuples_mask) >> part_no_shift);
    }

    void clear_part_no()
    {
        num_tuples &= num_tuples_mask;
    }

    [[nodiscard]]
    u64 get_num_tuples() const
    {
        return base_t::get_num_tuples() & num_tuples_mask;
    }

    [[nodiscard]]
    bool empty() const
    {
        return get_num_tuples() == 0;
    }
};

} // namespace ht
