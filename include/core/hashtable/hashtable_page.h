#pragma once

#include "core/network/page_communication.h"
#include "core/page.h"
#include "defaults.h"
#include "misc/concepts_traits/concepts_hashtable.h"
#include "utils/hash.h"

namespace hashtable {

template <typename... Attributes>
struct Entry {
    std::tuple<Attributes...> val;
};

template <concepts::is_slot Next>
struct Chained {
    Next next;
};

template <concepts::is_slot Next, typename... Attributes>
struct ChainedEntry : public Chained<Next>, public Entry<Attributes...> {};

// salted hash tables do not need a next pointer since maximum PSL = 1
template <concepts::is_slot Next, typename GroupAttributes, typename AggregateAttributes, bool is_entry_chained>
using EntryOnPage = std::conditional_t<is_entry_chained, ChainedEntry<Next, GroupAttributes, AggregateAttributes>,
                                       Entry<GroupAttributes, AggregateAttributes>>;

template <concepts::is_slot Next, typename GroupAttributes, typename AggregateAttributes, bool is_entry_chained,
          bool use_ptr = true>
requires(type_traits::is_tuple_v<GroupAttributes> and type_traits::is_tuple_v<AggregateAttributes>)
struct PageAggregation
    : public PageCommunication<defaults::hashtable_page_size,
                               EntryOnPage<Next, GroupAttributes, AggregateAttributes, is_entry_chained>, use_ptr> {
    using Entry = EntryOnPage<Next, GroupAttributes, AggregateAttributes, is_entry_chained>;
    using PageBase = PageCommunication<defaults::hashtable_page_size, Entry, use_ptr>;
    using PageBase::columns;
    using PageBase::emplace_back;
    using PageBase::get_value;
    using PageBase::num_tuples;

    ALWAYS_INLINE GroupAttributes& get_group(std::integral auto idx) { return std::get<0>(get_value(idx).val); }

    ALWAYS_INLINE GroupAttributes& get_group(concepts::is_pointer auto tuple_ptr)
    {
        return std::get<0>(reinterpret_cast<Entry*>(tuple_ptr)->val);
    }

    ALWAYS_INLINE AggregateAttributes& get_aggregates(std::integral auto idx)
    {
        return std::get<1>(get_value(idx).val);
    }

    ALWAYS_INLINE AggregateAttributes& get_aggregates(concepts::is_pointer auto tuple_ptr)
    {
        return std::get<1>(reinterpret_cast<Entry*>(tuple_ptr)->val);
    }

    ALWAYS_INLINE auto get_next(std::integral auto idx)
    requires(is_entry_chained)
    {
        return get_value(idx).next;
    }

    ALWAYS_INLINE auto get_next(concepts::is_pointer auto tuple_ptr)
    requires(is_entry_chained)
    {
        return reinterpret_cast<Entry*>(tuple_ptr)->next;
    }

    ALWAYS_INLINE auto emplace_back_grp(Next offset, GroupAttributes key, AggregateAttributes value)
    requires(is_entry_chained)
    {
        return emplace_back(Entry{offset, std::make_tuple(key, value)});
    }

    ALWAYS_INLINE auto emplace_back_grp(GroupAttributes key, AggregateAttributes value)
    requires(not is_entry_chained)
    {
        return emplace_back(Entry{std::make_tuple(key, value)});
    }
};

} // namespace hashtable