#pragma once

#include "common/page.h"
#include "concepts_traits/concepts_common.h"
#include "core/network/page_communication.h"
#include "defaults.h"
#include "utils/hash.h"

template <concepts::is_slot Next>
struct Chained {
    Next next;
};

template <concepts::is_slot Next, typename... Attributes>
struct ChainedEntry : public Chained<Next> {
    using Chained<Next>::next;
    std::tuple<Attributes...> val;

    friend std::ostream& operator<<(std::ostream& os, const ChainedEntry<Next, Attributes...>& entry)
    {
        return os << std::get<0>(std::get<0>(entry.val)) << " " << std::get<0>(std::get<1>(entry.val));
    }
};

template <concepts::is_slot Next, typename... Attributes>
using PagePreAggHT = PageCommunication<defaults::hashtable_page_size, ChainedEntry<Next, Attributes...>>;

template <concepts::is_slot Next, typename GroupAttributes, typename AggregateAttributes>
requires(type_traits::is_tuple_v<GroupAttributes> and type_traits::is_tuple_v<AggregateAttributes>)
struct PageAggHashTable : public PagePreAggHT<Next, GroupAttributes, AggregateAttributes> {
    using PageBase = PagePreAggHT<Next, GroupAttributes, AggregateAttributes>;
    using PageBase::emplace_back;
    using TupleAgg = ChainedEntry<Next, GroupAttributes, AggregateAttributes>;

    ALWAYS_INLINE GroupAttributes& get_group(std::integral auto idx) { return std::get<0>(get_value(idx).val); }

    ALWAYS_INLINE GroupAttributes& get_group(concepts::is_pointer auto tuple_ptr)
    {
        return std::get<0>(reinterpret_cast<TupleAgg*>(tuple_ptr)->val);
    }

    ALWAYS_INLINE AggregateAttributes& get_aggregates(std::integral auto idx)
    {
        return std::get<1>(get_value(idx).val);
    }

    ALWAYS_INLINE AggregateAttributes& get_aggregates(concepts::is_pointer auto tuple_ptr)
    {
        return std::get<1>(reinterpret_cast<TupleAgg*>(tuple_ptr)->val);
    }

    ALWAYS_INLINE auto get_next(std::integral auto idx) { return get_value(idx).next; }

    ALWAYS_INLINE auto get_next(concepts::is_pointer auto tuple_ptr)
    {
        return reinterpret_cast<TupleAgg*>(tuple_ptr)->next;
    }

    ALWAYS_INLINE auto emplace_back_grp(Next offset, GroupAttributes key, AggregateAttributes value)
    {
        return emplace_back(TupleAgg{offset, std::make_tuple(key, value)});
    }
};
