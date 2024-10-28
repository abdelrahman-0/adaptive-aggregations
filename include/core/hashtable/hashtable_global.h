#pragma once

namespace ht {
//    using PointerEntry = std::conditional_t<concurrent, std::atomic<Slot>, Slot>;

//     void aggregate(Entry& entry)
//    requires(true and concurrent)
//    {
//        auto& key = entry.get_group();
//        auto& agg = entry.get_aggregates();
//        auto& next = *reinterpret_cast<Entry**>(&entry.get_next());
//        aggregate(key, agg, next, hash_tuple(key), &entry);
//    }



//void aggregate(Key& key, Value& value, Entry*& next, u64 key_hash, Entry* addr)
//requires(true and concurrent)
//{
//    // extract lower bits from hash
//    // TODO current chained entry
//    u64 mod = key_hash & ht_mask;
//    Entry* head = slots[mod];
//    while (true) {
//        Entry* slot = head;
//        // try update
//        while (slot) {
//            // walk chain of slots
//            //                if (std::get<0>(slot->val) == key) {
//            //                    fn_agg(slot->get_aggregates(), value);
//            //                    return;
//            //                }
//            slot = slot->get_next();
//        }
//
//        // try insert
//        //            if (slots[mod].compare_exchange_strong(head, )) {
//        //                return;
//        //            }
//        // chain was updated, try again
//    }
//}
}
