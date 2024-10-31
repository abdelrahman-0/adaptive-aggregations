#pragma once

#include "core/memory/block_allocator.h"
#include "defaults.h"

namespace ubench {

template <typename EgressMgr, typename IngressMgr, typename BufferPage>
struct HeterogeneousThreadGroup {
    EgressMgr* egress_mgr;
    IngressMgr* ingress_mgr;
    mem::BlockAllocator<BufferPage, mem::MMapAllocator<true>, true> ingress_block_alloc;

    HeterogeneousThreadGroup() = delete;
    HeterogeneousThreadGroup(u32 block_sz, u32 max_alloc) : ingress_block_alloc(block_sz, max_alloc) {}
};

} // namespace ubench
