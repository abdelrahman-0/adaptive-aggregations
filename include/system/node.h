#pragma once

#include <gflags/gflags.h>

#include "topology.h"

DECLARE_uint32(nodes);

namespace sys {

class Node {
  private:
    NodeTopology topology;
    u32 node_id;

  public:
    explicit Node(u32 threads) : topology(threads)
    {
        auto env_var = std::getenv("NODE_ID");
        node_id = env_var ? std::stoul(env_var) : 0;
        print("NODE", node_id, "of", FLAGS_nodes - 1, ":");
        print("--------------");
        topology.init();
    }

    [[nodiscard]]
    u32 get_id() const
    {
        return node_id;
    }

    [[nodiscard]]
    static u32 get_npeers()
    {
        return FLAGS_nodes - 1;
    }

    void pin_thread(int tid) { topology.pin_thread(tid); }
};

} // namespace sys
