#include <gflags/gflags.h>

#include "adaptive/worker_state.h"
#include "bench/common_flags.h"
#include "core/network/connection.h"
#include "core/network/network_manager.h"
#include "defaults.h"

// TODO config file
// TODO separate flags

int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // accept from nworkers
    // TODO rewrite?
    auto conn = Connection{FLAGS_nodes, 1, 0};
    conn.setup_ingress();

    auto initial_nworkers        = 1u;
    auto egress_network_manager  = network::HomogeneousEgressNetworkManager<adapt::StateMessage>{FLAGS_nodes, FLAGS_depthnw, FLAGS_sqpoll, conn.socket_fds};
    auto ingress_network_manager = network::HomogeneousIngressNetworkManager<adapt::StateMessage>{FLAGS_nodes, FLAGS_depthnw, FLAGS_sqpoll, conn.socket_fds};
    auto workers_manager         = adapt::WorkersManager{egress_network_manager, ingress_network_manager, initial_nworkers, FLAGS_nodes};

    // send task offer to all nodes (static partitioning) and post recvs
    while (ingress_network_manager.has_inflight()) {
        // wait for recv
        ingress_network_manager.consume_done();
        egress_network_manager.try_drain_pending();
    }
    workers_manager.finalize_query();
}
