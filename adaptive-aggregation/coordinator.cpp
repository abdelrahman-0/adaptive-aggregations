#include <gflags/gflags.h>

#include "adaptive/node_monitor.h"
#include "bench/common_flags.h"
#include "core/network/connection.h"
#include "core/network/network_manager.h"
#include "utils/configuration.h"

DEFINE_uint32(initworkers, 1, "initial number of workers to use");

int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    adapt::Configuration config{FLAGS_config};

    // accept connections from workers
    print("coordinator: waiting for", FLAGS_nodes, "connections");
    auto conn_fds                = Connection::setup_ingress(config.get_coordinator_info().port, FLAGS_nodes);
    auto egress_network_manager  = network::HomogeneousEgressNetworkManager<adapt::StateMessage>{FLAGS_nodes, FLAGS_depthnw, FLAGS_sqpoll, conn_fds};
    auto ingress_network_manager = network::HomogeneousIngressNetworkManager<adapt::StateMessage>{FLAGS_nodes, FLAGS_depthnw, FLAGS_sqpoll, conn_fds};
    auto available_workers       = std::vector<node_t>(FLAGS_nodes);
    std::iota(available_workers.begin(), available_workers.end(), 0);
    ENSURE(FLAGS_nodes <= config.get_num_workers());
    ENSURE(FLAGS_initworkers <= FLAGS_nodes);
    auto monitor = adapt::CoordinatorMonitor{FLAGS_initworkers, available_workers, egress_network_manager, ingress_network_manager};

    print("coordinator: launching...");
    monitor.monitor_query(0u, FLAGS_npages);
    print("coordinator: finished");
    Connection::close_connections(conn_fds);
}
