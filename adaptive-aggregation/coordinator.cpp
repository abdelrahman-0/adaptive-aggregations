#include <gflags/gflags.h>

#include "adaptive/node_monitor.h"
#include "bench/common_flags.h"
#include "core/network/connection.h"
#include "core/network/network_manager.h"
#include "utils/configuration.h"

DEFINE_string(config, "../../configs/config_local.json", "path to config file");
DEFINE_uint32(initworkers, 1, "initial number of workers to use");

int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    adapre::Configuration config{FLAGS_config};

    // accept connections from workers
    print("coordinator: waiting for", FLAGS_nodes, "connections");
    auto conn_fds                = Connection::setup_ingress(config.get_coordinator_info().port, FLAGS_nodes);
    auto egress_network_manager  = network::HomogeneousEgressNetworkManager<adapre::StateMessage>{FLAGS_nodes, FLAGS_depthnw, FLAGS_sqpoll, conn_fds};
    auto ingress_network_manager = network::HomogeneousIngressNetworkManager<adapre::StateMessage>{FLAGS_nodes, FLAGS_depthnw, FLAGS_sqpoll, conn_fds};
    auto monitor                 = adapre::CoordinatorMonitor{FLAGS_initworkers, FLAGS_nodes, egress_network_manager, ingress_network_manager};

    print("coordinator: starting query");
    monitor.process_query(0u, FLAGS_npages);
    Connection::close_connections(conn_fds);
    print("coordinator done");
}
