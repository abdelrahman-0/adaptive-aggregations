#include "includes.h"

int main(int argc, char* argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    adapre::Configuration config{FLAGS_config};
    sys::Node node{FLAGS_threads};

    // generate data
    u16 node_id = node.get_id();
    auto table  = Table{FLAGS_npages};
    auto& swips = table.get_swips();
    auto cache  = Cache<PageTable>{swips.size()};
    auto npeers = u32{FLAGS_nodes - 1};
    table.populate_cache(cache, swips.size(), FLAGS_sequential_io);

    // master thread connects to coordinator
    std::barrier coordinator_barrier{2};
    auto monitor_thread = std::jthread{[node_id, &config, &coordinator_barrier]() {
        auto coordinator_info        = config.get_coordinator_info();
        auto conn_fds                = Connection::setup_egress(node_id, coordinator_info.ip, coordinator_info.port, 1);
        std::ignore                  = coordinator_barrier.arrive();
        auto egress_network_manager  = network::HomogeneousEgressNetworkManager<adapre::StateMessage>{1, FLAGS_depthnw, FLAGS_sqpoll, conn_fds};
        auto ingress_network_manager = network::HomogeneousIngressNetworkManager<adapre::StateMessage>{1, FLAGS_depthnw, FLAGS_sqpoll, conn_fds};
        adapre::WorkerMonitor monitor{egress_network_manager, ingress_network_manager};
        monitor.process_query();
        Connection::close_connections(conn_fds);
    }};
    coordinator_barrier.arrive_and_wait();

    FLAGS_slots       = next_power_2(FLAGS_slots);
    FLAGS_partitions  = next_power_2(FLAGS_partitions) * FLAGS_nodes;
    auto storage_glob = StorageGlobal{FLAGS_partitions};
    auto sketch_glob  = Sketch{};
    auto ht_glob      = HashtableGlobal{};
    // instantiate query threads
    std::vector<std::jthread> threads;
    for (u16 thread_id : range(FLAGS_threads)) {
        threads.emplace_back([npeers, thread_id, node_id, &node, &config, &storage_glob, &sketch_glob, &ht_glob]() {
            // open max nodes connections, use config for ips and ports
            if (FLAGS_pin) {
                node.pin_thread(thread_id);
            }
            /* --------------------------------------- */
            // setup connections to each node, forming a logical clique topology
            // note that connections need to be setup in a particular order to avoid deadlocks!
            auto socket_fds = std::vector<int>{};
            /* --------------------------------------- */
            // accept from [0, node_id)
            if (node_id) {
                int port_base = std::stoi(config.get_worker_info(node_id).port);
                socket_fds    = Connection::setup_ingress(std::to_string(port_base + node_id * FLAGS_threads + thread_id), node_id);
            }
            /* --------------------------------------- */
            // connect to [node_id + 1, FLAGS_nodes)
            for (u16 peer : range(node_id + 1u, FLAGS_nodes)) {
                auto worker_info = config.get_worker_info(peer);
                int port_base    = std::stoi(worker_info.port);
                socket_fds.emplace_back(Connection::setup_egress(node_id, worker_info.ip, std::to_string(port_base + peer * FLAGS_threads + thread_id)));
            }
            /* --------------------------------------- */
            auto recv_alloc                 = BlockAlloc{npeers * 10, FLAGS_maxalloc};
            auto manager_recv               = IngressManager{npeers, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
            auto manager_send               = EgressManager{npeers, FLAGS_depthnw, FLAGS_sqpoll, socket_fds};
            auto remote_sketches            = std::vector<Sketch>(npeers);
            u32 peers_done                  = 0;
            /* --------------------------------------- */
            auto ingress_page_consumer_fn   = std::function{[&recv_alloc, &storage_glob, &remote_sketches, &manager_recv](PageResult* page, u32 dst) {
                if (page->is_last_page()) {
                    // recv sketch after last page
                    manager_recv.recv(dst, remote_sketches.data() + dst);
                    if (page->empty()) {
                        return;
                    }
                }
                else {
                    manager_recv.recv(dst, recv_alloc.get_object());
                }
                storage_glob.add_page(page, page->get_part_no());
            }};
            auto ingress_sketch_consumer_fn = std::function{[&sketch_glob, &peers_done](const Sketch* sketch, u32) {
                sketch_glob.merge_concurrent(*sketch);
                peers_done++;
            }};
            manager_recv.register_consumer_fn(ingress_page_consumer_fn);
            manager_recv.register_consumer_fn(ingress_sketch_consumer_fn);
            /* --------------------------------------- */

            // prepare hashtables

            // figure out morsel-mechanism

            // figure out how to send back TASK_OFFER_RESPONSE with estimated number of workers

            // figure out sketches
        });
    }

    // after each morsel, check active_workers, if changed, update and replace evictor functions,

    // threads check if 90% of the way or PK behaviour encountered
    // => calculate usage metrics and estimate workers needed
    // => reply
    // => handle next range TODO use morsel barrier

    // think of sketches (not = 4)
}
