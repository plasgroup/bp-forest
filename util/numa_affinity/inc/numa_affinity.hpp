#pragma once

#include <numa.h>

#include <cerrno>
#include <system_error>
#include <vector>


namespace NUMA
{

struct Topology {
    std::vector<unsigned> cpus_per_node;
    unsigned total_cpus = 0;

    Topology()
    {
        if (numa_available() >= 0) {
            const int max_node = numa_max_node();
            cpus_per_node.resize(static_cast<unsigned>(max_node) + 1u);

            struct bitmask* cpus = numa_allocate_cpumask();
            for (int node = 0; node <= max_node; ++node) {
                unsigned count = 0;
                if (numa_node_to_cpus(node, cpus) == 0) {
                    count = numa_bitmask_weight(cpus);
                }
                cpus_per_node[static_cast<unsigned>(node)] = count;
                total_cpus += count;
            }
            numa_free_cpumask(cpus);
        }
    }
};

//! @return ID of assigned NUMA node
unsigned set_compact_affinity(unsigned index, const Topology& topo)
{
    unsigned node = 0;

    if (topo.cpus_per_node.size() > 1) {
        {
            const unsigned pos = index % topo.total_cpus;
            unsigned acc = 0;
            for (;; ++node) {
                acc += topo.cpus_per_node[node];
                if (pos < acc) {
                    break;
                }
            }
        }

        if (numa_run_on_node(static_cast<int>(node)) != 0) {
            throw std::system_error{errno, std::generic_category(), "numa_run_on_node"};
        }
    }

    return node;
}

}  // namespace NUMA
