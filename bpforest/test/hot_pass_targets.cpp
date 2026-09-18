// Exercises the case where the cold pass of an incremental repartition
// finds a DPU's cold tree not overloaded after all (its range queries only
// span it, with no endpoint inside) while the hot tree of the same DPU is:
// the hot pass has to take that DPU up all the same
// (docs/rebalancing-algorithm.md §6.1).
//
//   hot_pass_targets_test_<target> [nr_pairs] [batch_size] [part_log_out]

#include "bpforest.hpp"
#include "common.h"
#include "host_params.hpp"
#include "log.hpp"
#include "partition.hpp"
#include "workload_types.h"

#include <cstdint>
#include <cstdlib>
#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>


namespace
{

size_t count_lines(const std::string& text, const std::string& prefix)
{
    size_t count = 0;
    for (size_t pos = 0; (pos = text.find(prefix, pos)) != std::string::npos; pos += prefix.size()) {
        if (pos == 0 || text[pos - 1] == '\n') {
            count++;
        }
    }
    return count;
}

}  // namespace


int main(int argc, char* argv[])
{
    const size_t nr_pairs = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 1000000;
    const uint32_t batch_size = argc > 2 ? static_cast<uint32_t>(std::strtoul(argv[2], nullptr, 10)) : 100000;

    std::mt19937_64 rng{20260918};
    std::vector<KVPair> pairs(nr_pairs);
    for (size_t i = 0; i < nr_pairs; i++) {
        pairs[i] = {static_cast<key_uint64_t>(i + 1) * 8, static_cast<value_int64_t>(i)};
    }
    const auto random_initial_key = [&] { return pairs[rng() % nr_pairs].key; };

    auto log = std::make_unique<std::ostringstream>();
    const std::ostringstream& log_ref = *log;
    partitioning_log = std::move(log);

    // Without the host cache, the storm keeps overloading the hot tree.  With
    // the balancing factor, the hot range carved is a quarter of the base
    // partition, so that the host keeps a cold tree.
    BPForest::Param param;
    param.enable_hot_cache = false;
    param.balancing = 4;
    BPForest forest(pairs.data(), nr_pairs, param);
    const size_t nr_dpus = forest.get_nr_pairs().size();
    // The key sits in the last chunk of the second block (see
    // find_relatively_hot_ranges) of a base partition: the storm's load then
    // gives the split of that block no cut point, so the whole block, which
    // spans several chunks, becomes the hot range, and one the hot pass may
    // split.
    const size_t base_first = nr_pairs * (nr_dpus / 2) / nr_dpus, base_npairs = nr_pairs * (nr_dpus / 2 + 1) / nr_dpus - base_first,
                 hot_npairs = (base_npairs + param.balancing - 1) / param.balancing;
    size_t block_begin = base_first, block_end = base_first, last_chunk_begin = base_first;
    for (unsigned idx_block = 0; idx_block < 2; idx_block++) {
        block_begin = block_end;
        for (size_t npairs = 0; block_end < base_first + base_npairs && npairs < hot_npairs;) {
            last_chunk_begin = block_end;
            block_end = std::min(block_end + KVPairsChunkSize, base_first + base_npairs);
            npairs += block_end - last_chunk_begin;
        }
    }
    const key_uint64_t hot_key = pairs[(last_chunk_begin + block_end) / 2].key;
    const uint32_t storm = batch_size * 2 / 5;
    if (last_chunk_begin < block_begin + KVPairsChunkSize || storm <= (2 * batch_size + nr_dpus - 1) / nr_dpus) {
        std::cerr << "the second block of a base partition has to span at least 2 chunks, and the storm has to exceed 2 * batch / DPUs: use more pairs or fewer DPUs" << std::endl;
        return 2;
    }

    // 1. A get storm carves a hot range around the key.
    std::vector<key_uint64_t> keys(batch_size);
    std::vector<value_int64_t> values(batch_size);
    for (unsigned round = 0; round < 2; round++) {
        for (uint32_t i = 0; i < batch_size; i++) {
            keys[i] = rng() % batch_size < storm ? hot_key : random_initial_key();
        }
        forest.batch_get(batch_size, keys.data(), values.data());
    }
    const std::vector<Partition> partitions = forest.dump_partitions();
    size_t host = nr_dpus;
    for (size_t idx_dpu = 0; idx_dpu < nr_dpus; idx_dpu++) {
        const Partition& hot = partitions[nr_dpus + idx_dpu];
        if (hot.length != 0 && key_int64_to_uint64(hot.left_key) <= hot_key - 8 && hot_key + 8 < key_int64_to_uint64(hot.left_key) + hot.length) {
            host = idx_dpu;
        }
    }
    if (host == nr_dpus) {
        std::cout << "FAIL: the storm did not carve a hot range around the key (with room on both sides)" << std::endl;
        return 1;
    }
    const Partition& base = partitions[host];
    if (base.length == 0) {
        std::cout << "FAIL: DPU " << host << " has no base partition" << std::endl;
        return 1;
    }
    const key_uint64_t base_begin = key_int64_to_uint64(base.left_key), base_end = base_begin + base.length - 1;
    std::cout << "hot range around the key is on DPU " << host << std::endl;
    const size_t log_seen = log_ref.str().size();

    // 2. Range counts: the storm's ranges lie inside the hot range, and the
    //    rest span the host's base partition entirely (the hot range too, if
    //    it lies within), so that they reach the host's cold tree (fragments
    //    beyond the goal) without an endpoint inside it.
    std::vector<RangeCountQuery> rcqs(batch_size);
    std::vector<uint64_t> counts(batch_size);
    for (uint32_t i = 0; i < batch_size; i++) {
        if (rng() % batch_size < storm) {
            rcqs[i] = {{hot_key - 8, hot_key + 8}, 0};
        } else {
            rcqs[i] = {{base_begin - 8, base_end + 8}, 0};
        }
    }
    forest.batch_range_count(batch_size, rcqs.data(), counts.data());

    if (argc > 3) {
        std::ofstream{argv[3]} << log_ref.str();
    }
    const std::string said = log_ref.str().substr(log_seen);
    const std::string dpu = std::to_string(host);
    const size_t nr_split = count_lines(said, "split hot " + dpu + " ") + count_lines(said, "nosplit hot " + dpu + " ");
    std::cout << "hot split lines for DPU " << host << ": " << nr_split
              << ", partial reshardings " << count_lines(said, "start partial resharding")
              << ", full reshardings " << count_lines(said, "start full resharding") << std::endl;
    if (count_lines(said, "start partial resharding") == 0) {
        std::cout << "FAIL: the range batch did not trigger an incremental repartition" << std::endl;
        return 1;
    }
    if (count_lines(said, "serialize hot " + dpu + " ") == 0) {
        std::cout << "FAIL: the hot tree of DPU " << host << " was not a target" << std::endl;
        return 1;
    }
    if (nr_split == 0) {
        std::cout << "FAIL: the hot pass skipped DPU " << host << std::endl;
        return 1;
    }
    std::cout << "all passed" << std::endl;
    return 0;
}
