// Exercises find_relatively_hot_ranges (docs/rebalancing-algorithm.md §4) on
// random cold range lists against a plain reimplementation of its contract:
// the blocks it hands over, their order, and the cold ranges it leaves.
//
//   hot_range_finding_test_<target> [nr_trials] [seed]

#include "bpforest.hpp"
#include "common.h"
#include "host_params.hpp"
#include "linked_list.hpp"
#include "pairs_range.hpp"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <random>
#include <vector>


namespace
{

struct Block {
    size_t range, begin, end;  // indices into the pairs of the trial
    size_t first_chunk;        // index into the chunk loads of the trial
    uint32_t load;
};

unsigned nr_failures = 0;

void fail(unsigned trial, const char* what)
{
    std::cout << "FAIL trial " << trial << ": " << what << std::endl;
    nr_failures++;
}

//! The blocks of the ranges [begins[i], begins[i + 1]) of `pairs`, in key
//! order: whole chunks are appended until a block holds hot_npairs pairs, and
//! the tail of a range is a block of its own.
std::vector<Block> cut_into_blocks(const std::vector<size_t>& begins, const std::vector<uint32_t>& chunk_loads, uint32_t hot_npairs)
{
    std::vector<Block> blocks;
    size_t idx_chunk = 0;
    for (size_t r = 0; r + 1 < begins.size(); r++) {
        size_t pos = begins[r];
        while (pos < begins[r + 1]) {
            Block block{r, pos, pos, idx_chunk, 0};
            size_t npairs = 0;
            while (pos < begins[r + 1] && npairs < hot_npairs) {
                const size_t chunk_end = std::min(pos + KVPairsChunkSize, begins[r + 1]);
                npairs += chunk_end - pos;
                block.load += chunk_loads[idx_chunk++];
                pos = chunk_end;
            }
            block.end = pos;
            blocks.push_back(block);
        }
    }
    return blocks;
}

}  // namespace

int main(int argc, char* argv[])
{
    const unsigned nr_trials = argc > 1 ? static_cast<unsigned>(std::atoi(argv[1])) : 2000;
    const unsigned seed = argc > 2 ? static_cast<unsigned>(std::atoi(argv[2])) : 1;
    std::mt19937_64 rng{seed};

    for (unsigned trial = 0; trial < nr_trials; trial++) {
        // 1..4 cold ranges of 1..12 chunks each, the last chunk possibly partial
        const size_t nr_ranges = 1 + rng() % 4;
        std::vector<size_t> begins{0};
        for (size_t r = 0; r < nr_ranges; r++) {
            const size_t npairs = KVPairsChunkSize * (rng() % 12) + 1 + rng() % KVPairsChunkSize;
            begins.push_back(begins.back() + npairs);
        }
        const size_t nr_pairs = begins.back();
        std::vector<KVPair> pairs(nr_pairs);
        for (size_t i = 0; i < nr_pairs; i++) {
            pairs[i] = {static_cast<key_uint64_t>(i * 3), 0};
        }

        size_t nr_chunks = 0;
        for (size_t r = 0; r < nr_ranges; r++) {
            nr_chunks += (begins[r + 1] - begins[r] + KVPairsChunkSize - 1) / KVPairsChunkSize;
        }
        std::vector<LinkedChunkedPairsRange> nodes(nr_ranges + nr_chunks);  // enough for every run the removal can leave
        std::vector<uint32_t> loads(nr_chunks);
        LinkedList<ChunkedPairsRange> list;
        for (size_t r = 0; r < nr_ranges; r++) {
            nodes[r] = ChunkedPairsRange{PairsRange{&pairs[begins[r]], &pairs[begins[r + 1]]}};
            list.push_back(nodes[r]);
        }
        for (uint32_t& load : loads) {
            load = rng() % 4 == 0 ? 0 : static_cast<uint32_t>(rng() % 1000);
        }
        {
            uint32_t* cursor = loads.data();
            for (ChunkedPairsRange& range : list) {
                range.set_load_ary(cursor);
                cursor += range.nchunks();
            }
        }

        const uint32_t hot_npairs = 1 + static_cast<uint32_t>(rng() % (3 * KVPairsChunkSize));
        const std::vector<Block> want = cut_into_blocks(begins, loads, hot_npairs);
        for (size_t i = 0; i + 1 < want.size(); i++) {
            if (want[i].range == want[i + 1].range && (want[i].end - want[i].begin + KVPairsChunkSize - 1) / KVPairsChunkSize != (hot_npairs + KVPairsChunkSize - 1) / KVPairsChunkSize) {
                fail(trial, "the reference cut a block other than the last of its range to a wrong number of chunks");
            }
        }
        // The hook returns false on the nr_taken-th block; that block counts
        // as handed over too.
        const size_t nr_taken = 1 + rng() % want.size();

        std::vector<Block> got;
        size_t nr_new_nodes = 0;
        find_relatively_hot_ranges(
            list, hot_npairs,
            [&](const ChunkedPairsRange& range, DataChunkIterator begin, DataChunkIterator end, uint32_t load) {
                const size_t idx_range = static_cast<size_t>(std::lower_bound(begins.begin(), begins.end(), static_cast<size_t>(range.PairsRange::begin() - pairs.data()) + 1) - begins.begin()) - 1;
                if (static_cast<size_t>(range.PairsRange::begin() - pairs.data()) != begins[idx_range] || static_cast<size_t>(range.PairsRange::end() - pairs.data()) != begins[idx_range + 1]) {
                    fail(trial, "a range was already altered while the hook was running");
                }
                uint32_t sum = 0;
                for (DataChunkIterator chunk = begin; chunk != end; ++chunk) {
                    sum += chunk->load();
                }
                if (sum != load) {
                    fail(trial, "the chunk iterators handed over do not sum to the load handed over");
                }
                got.push_back({idx_range, static_cast<size_t>(begin.begin() - pairs.data()), static_cast<size_t>(end.begin() - pairs.data()),
                    static_cast<size_t>(begin.load_ptr() - loads.data()), load});
                return got.size() < nr_taken;
            },
            [&]() -> LinkedChunkedPairsRange& { return nodes[nr_ranges + nr_new_nodes++]; });

        // the blocks handed over: the nr_taken most loaded ones, in descending load
        if (got.size() != nr_taken) {
            fail(trial, "wrong number of blocks handed over");
            continue;
        }
        std::vector<Block> sorted = want;
        std::stable_sort(sorted.begin(), sorted.end(), [](const Block& lhs, const Block& rhs) { return lhs.load > rhs.load; });
        for (size_t i = 0; i < got.size(); i++) {
            if (got[i].load != sorted[i].load) {
                fail(trial, "blocks not handed over in descending load");
                break;
            }
            const auto it = std::find_if(want.begin(), want.end(), [&](const Block& b) { return b.begin == got[i].begin && b.end == got[i].end; });
            if (it == want.end() || it->range != got[i].range) {
                fail(trial, "a block handed over is not one of the expected blocks");
                break;
            }
            if (it->load != got[i].load || it->first_chunk != got[i].first_chunk) {
                fail(trial, "the load or the chunk loads handed over with a block are not that block's");
                break;
            }
        }

        // what is left: the runs of blocks not handed over, in key order
        std::vector<bool> taken(want.size(), false);
        for (const Block& b : got) {
            for (size_t i = 0; i < want.size(); i++) {
                if (want[i].begin == b.begin) {
                    if (taken[i]) {
                        fail(trial, "a block was handed over twice");
                    }
                    taken[i] = true;
                }
            }
        }
        struct Run {
            size_t begin, end, first_chunk;
            const LinkedChunkedPairsRange* node;  // the range's own node for its first run, otherwise a new one
        };
        std::vector<Run> runs;
        std::vector<bool> range_has_run(nr_ranges, false);
        for (size_t i = 0; i < want.size();) {
            if (taken[i]) {
                i++;
                continue;
            }
            const size_t run_begin = want[i].begin, run_first_chunk = want[i].first_chunk, range = want[i].range;
            size_t run_end = want[i].end;
            for (i++; i < want.size() && !taken[i] && want[i].range == range; i++) {
                run_end = want[i].end;
            }
            runs.push_back({run_begin, run_end, run_first_chunk, range_has_run[range] ? nullptr : &nodes[range]});
            range_has_run[range] = true;
        }
        size_t idx_run = 0;
        for (const ChunkedPairsRange& range : list) {
            const LinkedChunkedPairsRange& node = static_cast<const LinkedChunkedPairsRange&>(range);
            if (idx_run == runs.size()) {
                fail(trial, "more cold ranges left than runs of blocks not handed over");
                break;
            }
            const Run& run = runs[idx_run++];
            if (static_cast<size_t>(range.PairsRange::begin() - pairs.data()) != run.begin || static_cast<size_t>(range.PairsRange::end() - pairs.data()) != run.end) {
                fail(trial, "a cold range left does not match the run of blocks not handed over");
            }
            if (range.begin().load_ptr() != loads.data() + run.first_chunk) {
                fail(trial, "a cold range left points at the wrong chunk loads");
            }
            const bool new_node = &node >= &nodes[nr_ranges] && &node < &nodes[nr_ranges + nr_new_nodes];
            if (run.node != nullptr ? &node != run.node : !new_node) {
                fail(trial, "the first run of a range is not in the range's own node, or a later run is");
            }
        }
        if (idx_run != runs.size()) {
            fail(trial, "fewer cold ranges left than runs of blocks not handed over");
        }
        const size_t nr_ranges_with_run = static_cast<size_t>(std::count(range_has_run.begin(), range_has_run.end(), true));
        if (nr_new_nodes != runs.size() - nr_ranges_with_run) {
            fail(trial, "the number of new nodes is not the number of runs beyond the first of each range");
        }
    }

    if (nr_failures != 0) {
        std::cout << nr_failures << " failure(s)" << std::endl;
        return 1;
    }
    std::cout << "all passed (" << nr_trials << " trials)" << std::endl;
    return 0;
}
