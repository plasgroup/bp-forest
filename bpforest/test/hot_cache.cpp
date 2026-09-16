// Exercises the host-side cache of the pair a hot partition's queries pile
// onto (docs/host_hot_cache.md): a get storm on one key has to install it,
// and get, insert, pred, range and delete batches on that key have to keep
// agreeing with a reference map afterwards, with the cache on and with it off.
//
//   hot_cache_test_<target> [nr_pairs] [batch_size] [cache_enabled=1|0] [part_log_out]

#include "bpforest.hpp"
#include "common.h"
#include "host_params.hpp"
#include "log.hpp"
#include "workload_types.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>


namespace
{

//! The semantics every batch is checked against, one query after another.
struct Oracle {
    std::map<key_uint64_t, value_int64_t> pairs;

    void get(size_t n, const key_uint64_t keys[], value_int64_t results[]) const
    {
        for (size_t i = 0; i < n; i++) {
            const auto it = pairs.find(keys[i]);
            results[i] = it == pairs.end() ? NOT_FOUND_VALUE : it->second;
        }
    }
    void pred(size_t n, const key_uint64_t keys[], KVPair results[]) const
    {
        for (size_t i = 0; i < n; i++) {
            const auto it = pairs.lower_bound(keys[i]);
            results[i] = it == pairs.begin() ? KVPair{KEY_MIN, NOT_FOUND_VALUE} : KVPair{std::prev(it)->first, std::prev(it)->second};
        }
    }
    void insert(size_t n, const KVPair queries[])
    {
        for (size_t i = 0; i < n; i++) {
            pairs[queries[i].key] = queries[i].value;
        }
    }
    //! Of duplicates, the first reports the deletion (docs/parallel_delete.md).
    void remove(size_t n, const key_uint64_t keys[], uint8_t existed[])
    {
        for (size_t i = 0; i < n; i++) {
            existed[i] = pairs.erase(keys[i]) != 0;
        }
    }
    void range_count(size_t n, const RangeCountQuery queries[], uint64_t results[]) const
    {
        for (size_t i = 0; i < n; i++) {
            uint64_t count = 0;
            for (auto it = pairs.lower_bound(queries[i].range.begin); it != pairs.end() && it->first <= queries[i].range.end; ++it) {
                count += it->second == queries[i].needle;
            }
            results[i] = count;
        }
    }
};

unsigned nr_failures = 0;

void fail(const std::string& what)
{
    nr_failures++;
    std::cout << "FAIL: " << what << std::endl;
}

std::ostream& operator<<(std::ostream& os, const KVPair& pair) { return os << '{' << pair.key << ", " << pair.value << '}'; }

template <typename T>
void expect_same(const char* what, size_t n, const T* got, const T* want)
{
    size_t nr_mismatches = 0, first = n;
    for (size_t i = 0; i < n; i++) {
        if (!(got[i] == want[i])) {
            if (nr_mismatches == 0) {
                first = i;
            }
            nr_mismatches++;
        }
    }
    if (nr_mismatches != 0) {
        std::ostringstream msg;
        msg << what << ": " << nr_mismatches << " of " << n << " results differ, first at " << first << ": got ";
        if constexpr (std::is_same_v<T, uint8_t>) {
            msg << +got[first] << ", want " << +want[first];
        } else {
            msg << got[first] << ", want " << want[first];
        }
        fail(msg.str());
    }
}

//! What the partitioning log said since the last look.
struct LogTail {
    const std::ostringstream& stream;
    size_t seen = 0;

    std::string fresh()
    {
        const std::string all = stream.str();
        std::string tail = all.substr(seen);
        seen = all.size();
        return tail;
    }
};

size_t count_lines(const std::string& text, const char* prefix)
{
    size_t count = 0;
    for (size_t pos = 0; (pos = text.find(prefix, pos)) != std::string::npos; pos += std::strlen(prefix)) {
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
    const uint32_t batch_size = argc > 2 ? static_cast<uint32_t>(std::strtoul(argv[2], nullptr, 10)) : 10000;
    const bool cache_enabled = argc > 3 ? std::atoi(argv[3]) != 0 : true;
    if (nr_pairs < 256 * KVPairsChunkSize || batch_size < 1000) {
        std::cerr << "usage: " << argv[0] << " [nr_pairs >= " << 256 * KVPairsChunkSize << "] [batch_size >= 1000] [cache_enabled=1|0] [part_log_out]" << std::endl;
        return 2;
    }

    std::mt19937_64 rng{20260916};
    const auto random_value = [&] { return static_cast<value_int64_t>(rng() >> 2); };  // the non-negative half of the user range

    // Keys 8 apart, so that there is room for pairs that do not exist yet.
    std::vector<KVPair> pairs(nr_pairs);
    for (size_t i = 0; i < nr_pairs; i++) {
        pairs[i] = {static_cast<key_uint64_t>(i + 1) * 8, random_value()};
    }
    // Two and a half chunks past the middle, which is a partition boundary
    // for an even DPU count: the hot range carved around the key spans more
    // than one chunk (a single-chunk one is exempt from splitting), the key
    // sits in its last chunk (where the split fails), and the ranges of step
    // 4 stay within that chunk.
    const key_uint64_t hot_key = pairs[nr_pairs / 2 + 2 * KVPairsChunkSize + KVPairsChunkSize / 2].key;
    const auto random_initial_key = [&] { return pairs[rng() % nr_pairs].key; };

    auto log = std::make_unique<std::ostringstream>();
    LogTail log_tail{*log};
    partitioning_log = std::move(log);
    const auto served_since_last_look = [&] { return count_lines(log_tail.fresh(), "cache hits ") != 0; };

    BPForest::Param param;
    param.enable_hot_cache = cache_enabled;
    BPForest forest(pairs.data(), nr_pairs, param);
    Oracle oracle;
    for (const KVPair& pair : pairs) {
        oracle.pairs.emplace(pair.key, pair.value);
    }
    std::cout << "forest built: " << nr_pairs << " pairs, batch " << batch_size << ", cache " << cache_enabled << ", hot key " << hot_key << std::endl;

    std::vector<key_uint64_t> keys(batch_size);
    std::vector<value_int64_t> values(batch_size), values_want(batch_size);
    std::vector<KVPair> kv_queries(batch_size), kv_results(batch_size), kv_want(batch_size);
    std::vector<uint8_t> flags(batch_size), flags_want(batch_size);
    std::vector<RangeCountQuery> rcqs(batch_size);
    std::vector<uint64_t> counts(batch_size), counts_want(batch_size);

    const auto fill_get_keys = [&](uint32_t nr_hot) {
        for (uint32_t i = 0; i < batch_size; i++) {
            keys[i] = rng() % batch_size < nr_hot ? hot_key : random_initial_key();
        }
    };
    const auto get_batch = [&](const char* what, uint32_t nr_hot) {
        fill_get_keys(nr_hot);
        forest.batch_get(batch_size, keys.data(), values.data());
        oracle.get(batch_size, keys.data(), values_want.data());
        expect_same(what, batch_size, values.data(), values_want.data());
    };
    const uint32_t storm = batch_size * 2 / 5;

    // 1. A get storm on one key: one batch carves the hot range, the next
    //    fails to split it and installs the cache, the rest read from it.
    for (unsigned round = 0; round < 4; round++) {
        get_batch("get storm", storm);
    }
    {
        const std::string said = log_tail.fresh();
        const size_t nr_cached = count_lines(said, "cache hot "), nr_hits = count_lines(said, "cache hits ");
        std::cout << "get storm: cache hot x" << nr_cached << ", batches with hits " << nr_hits << std::endl;
        if (cache_enabled && nr_cached == 0) {
            fail("get storm did not install the cache");
        }
        if (cache_enabled && nr_hits == 0) {
            fail("get storm was not served from the cache");
        }
        if (!cache_enabled && (nr_cached != 0 || nr_hits != 0)) {
            fail("cache used while disabled");
        }
    }

    // 2. An insert storm on the same key with a value that keeps changing:
    //    the last one has to win, and the other keys of the batch, some new
    //    and some existing, have to land too.
    {
        for (uint32_t i = 0; i < batch_size; i++) {
            if (rng() % 10 < 3) {
                kv_queries[i] = {hot_key, random_value()};
            } else {
                kv_queries[i] = {random_initial_key() + (rng() % 2 == 0 ? 0 : 1 + rng() % 7), random_value()};
            }
        }
        forest.batch_insert(batch_size, kv_queries.data());
        oracle.insert(batch_size, kv_queries.data());
        if (cache_enabled && !served_since_last_look()) {
            fail("insert storm was not served from the cache");
        }
        for (uint32_t i = 0; i < batch_size; i++) {
            keys[i] = kv_queries[i].key;
        }
        forest.batch_get(batch_size, keys.data(), values.data());
        oracle.get(batch_size, keys.data(), values_want.data());
        expect_same("get after insert storm", batch_size, values.data(), values_want.data());
    }

    // 3. A pred storm on the key: one goes, the rest copy its answer.
    {
        for (uint32_t i = 0; i < batch_size; i++) {
            keys[i] = rng() % 10 < 3 ? hot_key : random_initial_key() + rng() % 8;
        }
        forest.batch_pred(batch_size, keys.data(), kv_results.data());
        oracle.pred(batch_size, keys.data(), kv_want.data());
        expect_same("pred storm", batch_size, kv_results.data(), kv_want.data());
        if (cache_enabled && !served_since_last_look()) {
            fail("pred storm was not served from the cache");
        }
    }

    // 4. Range counts around the key: the DPU has to hold the forwarded value.
    //    Few of them, so that the hot range is not driven into a split.
    {
        const value_int64_t hot_value = oracle.pairs.at(hot_key);
        for (uint32_t i = 0; i < batch_size; i++) {
            if (rng() % 100 == 0) {
                rcqs[i] = {{hot_key - rng() % 64, hot_key + rng() % 64}, hot_value};
            } else {
                const key_uint64_t begin = random_initial_key();
                const auto it = oracle.pairs.lower_bound(begin + rng() % 100);
                rcqs[i] = {{begin, begin + 200}, it == oracle.pairs.end() ? hot_value : it->second};
            }
        }
        forest.batch_range_count(batch_size, rcqs.data(), counts.data());
        oracle.range_count(batch_size, rcqs.data(), counts_want.data());
        expect_same("range count", batch_size, counts.data(), counts_want.data());
    }

    // 5. A delete storm on the key, duplicates of other keys mixed in: one
    //    deletion is reported per key, the cache lets go of the pair, and
    //    the key reads as absent afterwards.
    {
        for (uint32_t i = 0; i < batch_size; i++) {
            keys[i] = rng() % 10 < 3 ? hot_key : pairs[nr_pairs / 4 + rng() % 200].key;
        }
        forest.batch_delete(batch_size, keys.data(), flags.data());
        oracle.remove(batch_size, keys.data(), flags_want.data());
        expect_same("delete storm", batch_size, flags.data(), flags_want.data());
        const std::string said = log_tail.fresh();
        if (cache_enabled && count_lines(said, "cache hits ") == 0) {
            fail("delete storm was not served from the cache");
        }
        if (cache_enabled && count_lines(said, "cache evict ") == 0) {
            fail("deleting the cached pair did not evict it");
        }

        // A few reads of the gone key are enough to catch a stale slot; a
        // storm would fail the hot's split with nothing to cache and set
        // the flag that keeps the key from being examined after reinsertion.
        get_batch("get after delete storm", 8);
        if (served_since_last_look()) {
            fail("get served from the cache after the pair was deleted");
        }
    }

    // 6. The key comes back and the storm resumes: it has to be cached again.
    {
        for (uint32_t i = 0; i < batch_size; i++) {
            kv_queries[i] = {rng() % 10 < 3 ? hot_key : random_initial_key(), random_value()};
        }
        forest.batch_insert(batch_size, kv_queries.data());
        oracle.insert(batch_size, kv_queries.data());
        rcqs[0] = {{hot_key - 8, hot_key + 8}, oracle.pairs.at(hot_key)};
        forest.batch_range_count(1, rcqs.data(), counts.data());
        if (counts[0] != 1) {
            fail("the DPU does not hold the reinserted pair");
        }
        for (unsigned round = 0; round < 4; round++) {
            get_batch("get storm after reinsertion", storm);
        }
        const std::string said = log_tail.fresh();
        std::cout << "reinsertion: cache hot x" << count_lines(said, "cache hot ") << ", batches with hits " << count_lines(said, "cache hits ") << std::endl;
        if (cache_enabled && count_lines(said, "cache hits ") == 0) {
            fail("the reinserted key was not cached again");
        }
    }

    // 7. A full resharding lets go of the pair, so that a value written while
    //    the key is out of the cache is what gets read afterwards.  The keys
    //    only serve as the load sample; nothing is answered.
    {
        fill_get_keys(storm);
        forest.partition_with_get_batch(batch_size, keys.data(), values.data());
        if (cache_enabled && count_lines(log_tail.fresh(), "cache evict ") == 0) {
            fail("full resharding did not drop the cached pair");
        }
        kv_queries[0] = {hot_key, random_value()};
        forest.batch_insert(1, kv_queries.data());
        oracle.insert(1, kv_queries.data());
        get_batch("get after resharding", storm);
    }

    // 8. Everything else is intact.
    get_batch("final sweep", 0);
    for (uint32_t i = 0; i < batch_size; i++) {
        keys[i] = random_initial_key() + rng() % 8;
    }
    forest.batch_pred(batch_size, keys.data(), kv_results.data());
    oracle.pred(batch_size, keys.data(), kv_want.data());
    expect_same("final pred sweep", batch_size, kv_results.data(), kv_want.data());

    if (argc > 4) {
        std::ofstream{argv[4]} << log_tail.stream.str();
    }
    if (nr_failures != 0) {
        std::cout << nr_failures << " failure(s)" << std::endl;
        return 1;
    }
    std::cout << "all passed" << std::endl;
    return 0;
}
