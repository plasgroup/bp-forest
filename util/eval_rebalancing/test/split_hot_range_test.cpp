#include "split_hot_range.hpp"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <vector>

namespace
{
struct Piece {
    PairsRange pairs;
    KeyRange keys;
    uint32_t load;
    uint32_t max_chunk_load;
};

// Builds `chunk_loads.size()` full chunks of KVPairs (keys = global pair
// index, so they are strictly increasing) and runs the splitter over them.
struct Harness {
    std::vector<KVPair> pairs;
    std::vector<uint32_t> load_ary;
    ChunkedPairsRange hot;

    explicit Harness(const std::vector<uint32_t>& chunk_loads)
    {
        const size_t nchunks = chunk_loads.size();
        pairs.resize(nchunks * KVPairsChunkSize);
        for (size_t i = 0; i < pairs.size(); i++) {
            pairs[i].key = i;
            pairs[i].value = 0;
        }
        load_ary.assign(nchunks, 0);
        hot = ChunkedPairsRange{PairsRange{&pairs[0], &pairs[0] + pairs.size()}};
        hot.set_load_ary(&load_ary[0]);
        // Distribute each chunk's load onto its first pair's key so that the
        // per-chunk accumulation reproduces chunk_loads exactly.
        uint32_t* cursor = &load_ary[0];
        for (size_t c = 0; c < nchunks; c++) {
            cursor[c] = chunk_loads[c];
        }
    }

    uint32_t total_load() const
    {
        uint32_t s = 0;
        for (uint32_t v : load_ary) {
            s += v;
        }
        return s;
    }

    std::vector<Piece> split(dpu_id_t target_pieces, dpu_id_t& emit_count, uint32_t total)
    {
        std::vector<Piece> out;
        emit_count = split_hot_range_equal_load(hot, total, target_pieces, KEY_MAX,
            [&](PairsRange pr, KeyRange kr, uint32_t ld, uint32_t mcl) {
                out.push_back(Piece{pr, kr, ld, mcl});
            });
        assert(emit_count == out.size());
        return out;
    }
};

// (b) full coverage, no gaps, contiguous & complete KeyRanges.
void check_coverage(const Harness& h, const std::vector<Piece>& pieces)
{
    assert(!pieces.empty());
    assert(pieces.front().pairs.begin() == h.hot.PairsRange::begin());
    assert(pieces.back().pairs.end() == h.hot.PairsRange::end());
    assert(pieces.front().keys.begin == h.pairs.front().key);
    assert(pieces.back().keys.end == KEY_MAX);
    for (size_t i = 1; i < pieces.size(); i++) {
        assert(pieces[i].pairs.begin() == pieces[i - 1].pairs.end());
        assert(pieces[i].keys.begin == pieces[i - 1].keys.end + 1);
    }
}

void test_uniform_load()
{
    // 8 chunks, load 1 each, target 4 -> target_load = 2 -> 4 pieces of 2.
    Harness h(std::vector<uint32_t>(8, 1));
    dpu_id_t emit;
    const uint32_t total = h.total_load();
    const auto pieces = h.split(4, emit, total);
    assert(emit == 4);
    check_coverage(h, pieces);
    for (const auto& p : pieces) {
        assert(p.load == 2);
        assert(p.max_chunk_load == 1);
        // (c) bounded: load < target_load + max_chunk_load
        assert(p.load < 2u + p.max_chunk_load);
    }
}

void test_single_chunk()
{
    // (e) one chunk cannot be subdivided -> emit_count == 1.
    Harness h({42});
    dpu_id_t emit;
    const auto pieces = h.split(4, emit, h.total_load());
    assert(emit == 1);
    check_coverage(h, pieces);
    assert(pieces[0].load == 42);
    assert(pieces[0].max_chunk_load == 42);
}

void test_target_exceeds_chunks()
{
    // (d) target_pieces > nr_chunks -> emit_count <= nr_chunks.
    Harness h(std::vector<uint32_t>(3, 5));
    dpu_id_t emit;
    const auto pieces = h.split(10, emit, h.total_load());
    assert(emit <= 3);
    check_coverage(h, pieces);
}

void test_skewed_load()
{
    // (g) one dominant chunk -> fewer pieces than requested, still full
    // coverage; the heavy chunk's piece stays whole.
    Harness h({1, 1, 100, 1, 1});
    dpu_id_t emit;
    const uint32_t total = h.total_load();  // 104
    const auto pieces = h.split(4, emit, total);  // target_load = 26
    assert(emit >= 1 && emit <= 5);
    assert(emit < 4);  // cannot reach 4 pieces: load is concentrated
    check_coverage(h, pieces);
    uint32_t sum = 0, observed_max = 0;
    for (const auto& p : pieces) {
        sum += p.load;
        observed_max = std::max(observed_max, p.max_chunk_load);
        // (c) invariant maintained even when emit_count < target_pieces.
        const uint32_t target_load = (total + 4 - 1) / 4;
        assert(p.load < target_load + p.max_chunk_load);
    }
    assert(sum == total);          // load conserved
    assert(observed_max == 100);   // (f) heavy chunk surfaced
}

void test_max_chunk_load_per_piece()
{
    // (f) each piece reports the max single-chunk load within it.
    Harness h({3, 7, 2, 9, 1, 4});
    dpu_id_t emit;
    const uint32_t total = h.total_load();  // 26
    const auto pieces = h.split(3, emit, total);  // target_load = 9
    check_coverage(h, pieces);
    uint32_t sum = 0;
    for (const auto& p : pieces) {
        sum += p.load;
        assert(p.max_chunk_load <= p.load);
        assert(p.max_chunk_load >= 1);
    }
    assert(sum == total);
}

void test_emit_count_one_return()
{
    // (e) return value is exactly 1 for the degenerate single-piece case.
    Harness h(std::vector<uint32_t>(4, 1));
    dpu_id_t emit;
    // target_pieces = 1 -> target_load = total -> single trailing piece.
    const auto pieces = h.split(1, emit, h.total_load());
    assert(emit == 1);
    check_coverage(h, pieces);
    assert(pieces[0].load == 4);
}
}  // namespace

int main()
{
    test_uniform_load();
    test_single_chunk();
    test_target_exceeds_chunks();
    test_skewed_load();
    test_max_chunk_load_per_piece();
    test_emit_count_one_return();
    std::puts("split_hot_range_test passed");
    return 0;
}
