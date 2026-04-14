/// @file find_relatively_hot_ranges_single_range_naive.hpp
/// @brief 論文 Algorithm 3 の第2スキャンを、単一コールドパーティション前提で
///        そのまま愚直実装したもの。
///        bpforest.ipp の最適化版と同一シグネチャ・同一 callback 規約。
///        sliding window 最適化を使わず、各ステップで全走査する。
///        参照用であり、ビルド対象ではない。
///
/// 単一コールドパーティション前提:
///   begin_range + 1 == end_range。
///   この前提の下では、走査空間は単一 range 上の連続 chunk 列になり、
///   既存 hot range h* は空 (または走査空間外) とみなせる。
///   したがって bpforest.ipp との意味論差分 (slot-based 不連続窓) は消え、
///   論文の Algorithm 3 をそのまま実装できる。

#pragma once

#include "pairs_range.hpp"

#include <array>
#include <cassert>

// ── helper: SizeOf({c_i}_{i=a}^{b}) ─────────────────────
// chunk [a, b) (半開) の npairs 合計を毎回走査して返す。
inline uint32_t size_of(DataChunkIterator a, DataChunkIterator b)
{
    uint32_t sum = 0;
    for (DataChunkIterator it = a; it != b; ++it)
        sum += it->npairs();
    return sum;
}

// ── helper: NQrys({c_i}_{i=a}^{b}) ─────────────────────
// chunk [a, b) (半開) の load 合計を毎回走査して返す。
inline uint32_t nqrys(DataChunkIterator a, DataChunkIterator b)
{
    uint32_t sum = 0;
    for (DataChunkIterator it = a; it != b; ++it)
        sum += it->load();
    return sum;
}

// ── Algorithm 3 第2スキャン: argmax + carve-out (単一 range 愚直版) ──
//
// 引数と論文記号の対応:
//   hot_npairs  ≡ ceil((1/α)(D/P))  ── 1 hot range のペア数幅
//   nr_hots     ≡ β                  ── 選出する relative hot range 数
//
// 論文の添字は閉区間 {c_i}_{i=l}^{r} だが、コード上の iterator は
// 半開区間 [l, r+1) で扱う。hot_hook(range, hot_range, load) の
// hot_range も半開端 (= 既存 bpforest.ipp と同一規約)。
//
template <typename Func /* bool(ChunkedPairsRange&, PairsRange, load) */>
inline std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>
find_relatively_hot_ranges(
    LinkedList<ChunkedPairsRange>::iterator begin_range,
    LinkedList<ChunkedPairsRange>::iterator end_range,
    uint32_t hot_npairs, dpu_id_t nr_hots,
    Func&& hot_hook)
{
    using RangeIter = LinkedList<ChunkedPairsRange>::iterator;

    assert(begin_range != end_range);
    assert(nr_hots != 0);
    {
        RangeIter next_range = begin_range;
        ++next_range;
        assert(next_range == end_range);
    }

    ChunkedPairsRange& range = *begin_range;
    const DataChunkIterator s = range.begin();       // Alg.3: b = {c_i}_{i=s}^{e}
    const DataChunkIterator e = range.end();

    DataChunkIterator argmax_left = s;
    DataChunkIterator argmax_right = s;

    // ── Phase 1: Argmax スキャン (Alg.3 L1391-L1397) ──────
    //
    // h* は空 (または走査空間外) なので、論文の
    //   NQrys({c_i}_{i=l}^{r} \ h*)
    // はそのまま NQrys({c_i}_{i=l}^{r}) になる。
    DataChunkIterator l = s;                         // Alg.3 L1391: (l, l0, r0) ← (s, s, s)
    DataChunkIterator l0 = s;
    DataChunkIterator r0 = s;
    uint32_t best_load = 0;

    for (DataChunkIterator r = s; r != e; ++r) {    // Alg.3 L1392: for r ← s to e

        // ── Alg.3 L1393: l' ← max{ l' ∈ (l, r] | SizeOf >= nr_hots * hot_npairs }
        //    候補なしなら l' は無効 (= l 自身)。
        //    右から左へ走査し、最初に条件を満たした候補が max。
        DataChunkIterator l_prime = l;
        {
            DataChunkIterator cand = r;
            while (cand != l) {                      // cand ∈ {r, r-1, ..., l+1}
                if (size_of(cand, r + 1)
                    >= static_cast<uint32_t>(nr_hots) * hot_npairs) {
                    l_prime = cand;
                    break;
                }
                --cand;
            }
        }

        // ── Alg.3 L1394: l ← max{l, l'}
        if (l < l_prime) {
            l = l_prime;
        }

        // ── Alg.3 L1395-L1396: if NQrys({c_i}_{i=l}^{r}) > NQrys({c_i}_{i=l0}^{r0})
        const uint32_t load = nqrys(l, r + 1);
        if (load > best_load) {
            best_load = load;
            l0 = l;
            r0 = r + 1;                              // 半開端で保持
        }
    }

    argmax_left = l0;
    argmax_right = r0;

    // ── Phase 2: Carve-out (Alg.3 L1399-L1404) ───────────
    // argmax 位置 [argmax_left, argmax_right) を右→左に hot_npairs ペアずつ分割し、
    // 各部分を hot_hook に渡す。単一 range 前提なので range 境界は跨がない。
    DataChunkIterator r = argmax_right;              // Alg.3 L1399: r ← r0
    while (argmax_left < r) {                        // Alg.3 L1400: while l0 <= r
        DataChunkIterator l_hot = r;
        uint32_t load = 0;
        PairsRange hot_range{r->begin(), r->begin()};

        while (argmax_left < l_hot && hot_range.npairs() < hot_npairs) {  // Alg.3 L1401
            --l_hot;
            hot_range = PairsRange{l_hot->begin(), hot_range.end()};
            load += l_hot->load();
        }

        // ── Alg.3 L1402: l ← max{l0, l}
        if (l_hot < argmax_left) {
            l_hot = argmax_left;
        }

        // ── Alg.3 L1403: h ← h ∪ ({c_i}_{i=l}^{r} \ h*) \ {∅}
        if (!hot_hook(range, hot_range, load)) {
            return std::array<std::pair<RangeIter, DataChunkIterator>, 2>{
                {{begin_range, l_hot}, {begin_range, argmax_right}}};
        }

        r = l_hot;                                   // Alg.3 L1404: r ← l - 1 (半開区間では r ← l)
    }

    return std::array<std::pair<RangeIter, DataChunkIterator>, 2>{
        {{begin_range, argmax_left}, {begin_range, argmax_right}}};
}
