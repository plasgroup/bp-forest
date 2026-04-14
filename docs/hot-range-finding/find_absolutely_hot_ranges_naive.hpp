/// @file find_absolutely_hot_ranges_naive.hpp
/// @brief 論文 Algorithm 2 (Greedy hot range selection) の愚直実装。
///        bpforest.ipp の最適化版と同一シグネチャ・同一 callback 規約。
///        sliding window を使わず、各ステップで全走査する。
///        参照用であり、ビルド対象ではない。

#pragma once

#include "pairs_range.hpp"

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

// ── Algorithm 2: Greedy hot range selection (愚直版) ─────
//
// 引数と論文記号の対応:
//   hot_npairs  ≡ ceil((1/α)(D/P))  ── ウィンドウ幅閾値 (ペア数)
//   hot_nqrys   ≡ ceil(Q/P)         ── クエリ負荷閾値
//
// 論文の添字は閉区間 {c_i}_{i=l}^{r} だが、コード上の iterator は
// 半開区間 [l, r+1) で扱う。hot_hook(part, left, right, load) の
// right も半開端 (= 既存 bpforest.ipp と同一規約)。
//
template <typename HotHook>
inline void find_absolutely_hot_ranges(
    LinkedChunkedPairsRange* const begin_part,
    LinkedChunkedPairsRange* const end_part,
    uint32_t hot_npairs, uint32_t hot_nqrys,
    HotHook&& hot_hook)
{
    for (LinkedChunkedPairsRange* part = begin_part;
         part != end_part; ++part)
    {
        const DataChunkIterator s = part->begin();   // Alg.2: b = {c_i}_{i=s}^{e}
        const DataChunkIterator e = part->end();
        DataChunkIterator l = s;                     // Alg.2 L1175: l ← s

        for (DataChunkIterator r = s; r != e; ++r) { // Alg.2 L1176: for r ← s to e

            // ── Alg.2 L1177: l' ← max{ l' ∈ (l, r] | SizeOf >= hot_npairs }
            //    (l, r] は l を含まない。候補なしなら l' は無効 (= -∞)。
            //    右から左へ走査し、最初に条件を満たした候補が max。
            DataChunkIterator l_prime = l;           // 無効値 (= l 自身: 後段の l < l_prime で弾かれる)
            {
                DataChunkIterator cand = r;
                while (cand != l) {                  // cand ∈ {r, r-1, ..., l+1}
                    if (size_of(cand, r + 1) >= hot_npairs) {
                        l_prime = cand;
                        break;
                    }
                    --cand;
                }
            }

            // ── Alg.2 L1178: l ← max{l, l'}
            if (l < l_prime) {
                l = l_prime;
            }

            // ── Alg.2 L1179: if NQrys({c_i}_{i=l}^{r}) >= Q/P
            const uint32_t load = nqrys(l, r + 1);
            if (load >= hot_nqrys) {
                // ── Alg.2 L1180: h ← h ∪ {…}
                if (!hot_hook(*part, l, r + 1, load)) {
                    return;
                }
                // ── Alg.2 L1181: l ← r + 1
                l = r + 1;
            }
        }
    }
}
