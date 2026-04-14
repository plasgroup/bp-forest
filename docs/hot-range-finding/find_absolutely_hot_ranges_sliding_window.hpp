/// @file find_absolutely_hot_ranges_sliding_window.hpp
/// @brief 論文 Algorithm 2 (Greedy hot range selection) の sliding window 最適化版。
///        naive 版 (find_absolutely_hot_ranges_naive.hpp) と同一シグネチャ・同一 callback 規約。
///        SizeOf / NQrys の都度全走査をやめ、各 chunk を高々定数回だけ触る O(N)。
///        参照用であり、ビルド対象ではない。

#pragma once

#include "pairs_range.hpp"

// ── Algorithm 2: Greedy hot range selection (sliding window 最適化版) ─────
//
// 引数と論文記号の対応:
//   hot_npairs  ≡ ceil((1/α)(D/P))  ── ウィンドウ幅閾値 (ペア数)
//   hot_nqrys   ≡ ceil(Q/P)         ── クエリ負荷閾値
//
// 論文の添字は閉区間 {c_i}_{i=l}^{r} だが、コード上の iterator は
// 半開区間 [l, r+1) で扱う。hot_hook(part, left, right, load) の
// right も半開端 (= 既存 bpforest.ipp と同一規約)。
//
// ── 最適化の概要 ─────────────────────────────────
//
// naive 版では r ごとに以下を全走査していた:
//   (a) l' の探索: (l, r] を右→左へ走査して SizeOf >= hot_npairs を満たす最大候補を探す
//   (b) NQrys の計算: [l, r] を走査して load の合計を取る
//
// sliding window 版では半開区間 [l, r+1) の SizeOf と NQrys を
// window_npairs / window_load として保持し、r の前進で加算、
// l の前進で減算することで、各集約値を O(1) で更新する。
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

        // [最適化] naive 版の size_of / nqrys 全走査を、区間 [l, r+1) の
        // 集約値として保持する。r の前進で加算、l の前進で減算。
        uint32_t window_npairs = 0;
        uint32_t window_load = 0;

        for (DataChunkIterator r = s; r != e; ++r) { // Alg.2 L1176: for r ← s to e

            // [最適化] 右端 r を 1 chunk 伸ばす: naive 版では r ごとに
            // size_of(cand, r+1) / nqrys(l, r+1) を全走査していたが、
            // ここでは新規 chunk ぶんだけ集約値を加算する。
            window_npairs += r->npairs();
            window_load += r->load();

            // ── Alg.2 L1177-1178: l' の探索と l ← max{l, l'}
            //    naive 版: (l, r] を右→左走査して SizeOf >= hot_npairs の max を探す。
            //    [最適化] 「l を外しても hot_npairs を保てる限り l を前進させる」
            //    ことで、l' の探索と l への反映を同時に O(1) amortized で行う。
            while (window_npairs - l->npairs() >= hot_npairs) {
                window_npairs -= l->npairs();
                window_load -= l->load();
                ++l;
            }

            // ── Alg.2 L1179: if NQrys({c_i}_{i=l}^{r}) >= Q/P
            //    [最適化] naive 版の nqrys(l, r+1) を window_load の O(1) 参照に。
            if (window_load >= hot_nqrys) {
                // ── Alg.2 L1180: h ← h ∪ {…}
                if (!hot_hook(*part, l, r + 1, window_load)) {
                    return;
                }
                // ── Alg.2 L1181: l ← r + 1
                //    [最適化] window を空に reset して次の区間へ進む。
                l = r + 1;
                window_npairs = 0;
                window_load = 0;
            }
        }
    }
}
