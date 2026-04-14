/// @file find_relatively_hot_ranges_sliding_window.hpp
/// @brief 論文 Algorithm 3 第2スキャン (argmax + carve-out) の sliding window 最適化版参照実装。
///        bpforest.ipp::find_relatively_hot_ranges と同一シグネチャ・同一 callback 規約。
///        naive 版 (find_relatively_hot_ranges_naive.hpp) と同一の意味論 (effective pair
///        count semantics + 境界例外) を保ったまま、Phase 1 の argmax スキャン内ループを
///        「l を単調前進させる両端 2 ポインタ」に置き換え、全体を amortized O(N + nr_hots)
///        程度に抑える。ビルド対象ではない (参照用)。
///
/// ── naive からの変更点 ───────────────────────────
/// naive 版:
///   for r in all chunks:
///     for cand = r, r-1, ..., l+1:   (全走査)
///       if effective_window_pair_count(cand, r+1) >= required_npairs: l' = cand; break
///       elif cand == cand_range->begin() and effective_npairs(cand, r+1) == required_npairs:
///         l' = cand; break
///     l = max(l, l')
///     argmax を window_load(l, r+1) で更新
///
/// sliding 版:
///   for r in all chunks:
///     (r を 1 chunk 伸ばす: 状態を差分更新)
///     while l を 1 chunk 右に動かしても condition が保たれる:
///       (l を 1 chunk 前進: 状態を差分更新)
///     argmax を nqrys_in_window で更新
///
/// 単調性: ウィンドウを左に広げる操作に対して effective_window_pair_count は単調増加
/// なので、r が 1 chunk 進むたびに「condition を満たす最大 l'」は右方向にしか動かない。
/// よって l を 1 方向ポインタとして再利用できる。
///
/// ── bpforest.ipp に入っていて本ファイルに入れていない最適化 ──
///   - bootstrap (最初の右端初期化を cold range 単位で一気に進める)
///   - single-range fast path (l_range == r_range かつ max_nqrys_in_window 未更新時の早抜け)
///   - quota delta aggregation (`left_window_offcut_to_shrink` による遅延適用)
///   - margin compression (`while (right != right_range->end() && right_npairs_offcut + right->npairs() < right_window_offcut)`)
///   - range-level left shrink (`while (left_window_offcut <= left_window_offcut_to_shrink)` の range 単位バルク進み)
/// これらは学習者が段階的に追えるように別ファイル (opt1, opt2, ...) に分解する想定。
///
/// ── 論文との意味論の差分 ──────────────────────────────
/// naive.hpp のヘッダコメントを参照。要点のみ再掲:
///   - 入力は greedy pass 後の cold range list。ウィンドウは cold range を不連続に跨ぐ
///     半開区間 [l, r+1)。
///   - Alg.3 L1393 の pair 数下限は "effective pair count" 比較:
///       effective_window_pair_count(l, r+1)
///         = raw_npairs(left_partial)
///         + Σ slot_rounded_npairs(middle_range)
///         + slot_rounded_npairs(right_partial)
///     を hot_npairs * nr_hots と比較する。
///   - 境界例外の存在理由: argmax loop の目的は「carve_out_hot_ranges が丁度 nr_hots 個
///     の hot range を切り出せる最大荷重ウィンドウ」の発見であり、raw 比較だけでは
///     l_range が undersize (raw < slot_rounded) のとき l を l_range->begin() で止め損ね、
///     次 step で l_range が "完全通過 middle" に格下げされて carve 数が nr_hots + α に
///     膨らむ。これを避けるため、新 l が new_l_range->begin() に位置し
///     `slot_rounded_eff (= left も slot_rounded) == required_npairs` が成立する瞬間だけ
///     追加で l' に採用する。`>` のケースは raw 比較で既に検出済みなので、例外で
///     救うべきは構造的に `==` のケースだけ (詳細証明は naive.hpp)。

#pragma once

#include "pairs_range.hpp"

#include <array>
#include <cassert>
#include <iterator>

namespace relatively_hot_ranges_sliding_window_detail {

inline uint32_t ceil_div_u32(const uint32_t num, const uint32_t den)
{
    return (num + den - 1) / den;
}

// fully covered fragment が消費する effective pair 数。
// hot_npairs 単位に切り上げる (論文 SizeOf の cold-range-list 拡張)。
inline uint32_t slot_rounded_npairs(const uint32_t npairs, const uint32_t hot_npairs)
{
    return ceil_div_u32(npairs, hot_npairs) * hot_npairs;
}

inline uint32_t range_npairs(const LinkedList<ChunkedPairsRange>::iterator range)
{
    return static_cast<uint32_t>(range->npairs());
}

}  // namespace relatively_hot_ranges_sliding_window_detail

// ── Algorithm 3 第2スキャン: argmax + carve-out (sliding window 版) ──
//
// 引数と論文記号の対応:
//   hot_npairs  ≡ ceil((1/α)(D/P))     ── 1 hot range のペア数幅
//   nr_hots     ≡ β                     ── 選出する relative hot range 数
//
// 論文の閉区間 {c_i}_{i=l}^{r} を半開区間 [l, r+1) として扱う。
template <typename Func /* bool(ChunkedPairsRange&, PairsRange, load) */>
inline std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>
find_relatively_hot_ranges(const LinkedList<ChunkedPairsRange>::iterator begin_range, const LinkedList<ChunkedPairsRange>::iterator end_range,
    const uint32_t hot_npairs, const dpu_id_t nr_hots,
    Func&& hot_hook)
{
    using namespace relatively_hot_ranges_sliding_window_detail;
    using RangeIter = LinkedList<ChunkedPairsRange>::iterator;

    assert(begin_range != end_range);
    assert(nr_hots != 0);

    RangeIter argmax_left_range = begin_range, argmax_right_range = begin_range;
    DataChunkIterator argmax_left = begin_range->begin(), argmax_right = argmax_left;

    // ── Phase 2: Carve-out (Alg.3 L1399-L1404) ─────────────
    // argmax 位置の区間 [argmax_left, argmax_right) を右→左に hot_npairs ペアずつ
    // 束ねて hot_hook に渡す。cold range 境界を跨ぐときは 1 range ごとに callback を
    // 呼び直す (callback 規約 bool(ChunkedPairsRange&, PairsRange, load) のため)。
    // 本関数は naive.hpp から逐語流用。
    const auto carve_out_hot_ranges = [&] {
        RangeIter range = argmax_right_range;
        DataChunkIterator chunk = argmax_right;              // Alg.3 L1399: r ← r0

        for (;;) {
            // Alg.3 L1402 max{l0, l} を limit に吸収:
            // 左端 range では limit == argmax_left、それ以外では range->begin()。
            const DataChunkIterator limit = (range == argmax_left_range ? argmax_left : range->begin());
            while (limit < chunk) {
                uint32_t load = 0;
                PairsRange hot_range{chunk->begin(), chunk->begin()};
                while (limit < chunk && hot_range.npairs() < hot_npairs) {  // Alg.3 L1401
                    --chunk;
                    hot_range = PairsRange{chunk->begin(), hot_range.end()};
                    load += chunk->load();
                }
                if (!hot_hook(*range, hot_range, load)) {     // Alg.3 L1403
                    return std::array<std::pair<RangeIter, DataChunkIterator>, 2>{
                        {{range, chunk}, {argmax_right_range, argmax_right}}};
                }
            }

            if (range == argmax_left_range)
                break;

            --range;                                          // 1 range 左へ繰り上がる
            chunk = range->end();
        }

        return std::array<std::pair<RangeIter, DataChunkIterator>, 2>{
            {{argmax_left_range, argmax_left}, {argmax_right_range, argmax_right}}};
    };

    // ── Phase 1: Argmax スキャン (Alg.3 L1391-L1397) ───────
    //
    // 状態変数と不変条件 (ウィンドウ [l, r_end) に対して):
    //
    //   nqrys_in_window      = Σ chunk.load()  for chunks in [l, r_end)
    //
    //   単一 range (l_range == r_range) の場合:
    //     left_npairs_offcut   = raw pair count of [l, r_end)
    //     non_left_effective   = 0
    //     right_npairs_offcut  = 0   (未使用)
    //     right_window_offcut  = 0   (未使用)
    //     window_effective     = left_npairs_offcut
    //
    //   複数 range (l_range != r_range) の場合:
    //     left_npairs_offcut   = raw pair count of [l, l_range->end())
    //     right_npairs_offcut  = raw pair count of [r_range->begin(), r_end)
    //     right_window_offcut  = slot_rounded_npairs(right_npairs_offcut)
    //     non_left_effective   = right_window_offcut
    //                          + Σ_{m = next(l_range)}^{prev(r_range)} slot_rounded(m->npairs())
    //     window_effective     = left_npairs_offcut + non_left_effective
    //
    //   いずれの場合も
    //     window_effective == effective_window_pair_count(l_range, l, r_range, r_end, hot_npairs)
    //   が naive.hpp の関数と等しい。
    //
    // 判定:
    //   raw 条件       : window_effective >= required_npairs
    //   boundary 例外  : 新 l が (新) l_range->begin() に位置し、
    //                    slot_rounded(left_npairs_offcut) + non_left_effective == required_npairs
    //                    のとき。carve 数を nr_hots に抑えるための救済 (naive.hpp 参照):
    //                    ここで止めないと次 step で旧 l_range が完全通過 middle に格下げされ、
    //                    carve が nr_hots + α 個の hot range を切り出してしまう。

    const uint32_t required_npairs = static_cast<uint32_t>(nr_hots) * hot_npairs;

    RangeIter l_range = begin_range;                          // Alg.3 L1391: l ← s
    DataChunkIterator l = l_range->begin();

    uint32_t nqrys_in_window = 0;
    uint32_t best_load = 0;

    uint32_t left_npairs_offcut = 0;
    uint32_t right_npairs_offcut = 0;
    uint32_t right_window_offcut = 0;
    uint32_t non_left_effective = 0;

    for (RangeIter r_range = begin_range;                     // Alg.3 L1392: for r ← s to e
         r_range != end_range; ++r_range)
    {
        // 新しい right_range に入った瞬間、前 right_range の slot_rounded 寄与は
        // 既に non_left_effective に積まれている (middle range 扱いに遷移)。
        // 新 r_range の right_partial はゼロからカウントする。
        // (初回 r_range == begin_range のときも l_range == r_range なので右側状態は未使用。)
        right_npairs_offcut = 0;
        right_window_offcut = 0;

        for (DataChunkIterator r = r_range->begin();
             r != r_range->end(); ++r)
        {
            const DataChunkIterator r_end = r + 1;            // 半開端

            // ── Step A: r を 1 chunk 伸ばす (状態の差分更新) ──
            nqrys_in_window += r->load();

            if (l_range == r_range) {
                left_npairs_offcut += static_cast<uint32_t>(r->npairs());
                // non_left_effective == 0 のまま
                // window_effective == left_npairs_offcut
            } else {
                right_npairs_offcut += static_cast<uint32_t>(r->npairs());
                const uint32_t new_right_window_offcut =
                    slot_rounded_npairs(right_npairs_offcut, hot_npairs);
                non_left_effective += new_right_window_offcut - right_window_offcut;
                right_window_offcut = new_right_window_offcut;
                // window_effective == left_npairs_offcut + non_left_effective
            }

            // ── Step B: l を単調前進させる (Alg.3 L1393-L1394 : l ← max{l, l'}) ──
            //
            // naive 版は cand を右→左に全走査していたが、単調性 (l' は r の進行に対して
            // 右にしか動かない) により l を 1 方向ポインタとして再利用できる。
            //
            // 各ステップで「l を 1 chunk 前進させても condition を満たすか」を
            // 差分計算で判定し、成立するなら commit して次のステップへ進む。
            //
            // condition:
            //   raw             : window_effective(after pop) >= required_npairs
            //   exception       : pop が cold range 境界を跨ぐ (新 l が new_l_range->begin())
            //                      かつ slot_rounded_eff(after pop) == required_npairs。
            //                      carve 数を丁度 nr_hots に抑えるための救済であり、
            //                      ここで止めないと更に l が前進した次 step で旧 l_range が
            //                      完全通過 middle に格下げされて carve が nr_hots を超える
            //                      (naive.hpp ヘッダの「境界例外: なぜ必要か」参照)。
            for (;;) {
                //  ---- 単一 range (l_range == r_range) 内での前進 ----
                if (l_range == r_range) {
                    const uint32_t popped = static_cast<uint32_t>(l->npairs());
                    if (left_npairs_offcut - popped < required_npairs)
                        break;  // raw 条件不成立 (単一 range では exception なし: 新 l は中途位置)
                    // commit
                    nqrys_in_window -= l->load();
                    left_npairs_offcut -= popped;
                    ++l;
                    // window_effective == left_npairs_offcut (不変条件を保つ)
                    continue;
                }

                //  ---- 複数 range, l が l_range の途中にある場合 ----
                //  前進しても range 境界を跨がない: left_npairs_offcut が raw で残存する。
                if (left_npairs_offcut > static_cast<uint32_t>(l->npairs())) {
                    const uint32_t popped = static_cast<uint32_t>(l->npairs());
                    // new window_effective = (left_npairs_offcut - popped) + non_left_effective
                    if (left_npairs_offcut - popped + non_left_effective < required_npairs)
                        break;  // raw 不成立, exception 対象外 (新 l は中途位置)
                    // commit
                    nqrys_in_window -= l->load();
                    left_npairs_offcut -= popped;
                    ++l;
                    // non_left_effective 不変、window_effective は popped 分減少
                    continue;
                }

                //  ---- 複数 range, l が l_range の最終 chunk にある (前進で境界を跨ぐ) ----
                //  新 l は next(l_range)->begin()。2 ケース:
                //    (a) next(l_range) == r_range → 単一 range に合流
                //    (b) next(l_range) != r_range → 依然複数 range
                const RangeIter next_l_range = std::next(l_range);

                if (next_l_range == r_range) {
                    // (a) 合流後は右側状態が左側に吸収される。
                    //     new_left_npairs_offcut = right_npairs_offcut
                    //     new_non_left_effective = 0
                    //     new_window_effective   = right_npairs_offcut
                    //     new_l                  = r_range->begin() (= next_l_range->begin())
                    const uint32_t new_window_effective = right_npairs_offcut;
                    bool commit;
                    if (new_window_effective >= required_npairs) {
                        commit = true;  // raw OK
                    } else {
                        // exception: 新 l は new_l_range (== r_range) の先頭に位置するので
                        // 発火条件を満たす。この 1 chunk の前進で止めないと次 step で旧 l_range
                        // が完全通過 middle に格下げされ carve 数が nr_hots を超える。
                        //   slot_rounded_eff = slot_rounded(right_npairs_offcut) + 0
                        //                    = right_window_offcut
                        // が丁度 required なら carve は nr_hots 個ぴったりで終わる。
                        commit = (right_window_offcut == required_npairs);
                    }
                    if (!commit)
                        break;
                    // commit: l_range のすべての残 chunk (= 現 l 以降) の load を剥がす
                    for (DataChunkIterator c = l; c != l_range->end(); ++c)
                        nqrys_in_window -= c->load();
                    l_range = next_l_range;
                    l = l_range->begin();
                    left_npairs_offcut = right_npairs_offcut;
                    right_npairs_offcut = 0;
                    right_window_offcut = 0;
                    non_left_effective = 0;
                    // window_effective == left_npairs_offcut を維持
                    continue;
                }

                // (b) 依然複数 range。
                //     new_left_npairs_offcut = raw(next_l_range)
                //     new_non_left_effective = non_left_effective - slot_rounded(raw(next_l_range))
                //     new_window_effective   = new_left + new_non_left
                //                            = raw(next_l_range) + non_left_effective
                //                              - slot_rounded(raw(next_l_range))
                //     new_l                  = next_l_range->begin()
                const uint32_t next_raw = range_npairs(next_l_range);
                const uint32_t next_slot_rounded = slot_rounded_npairs(next_raw, hot_npairs);
                // (non_left_effective は先頭 middle として slot_rounded(next_l_range) を含むはず)
                const uint32_t new_non_left_effective = non_left_effective - next_slot_rounded;
                const uint32_t new_window_effective = next_raw + new_non_left_effective;
                bool commit;
                if (new_window_effective >= required_npairs) {
                    commit = true;  // raw OK
                } else {
                    // exception: 新 l は next_l_range->begin() に位置するので発火条件を満たす。
                    // この 1 chunk の前進で止めないと次 step で旧 l_range が完全通過 middle に
                    // 格下げされ carve 数が nr_hots を超えてしまう。
                    //   slot_rounded_eff = slot_rounded(next_raw) + new_non_left_effective
                    //                    = next_slot_rounded + non_left_effective - next_slot_rounded
                    //                    = non_left_effective
                    // が丁度 required なら carve は nr_hots 個ぴったりで終わる。
                    commit = (non_left_effective == required_npairs);
                }
                if (!commit)
                    break;
                // commit: l_range のすべての残 chunk の load を剥がす
                for (DataChunkIterator c = l; c != l_range->end(); ++c)
                    nqrys_in_window -= c->load();
                l_range = next_l_range;
                l = l_range->begin();
                left_npairs_offcut = next_raw;
                non_left_effective = new_non_left_effective;
                // window_effective == left_npairs_offcut + non_left_effective を維持
                continue;
            }

            // ── Step C: argmax 更新 (Alg.3 L1395-L1397) ──
            if (nqrys_in_window > best_load) {
                best_load = nqrys_in_window;
                argmax_left_range = l_range;
                argmax_left = l;
                argmax_right_range = r_range;
                argmax_right = r_end;
            }
        }

        // r_range の走査が終わった時点で、(もし l_range != r_range なら) 現 r_range は
        // 次ループで middle range 扱いに遷移する。right_window_offcut 分の寄与は既に
        // non_left_effective に積まれているため、次ループ先頭で right 側をリセットすれば
        // 自動的に middle に繰り上がる (不変条件保持)。
    }

    return carve_out_hot_ranges();
}
