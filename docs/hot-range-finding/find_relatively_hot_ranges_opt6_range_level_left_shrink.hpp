/// @file find_relatively_hot_ranges_opt6_range_level_left_shrink.hpp
/// @brief 論文 Algorithm 3 第2スキャンの sliding window 最適化版 + opt1..opt6。
///        ベースは find_relatively_hot_ranges_opt5_right_margin_compression.hpp。
///        ビルド対象ではない (参照用)。
///
/// ── opt6: RangeLevelLeftShrink ──────────────────────
/// bpforest.ipp L1369-L1398 相当。
///
/// 目的:
///   opt5 までの Step B は左端縮小を chunk 単位で毎回判定しており、
///   ウィンドウが複数 cold range をまたいで走査される際、途中の range を
///   1 chunk ずつ舐めて捨てていた。bpforest.ipp はこの縮小を
///     (a) cold range 単位の bulk ループ (L1370-L1389)
///     (b) 最終 range 内の chunk 単位ループ (L1393-L1398)
///   に分解しており、range 全体を O(1) で飛ばせる。
///
///   range 単位処理には "left 側の raw 残量 (left_npairs_offcut) と
///   slot_rounded 下限 (left_window_offcut) を別管理する" 状態表現が
///   自然である。これは sliding_window.hpp 以来の
///   `non_left_effective = Σ middle slot_rounded + right_window_offcut`
///   という一括表現とは別の分解であり、opt6 ではこの state 表現自体を
///   bpforest.ipp 方式に切り替える (raw は left_npairs_offcut,
///   slot_rounded 下限は left_window_offcut で持つ)。
///
///   shrink_budget = Δright_window_offcut。この予算で左端を縮めていく:
///     - (a) `left_window_offcut <= shrink_budget` が成立する間、現 left_range
///       の全残 chunk を捨てて `++left_range`。合流ケース
///       (new left_range == right_range) では right 側状態を left に畳む。
///     - (b) 残った shrink_budget を `left_window_offcut` から差し引き、
///       `left_npairs_offcut - left->npairs() >= left_window_offcut` の間
///       chunk を 1 つずつ捨てる。
///
///   opt6 は bpforest.ipp L1205-L1402 と semantically equivalent になる
///   (最終段階)。
///
///   含意:
///     - 状態変数の並びが変わるため Phase 1 全体を bpforest.ipp 準拠に
///       揃え直す。Phase 2 (carve_out_hot_ranges) は全 opt 共通。
///     - opt1 (whole-range bootstrap), opt2 (all-hot early return),
///       opt3 (single-range fast path), opt4 (threshold gate),
///       opt5 (right margin compression) は bpforest.ipp 側で既に素直に
///       書かれており、opt6 への集約後もすべて継承される。
///
/// ── opt5: RightMarginCompression ─────────────────────
/// bpforest.ipp L1362-L1367 相当。opt4 の閾値ゲート成立直後、同 range 内で
/// right を先取りしてマージンを最小化する。
///
/// ── opt4: RightQuotaThresholdGate ───────────────────
/// bpforest.ipp L1355-L1361 相当。Step A の重い recompute と Step B を
/// 閾値 `right_npairs_offcut > right_window_offcut` の成立時だけに遅延。
///
/// ── opt3: SingleRangeSteadyStateFastPath ────────────
/// bpforest.ipp L1329-L1338 相当。left_range == right_range のとき right を
/// 1 chunk 足して left を tight while で押し戻す専用分岐。
///
/// ── opt2: BootstrapAllHotEarlyExit ─────────────────
/// bpforest.ipp L1271-L1276 相当。bootstrap が全 range を吸収したら argmax
/// 確定で carve へ直行。
///
/// ── opt1: BootstrapWholeRangeAdvance ─────────────────
/// bpforest.ipp L1254-L1268 相当。メインループ前に right を cold range 単位で
/// 進める bootstrap。
///
/// 含まない最適化:
///   なし (opt6 が最終段階)。
///
/// ── naive 意味論との関係 ───────────────────────────
/// bpforest.ipp の状態表現では left 側寄与も `left_window_offcut`
/// (slot_rounded 下限) で測る。一方 naive.hpp の effective_window_pair_count
/// は left partial を raw で使う。sliding_window.hpp / opt1..opt5 は naive
/// 準拠の raw-left 表現を採っていたが、bpforest.ipp は別系統の invariant
/// で同じ argmax 位置を導出している。両表現は 29 ケースの回帰で同じ
/// 出力を返すことが確認済み。

#pragma once

#include "pairs_range.hpp"

#include <array>
#include <cassert>
#include <iterator>

template <typename Func /* bool(ChunkedPairsRange&, PairsRange, load) */>
inline std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>
find_relatively_hot_ranges(const LinkedList<ChunkedPairsRange>::iterator begin_range, const LinkedList<ChunkedPairsRange>::iterator end_range,
    const uint32_t hot_npairs, const dpu_id_t nr_hots,
    Func&& hot_hook)
{
    assert(begin_range != end_range);
    assert(nr_hots != 0);

    LinkedList<ChunkedPairsRange>::iterator argmax_left_range = begin_range, argmax_right_range = argmax_left_range;
    DataChunkIterator argmax_left = argmax_left_range->begin(), argmax_right = argmax_left;

    // ── Phase 2: carve-out (全 opt 共通) ──
    const auto carve_out_hot_ranges = [&] {
        LinkedList<ChunkedPairsRange>::iterator range = argmax_right_range;
        DataChunkIterator chunk = argmax_right;

        for (;;) {
            const DataChunkIterator limit = (range == argmax_left_range ? argmax_left : range->begin());
            while (limit < chunk) {
                uint32_t load = 0;
                PairsRange hot_range{chunk->begin(), chunk->begin()};
                while (limit < chunk && hot_range.npairs() < hot_npairs) {
                    --chunk;
                    hot_range = PairsRange{chunk->begin(), hot_range.end()};
                    load += chunk->load();
                }
                if (!hot_hook(*range, hot_range, load)) {
                    return std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>{
                        {{range, chunk}, {argmax_right_range, argmax_right}}};
                }
            }

            if (range == argmax_left_range) {
                break;
            }

            --range;
            chunk = range->end();
        }

        return std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>{
            {{argmax_left_range, argmax_left}, {argmax_right_range, argmax_right}}};
    };

    uint32_t nqrys_in_window = 0;

    LinkedList<ChunkedPairsRange>::iterator left_range = begin_range;
    DataChunkIterator left = left_range->begin();

    //----- 1. keeping the left end as is, extend the right end -----//
    // opt1 (BootstrapWholeRangeAdvance) + opt2 (BootstrapAllHotEarlyExit)

    LinkedList<ChunkedPairsRange>::iterator right_range = left_range;
    uint32_t right_window_offcut = hot_npairs * nr_hots;  // length of the window on right_range

    // push the right end outward per cold range
    while (right_range != end_range && right_range->npairs() <= right_window_offcut) {
        const dpu_id_t nr_hots_in_this_range = (static_cast<dpu_id_t>(right_range->npairs()) + hot_npairs - 1) / hot_npairs;
        right_window_offcut -= nr_hots_in_this_range * hot_npairs;

        for (DataChunkIterator chunk = right_range->begin(); chunk != right_range->end(); chunk++) {
            nqrys_in_window += chunk->load();
        }

        right_range++;
    }

    // all chunks are hot (opt2)
    if (right_range == end_range) {
        argmax_right_range = std::prev(right_range);
        argmax_right = argmax_right_range->end();

        return carve_out_hot_ranges();
    }

    // push the right end outward per data chunk
    DataChunkIterator right;
    uint32_t right_npairs_offcut;
    if (right_window_offcut == 0) {
        --right_range;
        right = right_range->end();
        right_npairs_offcut = static_cast<uint32_t>(right_range->npairs());
        right_window_offcut = (right_npairs_offcut + hot_npairs - 1) / hot_npairs * hot_npairs;

    } else {
        right = right_range->begin();
        right_npairs_offcut = 0;

        while (right_npairs_offcut + right->npairs() < right_window_offcut) {
            right_npairs_offcut += right->npairs();
            nqrys_in_window += right->load();
            ++right;
        }
    }

    uint32_t left_window_offcut, left_npairs_offcut;
    if (left_range != right_range) {
        left_npairs_offcut = static_cast<uint32_t>(left_range->npairs());
        left_window_offcut = (left_npairs_offcut + hot_npairs - 1) / hot_npairs * hot_npairs;

    } else {  // when both ends are in the same cold range, use the variable for the left end
        left_window_offcut = right_window_offcut;
        left_npairs_offcut = right_npairs_offcut;
        right_window_offcut = right_npairs_offcut = 0;
    }

    // finish keeping the left end

    // record the status
    uint32_t max_nqrys_in_window = nqrys_in_window;
    argmax_right_range = right_range;
    argmax_right = right;
    const auto compare_nqrys_in_window = [&] {
        if (nqrys_in_window > max_nqrys_in_window) {
            max_nqrys_in_window = nqrys_in_window;
            argmax_left_range = left_range;
            argmax_right_range = right_range;
            argmax_left = left;
            argmax_right = right;
        }
    };

    //----- 2. a cycle of extending the right end and then adjusting the left end -----//

    for (;; compare_nqrys_in_window()) {
        if (right != right_range->end() && left_range == right_range) {
            // opt3: single-range steady state fast path
            left_npairs_offcut += right->npairs();
            nqrys_in_window += right->load();
            ++right;

            while (left_npairs_offcut - left->npairs() >= left_window_offcut) {
                left_npairs_offcut -= left->npairs();
                nqrys_in_window -= left->load();
                ++left;
            }

        } else {
            if (right == right_range->end()) {  // the right edge is extending to the next cold range
                if (++right_range == end_range) {
                    return carve_out_hot_ranges();
                }

                assert(right_range->npairs() > 0);
                right_npairs_offcut = right_window_offcut = 0;
                right = right_range->begin();
            }

            right_npairs_offcut += static_cast<uint32_t>(right->npairs());
            nqrys_in_window += right->load();
            ++right;

            // opt4: threshold gate
            if (right_npairs_offcut > right_window_offcut) {
                const uint32_t new_right_window_offcut = (right_npairs_offcut + hot_npairs - 1) / hot_npairs * hot_npairs;
                uint32_t left_window_offcut_to_shrink = new_right_window_offcut - right_window_offcut;
                right_window_offcut = new_right_window_offcut;

                // opt5: RightMarginCompression
                while (right != right_range->end() && right_npairs_offcut + right->npairs() < right_window_offcut) {
                    right_npairs_offcut += right->npairs();
                    nqrys_in_window += right->load();
                    ++right;
                }

                // opt6: RangeLevelLeftShrink — (a) range-level bulk
                while (left_window_offcut <= left_window_offcut_to_shrink) {
                    while (left != left_range->end()) {
                        nqrys_in_window -= left->load();
                        ++left;
                    }
                    left_window_offcut_to_shrink -= left_window_offcut;

                    ++left_range;
                    left = left_range->begin();

                    if (left_range == right_range) {
                        left_window_offcut = right_window_offcut;
                        left_npairs_offcut = right_npairs_offcut;
                        right_window_offcut = right_npairs_offcut = 0;
                        break;
                    } else {
                        left_npairs_offcut = static_cast<uint32_t>(left_range->npairs());
                        left_window_offcut = (left_npairs_offcut + hot_npairs - 1) / hot_npairs * hot_npairs;
                    }
                }
                assert(left_window_offcut > left_window_offcut_to_shrink);
                left_window_offcut -= left_window_offcut_to_shrink;

                // opt6: RangeLevelLeftShrink — (b) final chunk-level
                while (left_npairs_offcut - left->npairs() >= left_window_offcut) {
                    left_npairs_offcut -= left->npairs();
                    nqrys_in_window -= left->load();
                    ++left;
                }
            }
        }
    }
}
