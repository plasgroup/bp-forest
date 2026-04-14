/// @file find_relatively_hot_ranges_opt2_bootstrap_all_hot_early_exit.hpp
/// @brief 論文 Algorithm 3 第2スキャンの sliding window 最適化版 + opt1 + opt2。
///        ベースは find_relatively_hot_ranges_opt1_bootstrap_whole_range_advance.hpp。
///        ビルド対象ではない (参照用)。
///
/// ── opt2: BootstrapAllHotEarlyExit ─────────────────
/// bpforest.ipp L1271-L1276 相当。
///
/// 目的:
///   opt1 の whole-range bootstrap がすべての range を吸収した場合、argmax は既に
///   「全 chunk が window に入っている唯一の状態」に一意に定まる (load は既に最大、
///   l を動かせば厳密に減少する)。定常 loop を回す意味がないため、bootstrap 直後に
///   carve_out_hot_ranges へ直行する。
///
///   なお bpforest.ipp 自身はこの early return を行うが、opt1 版は代わりに
///   prev(end_range)->end() を sentinel にして outer loop を通過させていた。opt2 で
///   その sentinel 経路を削除して本来の早期 return に置き換える。
///
/// ── opt1: BootstrapWholeRangeAdvance ─────────────────
/// bpforest.ipp L1254-L1268 相当 (whole-range 段のみ)。
///
/// 目的:
///   ベースの sliding 版は Phase 1 を
///     for r_range { for r { Step A, Step B, Step C } }
///   の一重走査から開始していたため、最初の「ウィンドウが容量下限に届くまで」の
///   区間も 1 chunk 単位で Step A/B/C を舐めていた。l はこの区間では一切動かない
///   (raw(window) が required_npairs 未満では Step B の pop 条件が成立しない) し、
///   argmax も単調非減少なので、この区間はまとめて飛ばして良い。
///
///   opt1 ではメインループ前に「l を begin_range のまま固定し、r を cold range
///   単位で前進させる」bootstrap を 1 回だけ行う。残予算 bootstrap_remaining_budget
///   (初期値 = required_npairs) を持ち、range_npairs(bootstrap_r_range) が budget
///   以下である間は range 丸ごとをウィンドウに取り込み、load と effective state を
///   バルクで更新する。その後は既存の Step A/B/C を bootstrap 後の (r_range, r) から
///   再開する。
///
/// 含まない最適化 (後続 opt で追加):
///   - opt3: SingleRangeSteadyStateFastPath
///   - opt4: RightQuotaThresholdGate (定常 loop の rounded 幅再計算の閾値化)
///   - opt5: RightMarginCompression
///   - opt6: RangeLevelLeftShrink
///
/// 含まない最適化のうち、bootstrap に関するもの:
///   - chunk 単位の bootstrap (L1279-L1297) は opt1 では行わない。
///     whole-range bootstrap が終わったら、残った端数は既存 Step A にそのまま引き継ぐ。
///
/// 不変条件:
///   bootstrap 直後も sliding_window.hpp 148-177 の状態変数不変条件 (single/multi
///   range 両ケース) が保たれる (詳細は bootstrap コメント参照)。
///
/// ── naive / sliding 版の意味論 ─────────────────────────
/// naive.hpp / sliding_window.hpp のヘッダコメントを参照。要点:
///   - 入力は greedy pass 後の cold range list。ウィンドウは cold range を不連続に
///     跨ぐ半開区間 [l, r+1)。
///   - effective_window_pair_count(l, r+1) = raw(left_partial) + Σ slot_rounded(middle)
///                                          + slot_rounded(right_partial)
///     と hot_npairs * nr_hots を比較する。
///   - 境界例外: 新 l が new_l_range->begin() に位置し、slot_rounded_eff ==
///     required_npairs が成立する瞬間だけ追加採用。carve 数を丁度 nr_hots に抑える
///     ための救済 (naive.hpp 参照)。

#pragma once

#include "pairs_range.hpp"

#include <array>
#include <cassert>
#include <iterator>

namespace relatively_hot_ranges_opt2_bootstrap_all_hot_early_exit_detail {

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

}  // namespace relatively_hot_ranges_opt2_bootstrap_all_hot_early_exit_detail

// ── Algorithm 3 第2スキャン: argmax + carve-out (opt2 版) ──
template <typename Func /* bool(ChunkedPairsRange&, PairsRange, load) */>
inline std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>
find_relatively_hot_ranges(const LinkedList<ChunkedPairsRange>::iterator begin_range, const LinkedList<ChunkedPairsRange>::iterator end_range,
    const uint32_t hot_npairs, const dpu_id_t nr_hots,
    Func&& hot_hook)
{
    using namespace relatively_hot_ranges_opt2_bootstrap_all_hot_early_exit_detail;
    using RangeIter = LinkedList<ChunkedPairsRange>::iterator;

    assert(begin_range != end_range);
    assert(nr_hots != 0);

    RangeIter argmax_left_range = begin_range, argmax_right_range = begin_range;
    DataChunkIterator argmax_left = begin_range->begin(), argmax_right = argmax_left;

    // ── Phase 2: Carve-out (sliding_window.hpp から逐語流用) ──
    const auto carve_out_hot_ranges = [&] {
        RangeIter range = argmax_right_range;
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
                    return std::array<std::pair<RangeIter, DataChunkIterator>, 2>{
                        {{range, chunk}, {argmax_right_range, argmax_right}}};
                }
            }

            if (range == argmax_left_range)
                break;

            --range;
            chunk = range->end();
        }

        return std::array<std::pair<RangeIter, DataChunkIterator>, 2>{
            {{argmax_left_range, argmax_left}, {argmax_right_range, argmax_right}}};
    };

    // ── Phase 1: Argmax スキャン ──

    const uint32_t required_npairs = static_cast<uint32_t>(nr_hots) * hot_npairs;

    RangeIter l_range = begin_range;
    DataChunkIterator l = l_range->begin();

    uint32_t nqrys_in_window = 0;
    uint32_t best_load = 0;

    uint32_t left_npairs_offcut = 0;
    uint32_t right_npairs_offcut = 0;
    uint32_t right_window_offcut = 0;
    uint32_t non_left_effective = 0;

    // ── BootstrapWholeRangeAdvance (bpforest.ipp L1254-L1268 相当) ──
    //
    // 意味:
    //   left は begin_range のまま固定し、right だけを cold range 単位で前進させる。
    //   bootstrap 後のウィンドウは [l, bootstrap_r) であり、以降の Step A はまだ
    //   数えていない chunk から再開する。
    //
    // 残予算:
    //   bootstrap_remaining_budget は「right 側にまだ足せる slot-rounded pair 数」。
    //   range を 1 つ吸収するたびに slot_rounded(raw) を引く。bpforest.ipp の
    //   right_window_offcut と同じ役割だが、本ファイルでは定常 loop 用の
    //   right_window_offcut とは分離する (opt1 の責務を bootstrap に閉じ込めるため)。
    const auto accumulate_whole_range_load = [&](const RangeIter range) {
        for (DataChunkIterator chunk = range->begin(); chunk != range->end(); ++chunk)
            nqrys_in_window += chunk->load();
    };

    RangeIter bootstrap_r_range = begin_range;
    DataChunkIterator bootstrap_r = begin_range->begin();
    bool bootstrapped_any_range = false;
    uint32_t bootstrap_remaining_budget = required_npairs;

    while (bootstrap_r_range != end_range
        && range_npairs(bootstrap_r_range) <= bootstrap_remaining_budget)
    {
        const uint32_t raw = range_npairs(bootstrap_r_range);
        const uint32_t rounded = slot_rounded_npairs(raw, hot_npairs);

        accumulate_whole_range_load(bootstrap_r_range);
        bootstrap_remaining_budget -= rounded;
        bootstrapped_any_range = true;

        if (bootstrap_r_range == l_range) {
            // 吸収した range は left partial 全体 (single-range ケース)。
            left_npairs_offcut = raw;
        } else {
            // begin_range の右にある whole range は middle 寄与として積む。
            // sentinel 正規化の段で、最後の吸収 range を right partial へ降格する
            // 可能性があるが、non_left_effective への寄与は同じ rounded なので
            // 合計は変わらない (right_window_offcut として再ラベルするだけ)。
            non_left_effective += rounded;
        }

        ++bootstrap_r_range;
    }

    if (bootstrapped_any_range) {
        // bootstrap が吸収した prefix 群は l 不変・load 非減少なので、bootstrap 後の
        // ウィンドウを初期ベストとして記録して良い。
        best_load = nqrys_in_window;
        argmax_left_range = l_range;
        argmax_left = l;

        if (bootstrap_r_range == end_range) {
            // opt2: bootstrap で全 range を吸収したので argmax は確定。
            // 現 window [l, prev(end_range)->end()) を確定し carve_out へ直行する。
            argmax_right_range = std::prev(end_range);
            argmax_right = argmax_right_range->end();
            return carve_out_hot_ranges();
        } else if (bootstrap_remaining_budget == 0) {
            // slot-rounded quota を range 境界で使い切った。
            // 直前に吸収した range を sentinel として保持し、次の range 以降は既存 loop に任せる。
            --bootstrap_r_range;
            bootstrap_r = bootstrap_r_range->end();
            argmax_right_range = bootstrap_r_range;
            argmax_right = bootstrap_r;

            if (bootstrap_r_range != l_range) {
                right_npairs_offcut = range_npairs(bootstrap_r_range);
                right_window_offcut = slot_rounded_npairs(right_npairs_offcut, hot_npairs);
            }
        } else {
            // 次 range の raw > 残予算 で bootstrap が打ち切られた。
            // bootstrap_r_range は未吸収 range を指しており、bootstrap_r はその先頭 chunk。
            // 既存 Step A はこの chunk から続行する。右側状態は 0 (= 新 r_range の初期値)。
            bootstrap_r = bootstrap_r_range->begin();
            argmax_right_range = bootstrap_r_range;
            argmax_right = bootstrap_r;
        }
    }

    for (RangeIter r_range = bootstrap_r_range;
         r_range != end_range; ++r_range)
    {
        // bootstrap 開始 range では既に state が立っている可能性があるので reset しない。
        // それ以降の新 r_range に入った瞬間だけ右側状態を空に戻す (middle 繰り上げ)。
        if (r_range != bootstrap_r_range) {
            right_npairs_offcut = 0;
            right_window_offcut = 0;
        }

        for (DataChunkIterator r = (r_range == bootstrap_r_range ? bootstrap_r : r_range->begin());
             r != r_range->end(); ++r)
        {
            const DataChunkIterator r_end = r + 1;

            // ── Step A: r を 1 chunk 伸ばす (状態の差分更新) ──
            nqrys_in_window += r->load();

            if (l_range == r_range) {
                left_npairs_offcut += static_cast<uint32_t>(r->npairs());
            } else {
                right_npairs_offcut += static_cast<uint32_t>(r->npairs());
                const uint32_t new_right_window_offcut =
                    slot_rounded_npairs(right_npairs_offcut, hot_npairs);
                non_left_effective += new_right_window_offcut - right_window_offcut;
                right_window_offcut = new_right_window_offcut;
            }

            // ── Step B: l を単調前進させる (sliding_window.hpp から逐語流用) ──
            for (;;) {
                if (l_range == r_range) {
                    const uint32_t popped = static_cast<uint32_t>(l->npairs());
                    if (left_npairs_offcut - popped < required_npairs)
                        break;
                    nqrys_in_window -= l->load();
                    left_npairs_offcut -= popped;
                    ++l;
                    continue;
                }

                if (left_npairs_offcut > static_cast<uint32_t>(l->npairs())) {
                    const uint32_t popped = static_cast<uint32_t>(l->npairs());
                    if (left_npairs_offcut - popped + non_left_effective < required_npairs)
                        break;
                    nqrys_in_window -= l->load();
                    left_npairs_offcut -= popped;
                    ++l;
                    continue;
                }

                const RangeIter next_l_range = std::next(l_range);

                if (next_l_range == r_range) {
                    // (a) 合流ケース
                    const uint32_t new_window_effective = right_npairs_offcut;
                    bool commit;
                    if (new_window_effective >= required_npairs) {
                        commit = true;
                    } else {
                        commit = (right_window_offcut == required_npairs);
                    }
                    if (!commit)
                        break;
                    for (DataChunkIterator c = l; c != l_range->end(); ++c)
                        nqrys_in_window -= c->load();
                    l_range = next_l_range;
                    l = l_range->begin();
                    left_npairs_offcut = right_npairs_offcut;
                    right_npairs_offcut = 0;
                    right_window_offcut = 0;
                    non_left_effective = 0;
                    continue;
                }

                // (b) 依然複数 range
                const uint32_t next_raw = range_npairs(next_l_range);
                const uint32_t next_slot_rounded = slot_rounded_npairs(next_raw, hot_npairs);
                const uint32_t new_non_left_effective = non_left_effective - next_slot_rounded;
                const uint32_t new_window_effective = next_raw + new_non_left_effective;
                bool commit;
                if (new_window_effective >= required_npairs) {
                    commit = true;
                } else {
                    commit = (non_left_effective == required_npairs);
                }
                if (!commit)
                    break;
                for (DataChunkIterator c = l; c != l_range->end(); ++c)
                    nqrys_in_window -= c->load();
                l_range = next_l_range;
                l = l_range->begin();
                left_npairs_offcut = next_raw;
                non_left_effective = new_non_left_effective;
                continue;
            }

            // ── Step C: argmax 更新 ──
            if (nqrys_in_window > best_load) {
                best_load = nqrys_in_window;
                argmax_left_range = l_range;
                argmax_left = l;
                argmax_right_range = r_range;
                argmax_right = r_end;
            }
        }
    }

    return carve_out_hot_ranges();
}
