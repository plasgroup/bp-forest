/// @file find_relatively_hot_ranges_naive.hpp
/// @brief 論文 Algorithm 3 第2スキャン (argmax + carve-out) の愚直版参照実装。
///        bpforest.ipp::find_relatively_hot_ranges と同一シグネチャ・同一 callback 規約。
///        sliding window 最適化を使わず、各 r につき l' 候補を右→左に全走査する。
///        ビルド対象ではない (参照用)。
///
/// ── 論文との意味論の差分 ──────────────────────────────
/// 論文は base partition 上の連続 chunk 列をウィンドウとするが、本関数の入力は
/// greedy pass 後の cold range list であり、hot range の位置は走査空間から消えている。
/// したがってウィンドウは cold range を不連続に跨ぐ半開区間 [l, r+1) として扱う。
///
/// ── argmax loop の目的と effective pair count ────────
/// 本関数のゴールは「carve_out_hot_ranges が丁度 nr_hots 個の hot range を切り出せる
/// 最大荷重ウィンドウ」を見つけること。argmax loop は r を 1 chunk ずつ伸ばし、
/// 各 r について effective pair count が required_npairs を初めて満たす最右の l' を選ぶ:
///   effective_window_pair_count(l, r+1)
///     = raw_npairs(left_partial)
///     + Σ slot_rounded_npairs(middle_range)
///     + slot_rounded_npairs(right_partial)
///   where slot_rounded_npairs(n) = ceil(n / hot_npairs) * hot_npairs
/// この式で left partial だけ raw を使うのは、carve が完全通過する range
/// (middle / right partial の場合は range の先頭から包み始める都合で右 partial も
/// 同様に丸める) を hot_npairs 単位で消費するのに対し、左端だけは「最後に追加した
/// chunk が hot 1 個分の余剰をどれだけ運ぶか」を chunk 粒度で見たいから。raw 比較が
/// 丁度 required を満たす最右の cand に止まれば、carve はその窓を丁度 nr_hots 個に
/// 切り分けられる。
///
/// l_range == r_range のとき式は raw(left, right) に退化し、
/// docs/hot-range-finding/find_relatively_hot_ranges_single_range_naive.hpp の pair 数条件と一致する。
///
/// ── 境界例外: なぜ必要か ─────────────────────────
/// 上記 raw 比較には盲点がある: cand が cand_range->begin() に達した瞬間、left partial
/// は cand_range 全体になる。cand_range が undersize (raw_npairs < slot_rounded_npairs)
/// のとき、raw 比較は cand_range を raw のまま数えるためまだ required に届かないが、
/// carve はその時点で cand_range をひとまとめの fragment として hot_npairs 単位 = 全 range で
/// slot_rounded(cand_range) / hot_npairs 個分消費する状態にある。
///
/// この瞬間に救済しないと、loop は cand を更に左 (前 range の最終 chunk) へ進める。
/// 新しいウィンドウは:
///   - 前 range の最後 1 chunk が左 partial
///   - cand_range が「完全通過 middle」に格下げされ slot_rounded 換算で参入
///   - その他の middle/right はそのまま
/// になり、carve に消費される hot range 数は
///   ceil(raw(prev の 1 chunk) / hot_npairs) + slot_rounded(cand_range) / hot_npairs + rest/hot
/// = nr_hots + α   (α ≥ 1)
/// と nr_hots を超過する。これは carve_out_hot_ranges の契約 (nr_hots 個ぴったり) を破る。
///
/// これを防ぐため cand == cand_range->begin() で `effective_npairs == required_npairs`
/// (left partial も slot_rounded した値) が成立すれば cand をその場で l' に採用して loop
/// を止める。これにより carve は丁度 nr_hots 個で終わることが保証される。
///
/// なぜ `==` だけで十分か (`>` を救う必要はない):
///   required, rest (= middle slot 和 + right_partial の slot), slot_rounded(cand_range)
///   は全て hot_npairs の倍数。raw 版が落ちた (raw(cand_range) + rest < required) という
///   前提で delta := slot_rounded(cand_range) - raw(cand_range) ∈ [0, hot_npairs) を使うと
///       effective_npairs = slot_rounded(cand_range) + rest
///                        = (raw(cand_range) + rest) + delta
///                        < required + hot_npairs
///   effective_npairs 自体も hot_npairs の倍数なので effective_npairs ≤ required。
///   よって `>` のケースは構造的に存在せず、追加で救うのは丁度 `==` だけ。
///   `<` の場合は窓の容量不足 (carve しても nr_hots に届かない) なので採用しない。
///
/// なお bpforest.ipp::find_relatively_hot_ranges も同一の規則を実装している
/// (bootstrap 直後の左端初期化と bulk left shrink の range 前進時に
/// left_window_offcut を slot_rounded(left_range->npairs()) に揃える)。

#pragma once

#include "pairs_range.hpp"

#include <array>
#include <cassert>

namespace relatively_hot_ranges_naive_detail {

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

inline uint32_t range_npairs(const DataChunkIterator begin, const DataChunkIterator end)
{
    return static_cast<uint32_t>(ChunkedPairsRange{begin, end}.npairs());
}

inline uint32_t range_load(const DataChunkIterator begin, const DataChunkIterator end)
{
    uint32_t load = 0;
    for (DataChunkIterator chunk = begin; chunk != end; ++chunk)
        load += chunk->load();
    return load;
}

// ウィンドウ [left, right) の effective pair count。
// left partial は raw、middle range と right partial は slot_rounded。
inline uint32_t effective_window_pair_count(
    LinkedList<ChunkedPairsRange>::iterator left_range,
    const DataChunkIterator left,
    const LinkedList<ChunkedPairsRange>::iterator right_range,
    const DataChunkIterator right,
    const uint32_t hot_npairs)
{
    if (left_range == right_range)
        return range_npairs(left, right);

    uint32_t npairs = range_npairs(left, left_range->end());
    for (LinkedList<ChunkedPairsRange>::iterator range = std::next(left_range);
         range != right_range; ++range) {
        npairs += slot_rounded_npairs(static_cast<uint32_t>(range->npairs()), hot_npairs);
    }
    npairs += slot_rounded_npairs(range_npairs(right_range->begin(), right), hot_npairs);
    return npairs;
}

// ウィンドウ [left, right) の effective pair count、ただし left partial も slot_rounded。
// 境界例外 (left が left_range->begin() に等しい時) でのみ使う。
inline uint32_t effective_npairs(
    LinkedList<ChunkedPairsRange>::iterator left_range,
    const DataChunkIterator left,
    const LinkedList<ChunkedPairsRange>::iterator right_range,
    const DataChunkIterator right,
    const uint32_t hot_npairs)
{
    if (left_range == right_range)
        return slot_rounded_npairs(range_npairs(left, right), hot_npairs);

    uint32_t count = slot_rounded_npairs(range_npairs(left, left_range->end()), hot_npairs);
    for (LinkedList<ChunkedPairsRange>::iterator range = std::next(left_range);
         range != right_range; ++range) {
        count += slot_rounded_npairs(static_cast<uint32_t>(range->npairs()), hot_npairs);
    }
    count += slot_rounded_npairs(range_npairs(right_range->begin(), right), hot_npairs);
    return count;
}

// NQrys (cross-range): ウィンドウ [left, right) の load 合計。
inline uint32_t window_load(
    LinkedList<ChunkedPairsRange>::iterator left_range,
    const DataChunkIterator left,
    const LinkedList<ChunkedPairsRange>::iterator right_range,
    const DataChunkIterator right)
{
    if (left_range == right_range)
        return range_load(left, right);

    uint32_t load = range_load(left, left_range->end());
    for (LinkedList<ChunkedPairsRange>::iterator range = std::next(left_range);
         range != right_range; ++range) {
        load += range_load(range->begin(), range->end());
    }
    load += range_load(right_range->begin(), right);
    return load;
}

}  // namespace relatively_hot_ranges_naive_detail

// ── Algorithm 3 第2スキャン: argmax + carve-out (愚直版) ──
//
// 引数と論文記号の対応:
//   hot_npairs  ≡ ceil((1/α)(D/P))     ── 1 hot range のペア数幅
//   nr_hots     ≡ β                     ── 選出する relative hot range 数
//
// 論文の閉区間 {c_i}_{i=l}^{r} を半開区間 [l, r+1) として扱う。
// 走査窓は cold range list 上の不連続区間であり、l' の判定は
// effective_window_pair_count >= hot_npairs * nr_hots で行う (上部コメント参照)。
template <typename Func /* bool(ChunkedPairsRange&, PairsRange, load) */>
inline std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>
find_relatively_hot_ranges(const LinkedList<ChunkedPairsRange>::iterator begin_range, const LinkedList<ChunkedPairsRange>::iterator end_range,
    const uint32_t hot_npairs, const dpu_id_t nr_hots,
    Func&& hot_hook)
{
    using namespace relatively_hot_ranges_naive_detail;
    using RangeIter = LinkedList<ChunkedPairsRange>::iterator;

    assert(begin_range != end_range);
    assert(nr_hots != 0);

    RangeIter argmax_left_range = begin_range, argmax_right_range = begin_range;
    DataChunkIterator argmax_left = begin_range->begin(), argmax_right = argmax_left;

    // ── Phase 2: Carve-out (Alg.3 L1399-L1404) ─────────────
    // argmax 位置の区間 [argmax_left, argmax_right) を右→左に hot_npairs ペアずつ
    // 束ねて hot_hook に渡す。cold range 境界を跨ぐときは 1 range ごとに callback を
    // 呼び直す (callback 規約 bool(ChunkedPairsRange&, PairsRange, load) のため)。
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
    // 各 r について l' = max{ l' ∈ (l, r+1] | effective_window_pair_count(l', r+1) >= required_npairs }
    // を右→左の全走査で求め、l ← max(l, l')、続いて NQrys を argmax と比較する。

    RangeIter l_range = begin_range;                          // Alg.3 L1391: l ← s
    DataChunkIterator l = l_range->begin();
    uint32_t best_load = 0;
    const uint32_t required_npairs = static_cast<uint32_t>(nr_hots) * hot_npairs;

    for (RangeIter r_range = begin_range;                     // Alg.3 L1392: for r ← s to e
         r_range != end_range; ++r_range)
    {
        for (DataChunkIterator r = r_range->begin();
             r != r_range->end(); ++r)
        {
            const DataChunkIterator r_end = r + 1;            // 半開端

            // Alg.3 L1393: 右→左に cand を走査し、最初に条件を満たす cand を l' に採用。
            // 無候補なら l_prime は l のまま (l は前進しない)。
            RangeIter l_prime_range = l_range;
            DataChunkIterator l_prime = l;
            {
                RangeIter cand_range = r_range;
                DataChunkIterator cand = r;

                while (cand_range != l_range || cand != l) {
                    if (effective_window_pair_count(cand_range, cand, r_range, r_end, hot_npairs)
                        >= required_npairs) {
                        l_prime_range = cand_range;
                        l_prime = cand;
                        break;
                    }

                    if (cand == cand_range->begin()) {
                        // 境界例外: cand_range が undersize でここを素通りすると、loop は
                        // 前 range の chunk へ進み cand_range が "完全通過 middle" に格下げ
                        // される結果、carve 数が nr_hots を超える。effective_npairs (left も
                        // slot_rounded) が丁度 required に等しい瞬間だけここで止めれば carve
                        // は丁度 nr_hots 個になる。`>` は構造的に起こらず (ヘッダ参照)、`<`
                        // は容量不足なので採用しない。
                        if (effective_npairs(cand_range, cand, r_range, r_end, hot_npairs)
                            == required_npairs) {
                            l_prime_range = cand_range;
                            l_prime = cand;
                            break;
                        }
                        --cand_range;
                        cand = cand_range->end();
                    }
                    --cand;
                }
            }

            // Alg.3 L1394: l ← max{l, l'}
            if (l_prime_range != l_range || l_prime != l) {
                l_range = l_prime_range;
                l = l_prime;
            }

            // Alg.3 L1395-L1397: argmax 更新
            const uint32_t load = window_load(l_range, l, r_range, r_end);
            if (load > best_load) {
                best_load = load;
                argmax_left_range = l_range;
                argmax_left = l;
                argmax_right_range = r_range;
                argmax_right = r_end;
            }
        }
    }

    return carve_out_hot_ranges();
}
