/// @file incremental_repartition_serialize_naive.hpp
/// @brief bpforest.ipp:1664-2138 の `BPForest::incremental_repartition` を、
///        同一シグネチャ・同一意味論のまま最も愚直に書き直した参照実装 (v2)。
///        参照用であり、ビルド対象ではない。
///
/// v2 の方針: **DPU 側 partial serialize プロトコル (TASK_SERIALIZE +
///            incision_indices) は最適化版のまま使い、host 側ロジック
///            だけを愚直化する。**
///            v1 (`incremental_repartition_naive.hpp`) は DPU 側プロトコル
///            も丸ごとリセット方式に単純化していた。
///
/// v1 との差分:
///   - Phase 2 は最適化版と同じく TASK_SERIALIZE (`do_cold=true`,
///     `do_hot=false`) で cold 片だけを回収する。
///   - incision_indices / hot_delim_keys / base_to_nr_hot_psum の
///     「流れ」はそのまま残すが、メンバ buffer ではなくローカル配列で
///     受ける (愚直化 (e))。
///   - Phase 3a の cold 片復元は、DPU から送られてくる incision_indices を
///     使うので host 側 key 比較は不要; 配列スライスでそのまま切り出す。
///     per-piece の KeyRange は Phase 1 で組み立てた
///     `per_dpu_cold_key_ranges` を index で引く。
///   - Phase 5 は TASK_MOVE_HOT (cold / hot の部分更新) を使う。v1 は
///     丸ごとリセットに単純化するが、v2 は Phase 2 の partial serialize と
///     対称に partial update を維持する。
///
/// 剥がした最適化 (= 愚直化の内容、v1 の (a)(b)(c)(d)(e) と共通):
///   (a) クエリの chunk 位置特定を線形走査にする (full と同じ)。
///   (b) 欠番。以前は「hot_hook 内の `cold_endpoint_cnt > goal` 判定を
///       廃止する」と書いていたが、これは誤り — この判定は Algorithm
///       2/3 のロジックそのものであり、`return true` を返し続けると
///       carve される hot 数・位置・最終パーティションが変わってしまう。
///       愚直版でも段間 (bpforest.ipp:1948, 1999) と hot_hook 内
///       (bpforest.ipp:1989, 2019) の両方の `cold_endpoint_cnt > goal`
///       判定を維持する。
///   (c) cold 範囲列を `LinkedList<ChunkedPairsRange>` ではなく
///       `std::list<ChunkedPairsRange>` で持つ。find_* の呼び出し時
///       だけ full_repartition_naive.hpp の bridge 関数経由で一時
///       LinkedList を組み立てる。
///   (d) carve のたびに cold list を in-place splice で更新するのではなく、
///       `rebuild_cold_list_with_origins_naive` (v1 の helper) で毎回
///       build-from-scratch する。
///   (e) BPForest 側の「一時データ用」メンバ (`chunked_cold_ranges
///        [,_lists]`, `chunk2load`, `new_hots`, `cold_loads`,
///        `cold_npairs_list`, `input_headers`, `hot_delim_keys`,
///        `base_to_nr_hot_psum`, `incision_indices`, `cold_key_ranges` 等)
///       を全てローカル変数に置換。
///
/// 保っている意味論 (= 最適化版と同じ):
///   - Phase 1 で対象 DPU を限定 (incremental のアイデンティティ)
///   - `cold_partial_cnt_goal` / `cold_partial_cnt_threshold` の計算式
///   - `param.enable_incremental == false` で full に委譲
///   - 既存 hot delim の key 境界で cold を incision して partial に
///     受け取る DPU 側プロトコル (v1 は廃止、v2 は維持)
///   - `hot_load` / `cold_endpoint_cnt_goal` / `hot_npairs` の計算式
///   - find_absolutely → find_relatively の 2 段構え
///   - 段間と hot_hook 内の `cold_endpoint_cnt > goal` 判定
///   - `nr_existing_hots + new_hots > nr_base_parts` での full 委譲
///   - 既存 hot を持つ DPU は新規 hot の配置先候補から除外
///   - Phase 5 の TASK_MOVE_HOT 経由 partial update (v2 独自)
///   - 末尾の `route_queries` 再実行

#pragma once

#include "bpforest.hpp"
#include "full_repartition_naive.hpp"         // bridge helpers
#include "incremental_repartition_naive.hpp"  // IncrementalOriginPiece, rebuild helper

#include <algorithm>
#include <array>
#include <limits>
#include <list>
#include <utility>
#include <vector>


// ─────────────────────────────────────────────────────────────
// helper (v2 固有): DPU 側 cold partial serialize 愚直版ラッパ
// ─────────────────────────────────────────────────────────────
//
// 最適化版 L1741-1825 の gather_to_dpu / execute / scatter_from_dpu の
// シーケンスを 1 つにまとめた愚直版エントリポイント (宣言のみ)。
//
// 入力:
//   touched_dpus          : DPU ごとに rebalance 対象かどうか
//   per_dpu_hot_delim_keys: 各 DPU について「cold を切る既存 hot の
//                           先頭 key」列。最適化版では
//                           `hot_delim_keys[base_to_nr_hot_psum[i]
//                           .. base_to_nr_hot_psum[i+1])` として
//                           詰め込まれているが、愚直版では per-DPU に
//                           ばらした 2 次元配列で渡す。
// 出力:
//   out_cold_pairs             : 対象 DPU ごとに、回収された cold 片
//                                (incision で区切られた複数片の連結)
//   out_incisions              : 対象 DPU ごとに、各 incision 地点での
//                                cold pair 累積数 (= incision_indices)
//   out_nr_cold_pairs_per_dpu  : 対象 DPU ごとの cold pair 総数
//                                (= `nr_pairs[d][0]`)
//
// 実装は dpu_emulator / upmem 通信層に属するため、本 docs では宣言のみ。
inline void serialize_cold_on_touched_dpus_naive(
    BPForest&,
    const std::vector<bool>& touched_dpus,
    const std::vector<std::vector<key_uint64_t>>& per_dpu_hot_delim_keys,
    std::vector<std::vector<KVPair>>& out_cold_pairs,
    std::vector<std::vector<uint32_t>>& out_incisions,
    std::vector<uint32_t>& out_nr_cold_pairs_per_dpu);


// ─────────────────────────────────────────────────────────────
// helper (v2 固有): Phase 5 の TASK_MOVE_HOT 送信ラッパ
// ─────────────────────────────────────────────────────────────
//
// 最適化版 L2097-2136 の input_headers[] 構築 + send_task を 1 つに
// まとめた愚直版エントリポイント (宣言のみ)。v1 は DPU 丸ごとリセットに
// 単純化するが、v2 は Phase 2 の partial serialize と対称に partial update
// を維持する。
inline void send_task_move_hot_naive(
    BPForest&,
    const std::vector<bool>& touched_dpus,
    const std::vector<std::list<ChunkedPairsRange>>& cold_lists,
    const std::vector<PairsRange>& hot_ranges,
    const std::vector<uint32_t>& cold_npairs_after);


// ─────────────────────────────────────────────────────────────
// Main: incremental_repartition の愚直版 v2 (DPU-side serialize)
// ─────────────────────────────────────────────────────────────
template <typename Query, typename Result>
inline void BPForest::incremental_repartition_serialize_naive(
    uint32_t nr_queries, const Query queries[], Result results[],
    QueryData<Query, Result>& routed)
{
    // ── 0. balancing off なら何もしない ─────────────────
    if (param.balancing == 0) return;

    // ── 1. Phase 1: 対象 DPU の選別と per_dpu_hot_delim_keys の構築 ──
    //
    // 最適化版 L1691-1738 と同式の threshold を作り、touch 対象を決める。
    // v2 独自: 対象 DPU については既存 hot の切れ目 key 列
    // (`per_dpu_hot_delim_keys`) と、対応する cold 片の KeyRange 列
    // (`per_dpu_cold_key_ranges`) を同時に組み立てる。これは最適化版の
    // `hot_delim_keys[]` / `cold_key_ranges[]` のローカル版。
    //
    const uint32_t cold_partial_cnt_goal
        = nr_queries * std::max(3u, param.balancing + 1) / 3 / nr_base_parts;
    const uint32_t cold_partial_cnt_threshold
        = static_cast<uint32_t>(cold_partial_cnt_goal * param.high_watermark_ratio);

    std::vector<bool> touch(nr_base_parts, false);
    dpu_id_t nr_touched = 0;
    std::vector<std::vector<key_uint64_t>> per_dpu_hot_delim_keys(nr_base_parts);
    std::vector<std::vector<KeyRange>> per_dpu_cold_key_ranges(nr_base_parts);

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        if (routed.cold[idx_dpu].nr_qrys <= cold_partial_cnt_threshold) {
            continue;
        }
        if (!param.enable_incremental) {
            return full_repartition_naive(nr_queries, queries, results, routed);
        }

        // cold_delims[idx_dpu] から cold_delims[idx_dpu+1] の間の
        // HotPartitionDelim を順に舐め、cold 片の KeyRange と既存 hot の
        // 開始 key を抽出する (最適化版 L1707-1726)。
        //
        // 不変条件: kranges[i] は i 番目 cold 片の KeyRange、
        //           hkeys[j] は i 番目と i+1 番目 cold 片の間にある
        //           「internal な」既存 hot の開始 key。
        //           よって `hkeys.size() == kranges.size() - 1`。
        std::vector<key_uint64_t>& hkeys = per_dpu_hot_delim_keys[idx_dpu];
        std::vector<KeyRange>& kranges = per_dpu_cold_key_ranges[idx_dpu];

        const key_uint64_t base_min = cold_delims[idx_dpu]->first;
        const key_uint64_t base_max
            = (cold_delims[idx_dpu + 1] == delims.cend())
                  ? KEY_MAX
                  : cold_delims[idx_dpu + 1]->first - 1;

        key_uint64_t cur_begin = base_min;
        bool tail_taken_by_hot = false;  // 末尾の既存 hot が base_max まで届くか
        for (auto it = std::next(cold_delims[idx_dpu]);
             it != cold_delims[idx_dpu + 1]; ++it) {
            const key_uint64_t hot_lo = it->first;
            const auto& hd = std::get<HotPartitionDelim>(it->second);

            if (cur_begin < hot_lo) {
                // 既存 hot の手前に非空の cold 片がある。この cold 片を
                // 記録し、対応する hot の開始 key を incision として push。
                kranges.push_back({cur_begin, hot_lo - 1});
                hkeys.push_back(hot_lo);
            }
            if (hd.max_key == base_max) {
                tail_taken_by_hot = true;
            }
            cur_begin = hd.max_key + 1;  // base_max == KEY_MAX のときオーバーフロー可
        }
        // 末尾の cold 片 (最後の既存 hot より後ろの領域) の処理。
        // - cold で終わるパターン: tail の cold 片を 1 つ push する。
        // - hot で終わるパターン (base_max まで既存 hot に取られている):
        //   push 済みの hkeys の末尾が「対応する cold 片を持たない末尾の
        //   飾り」になっているので pop。
        //   (`cur_begin <= base_max` で分岐すると base_max == KEY_MAX 時の
        //    オーバーフローで誤判定するため、別途 flag で検出する。)
        if (tail_taken_by_hot) {
            if (!hkeys.empty()) hkeys.pop_back();
        } else if (cur_begin <= base_max) {
            kranges.push_back({cur_begin, base_max});
        }

        if (!kranges.empty()) {
            touch[idx_dpu] = true;
            nr_touched++;
        }
    }
    if (nr_touched == 0) return;

    // ── 2. Phase 2: DPU 側 partial serialize ───────────
    //
    // 最適化版 L1741-1825 と同じ意味論で TASK_SERIALIZE を送り、cold 片と
    // incision 位置を回収する。ここはプロトコル維持で、ラッパ関数経由で
    // 呼ぶだけ (メンバ buffer ではなくローカルに受ける)。
    //
    std::vector<std::vector<KVPair>> retrieved_cold(nr_base_parts);
    std::vector<std::vector<uint32_t>> incisions(nr_base_parts);
    std::vector<uint32_t> nr_cold_total(nr_base_parts, 0);
    serialize_cold_on_touched_dpus_naive(
        *this, touch, per_dpu_hot_delim_keys,
        retrieved_cold, incisions, nr_cold_total);

    // retrieved_cold[idx_dpu] には「incision で区切られた複数 cold 片」が
    // 1 本に連結されて入っている。
    // incisions[idx_dpu][k] = k 番目 incision までの cold pair 累積数。

    // ── 3. 閾値定数 (最適化版 L1887-1899 と同式) ─────────
    const dpu_id_t nr_existing_hots
        = static_cast<dpu_id_t>(delims.size()) - nr_base_parts;
    constexpr uint32_t W = IsPointQuery<Query> ? 1 : 2;
    const uint32_t hot_load
        = (nr_queries * W + nr_base_parts - 1) / nr_base_parts;
    const uint32_t cold_endpoint_cnt_goal
        = nr_queries * W * std::max(3u, param.balancing + 1) / 3 / nr_base_parts;

    // ローカル一時領域 (愚直化 (e))
    std::vector<std::list<ChunkedPairsRange>> cold_lists(nr_base_parts);
    std::vector<std::vector<IncrementalOriginPiece>> origins(nr_base_parts);
    std::vector<std::vector<uint32_t>> chunk_load_bufs(nr_base_parts);
    std::vector<std::vector<KeyRange>> piece_key_ranges(nr_base_parts);

    std::vector<NewHotRange> new_hots_local;
    new_hots_local.reserve(nr_base_parts);
    std::vector<std::pair<dpu_id_t, uint32_t>> cold_loads_local(nr_base_parts);
    std::vector<uint32_t> cold_npairs_after(nr_base_parts, 0);

    // ── 4. Phase 3: 対象 DPU ごとに hot 選出 ─────────────
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        if (!touch[idx_dpu]) {
            cold_loads_local[idx_dpu]
                = {idx_dpu, routed.cold[idx_dpu].nr_qrys * W};
            continue;
        }

        // ── 4a. incision_indices から origin 片を復元 ────
        //
        // v1 は「既存 hot delim の key 境界で cold KV 配列を線形分割」
        // していたが、v2 では DPU から incision_indices を受け取って
        // いるので配列オフセットでスライスするだけ。
        //
        // 各 origin の KeyRange は Phase 1 で構築した
        // `per_dpu_cold_key_ranges[idx_dpu]` を順に index 引きする。
        // origins_here と kranges は同じ片数・同じ順序。
        //
        auto& origins_here = origins[idx_dpu];
        {
            const std::vector<KeyRange>& kranges
                = per_dpu_cold_key_ranges[idx_dpu];
            const KVPair* const cold_begin = retrieved_cold[idx_dpu].data();
            const KVPair* cursor = cold_begin;
            uint32_t pair_cursor = 0;

            for (const uint32_t incision_pos : incisions[idx_dpu]) {
                const uint32_t nr_in_piece = incision_pos - pair_cursor;
                if (nr_in_piece > 0) {
                    const size_t idx_piece = origins_here.size();
                    origins_here.push_back(IncrementalOriginPiece{
                        PairsRange{cursor, cursor + nr_in_piece},
                        kranges[idx_piece],
                        nullptr});
                }
                cursor += nr_in_piece;
                pair_cursor = incision_pos;
            }
            if (pair_cursor < nr_cold_total[idx_dpu]) {
                const uint32_t nr_in_piece
                    = nr_cold_total[idx_dpu] - pair_cursor;
                const size_t idx_piece = origins_here.size();
                origins_here.push_back(IncrementalOriginPiece{
                    PairsRange{cursor, cursor + nr_in_piece},
                    kranges[idx_piece],
                    nullptr});
            }
        }

        if (origins_here.empty()) {
            cold_loads_local[idx_dpu] = {idx_dpu, 0};
            touch[idx_dpu] = false;
            continue;
        }

        // chunk load buffer を per-DPU で確保し、各 origin に slice を
        // 割り当てる。初期 cold_list は「origin そのもの」の列。
        size_t total_nchunks = 0;
        for (const IncrementalOriginPiece& o : origins_here) {
            total_nchunks
                += (o.range.npairs() + KVPairsChunkSize - 1) / KVPairsChunkSize;
        }
        chunk_load_bufs[idx_dpu].assign(total_nchunks, 0u);

        auto& cold_list = cold_lists[idx_dpu];
        auto& pkr = piece_key_ranges[idx_dpu];
        uint32_t* load_cursor = chunk_load_bufs[idx_dpu].data();
        for (IncrementalOriginPiece& o : origins_here) {
            o.load_base = load_cursor;
            const size_t nch
                = (o.range.npairs() + KVPairsChunkSize - 1) / KVPairsChunkSize;
            load_cursor += nch;

            cold_list.emplace_back(o.range, o.load_base);
            pkr.push_back(o.key_range);
        }

        // ── 4b. base_npairs / hot_npairs (最適化版 L1938-1947) ──
        uint32_t cold_npairs = 0;
        for (const ChunkedPairsRange& p : cold_list) {
            cold_npairs += static_cast<uint32_t>(p.npairs());
        }
        uint32_t base_npairs = cold_npairs;
        for (auto iter = std::next(cold_delims[idx_dpu]);
             iter != cold_delims[idx_dpu + 1]; ++iter) {
            const auto& hd = std::get<HotPartitionDelim>(iter->second);
            base_npairs += nr_pairs[hd.dpu].get()[1];
        }
        const uint32_t hot_npairs
            = (base_npairs + param.balancing - 1) / param.balancing;

        // ── 4c. load 推定 (愚直 (a): chunk 線形走査) ─────
        //
        // 最適化版 (L1856-1935) は hot_delim_keys の upper_bound で
        // 「key がどの cold 片のどの chunk か」と「出るか (= 既存 hot 側)」
        // を判定する。愚直版は cold 片を 1 本ずつ走査し、どの片にも
        // 入らなければ捨てる (= 既存 hot 側に落ちた endpoint)。
        //
        uint32_t cold_endpoint_cnt = 0;
        {
            const auto find_chunk_in_pieces = [&](key_uint64_t key) -> DataChunkIterator {
                for (ChunkedPairsRange& p : cold_list) {
                    if (p.PairsRange::begin() == p.PairsRange::end()) continue;
                    if (p.PairsRange::begin()->key <= key
                        && key <= (p.PairsRange::end() - 1)->key) {
                        return find_chunk_containing_key_naive(p, key);
                    }
                }
                return DataChunkIterator{};
            };

            if constexpr (IsPointQuery<Query>) {
                for (const auto& qry_vec : routed.cold[idx_dpu].qrys) {
                    for (const auto& qry : qry_vec) {
                        const key_uint64_t key = PointQueryToKey<Query>{}(qry);
                        auto ch = find_chunk_in_pieces(key);
                        if (ch != DataChunkIterator{}) ch->load()++;
                    }
                }
                cold_endpoint_cnt = routed.cold[idx_dpu].nr_qrys;
            } else {
                uint32_t prev_orig = std::numeric_limits<uint32_t>::max();
                for (const auto& idx_vec : routed.cold[idx_dpu].orig_idxs) {
                    for (const auto orig_idx : idx_vec) {
                        if (orig_idx == prev_orig) continue;
                        prev_orig = orig_idx;
                        const KeyRange& rr
                            = RangeQueryToRange<Query>{}(queries[orig_idx]);
                        for (const auto key : {rr.begin, rr.end}) {
                            auto ch = find_chunk_in_pieces(key);
                            if (ch != DataChunkIterator{}) {
                                ch->load()++;
                                cold_endpoint_cnt++;
                            }
                        }
                    }
                }
            }
        }

        // ── 4d. 早期復帰 (最適化版 L1948-1954) ──────────
        //
        // touched DPU でも、chunk load を取ってみたら goal 以下だった
        // というケース。Algorithm の不可欠な分岐なので残す。
        //
        if (cold_endpoint_cnt <= cold_endpoint_cnt_goal) {
            cold_npairs_after[idx_dpu] = 0;
            cold_loads_local[idx_dpu] = {idx_dpu, cold_endpoint_cnt};
            touch[idx_dpu] = false;
            cold_list.clear();
            pkr.clear();
            continue;
        }

        // ── 4e. Absolute hot 段 (最適化版 L1956-1990) ────
        //
        // 最適化版は複数 cold 片を 1 回の `find_absolutely_hot_ranges`
        // で処理する (callback が false を返すと全 part を横断して
        // return する)。愚直版も list bridge 経由で一括呼び出しする。
        //
        // hot.key_range.end は、hot の末尾が cold 片の末尾と一致する
        // ときに限り kranges (per-piece) の end を使う (最適化版
        // L1980-1983 と同じロジック)。
        //
        const size_t abs_start = new_hots_local.size();
        find_absolutely_hot_ranges_on_list_naive(
            cold_list, hot_npairs, hot_load,
            [&](size_t piece_idx, ChunkedPairsRange& part,
                DataChunkIterator chunk_begin, DataChunkIterator chunk_end,
                uint32_t load) {
                const KeyRange& kr = pkr[piece_idx];
                NewHotRange hot;
                hot.pairs_range = {chunk_begin.begin(), chunk_end.begin()};
                hot.key_range.begin = hot.pairs_range.begin()->key;
                hot.key_range.end
                    = (hot.pairs_range.end() == part.PairsRange::end()
                              ? kr.end
                              : hot.pairs_range.end()->key - 1);
                hot.load = load;
                new_hots_local.push_back(hot);
                cold_npairs -= static_cast<uint32_t>(hot.pairs_range.npairs());
                cold_endpoint_cnt -= load;
                return cold_endpoint_cnt > cold_endpoint_cnt_goal;
            });

        // ── 4f. cold list を rebuild (愚直化 (d)) ────────
        {
            std::vector<PairsRange> hots_in_dpu;
            hots_in_dpu.reserve(new_hots_local.size() - abs_start);
            for (size_t i = abs_start; i < new_hots_local.size(); i++) {
                hots_in_dpu.push_back(new_hots_local[i].pairs_range);
            }
            std::sort(hots_in_dpu.begin(), hots_in_dpu.end(),
                [](const PairsRange& a, const PairsRange& b) {
                    return a.begin() < b.begin();
                });
            rebuild_cold_list_with_origins_naive(
                cold_list, pkr, origins_here, hots_in_dpu);
        }

        // ── 4g. Relative hot 段 (最適化版 L1999-2058) ────
        //
        // 段間ゲート (L1999) と hot_hook 内ゲート (L2019) の両方を保持。
        //
        if (cold_endpoint_cnt > cold_endpoint_cnt_goal && !cold_list.empty()) {
            const uint32_t nr_relative_hots = cold_endpoint_cnt / hot_load;

            find_relatively_hot_ranges_on_list_naive(
                cold_list, hot_npairs, nr_relative_hots,
                [&](size_t piece_idx, const ChunkedPairsRange& part,
                    const PairsRange& range, uint32_t load) {
                    const KeyRange& kr = pkr[piece_idx];
                    NewHotRange hot;
                    hot.pairs_range = range;
                    hot.key_range.begin = range.begin()->key;
                    hot.key_range.end
                        = (range.end() == part.PairsRange::end()
                                  ? kr.end
                                  : range.end()->key - 1);
                    hot.load = load;
                    new_hots_local.push_back(hot);
                    cold_npairs -= static_cast<uint32_t>(hot.pairs_range.npairs());
                    cold_endpoint_cnt -= load;
                    return cold_endpoint_cnt > cold_endpoint_cnt_goal;
                });

            // 全 hot (abs + rel) で cold list を再 rebuild
            std::vector<PairsRange> hots_in_dpu;
            hots_in_dpu.reserve(new_hots_local.size() - abs_start);
            for (size_t i = abs_start; i < new_hots_local.size(); i++) {
                hots_in_dpu.push_back(new_hots_local[i].pairs_range);
            }
            std::sort(hots_in_dpu.begin(), hots_in_dpu.end(),
                [](const PairsRange& a, const PairsRange& b) {
                    return a.begin() < b.begin();
                });
            rebuild_cold_list_with_origins_naive(
                cold_list, pkr, origins_here, hots_in_dpu);
        }

        cold_npairs_after[idx_dpu] = cold_npairs;
        cold_loads_local[idx_dpu] = {idx_dpu, cold_endpoint_cnt};

        // ── 4h. bailout (最適化版 L2063-2066) ───────────
        if (nr_existing_hots + static_cast<dpu_id_t>(new_hots_local.size())
            > nr_base_parts) {
            return full_repartition_naive(nr_queries, queries, results, routed);
        }
    }

    const dpu_id_t hot_count = static_cast<dpu_id_t>(new_hots_local.size());
    if (hot_count == 0) return;

    // ── 5. Phase 4: hot を低負荷 DPU に割り当て (最適化版 L2072-2093) ──
    //
    // 既に hot を持つ DPU は second = UINT32_MAX にして候補から除外。
    //
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        if (hot_delims[idx_dpu] != DelimIter{}) {
            cold_loads_local[idx_dpu].second
                = std::numeric_limits<uint32_t>::max();
        }
    }
    std::partial_sort(cold_loads_local.begin(),
        cold_loads_local.begin() + hot_count, cold_loads_local.end(),
        [](auto& l, auto& r) { return l.second < r.second; });
    std::sort(new_hots_local.begin(), new_hots_local.end(),
        [](auto& l, auto& r) { return l.load > r.load; });

    std::vector<PairsRange> hot_ranges_local(
        nr_base_parts, PairsRange{nullptr, nullptr});
    for (dpu_id_t idx_hot = 0; idx_hot < hot_count; idx_hot++) {
        const dpu_id_t idx_dpu = cold_loads_local[idx_hot].first;
        const NewHotRange& nh = new_hots_local[idx_hot];
        hot_ranges_local[idx_dpu] = nh.pairs_range;
        hot_delims[idx_dpu] = delims.emplace(
            nh.key_range.begin,
            HotPartitionDelim{nh.key_range.end, idx_dpu}).first;
        nr_pairs[idx_dpu].get()[1]
            = static_cast<uint32_t>(nh.pairs_range.npairs());
    }

    // ── 6. Phase 5: TASK_MOVE_HOT 相当の送信 ───────────
    //
    // v2 は Phase 2 の partial serialize と対称に、送信側も TASK_MOVE_HOT
    // で cold / hot を部分更新する (最適化版 L2097-2125 と同じ流れ)。
    // メンバ input_headers[] は使わずローカル配列で組むため、ラッパ関数
    // send_task_move_hot_naive 経由で呼ぶ。
    //
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        if (touch[idx_dpu]) {
            nr_pairs[idx_dpu].get()[0] = cold_npairs_after[idx_dpu];
        }
    }
    combine_delims();
    send_task_move_hot_naive(
        *this, touch, cold_lists, hot_ranges_local, cold_npairs_after);

    route_queries(nr_queries, queries, results, routed);
}


// ─────────────────────────────────────────────────────────────
// 参考: v1 / v2 / 最適化版の Phase 2 比較
// ─────────────────────────────────────────────────────────────
//
//              | DPU 側タスク      | host 側分割                  | cold 片境界の根拠
// ─────────────┼───────────────────┼──────────────────────────────┼─────────────────────
// 最適化版      | TASK_SERIALIZE    | cold_ranges_lists[] を       | DPU から返された
//              | (do_cold=true,    | chunked_cold_ranges[] pool に | incision_indices
//              |  do_hot=false)    | incision_indices で流し込む   | (最適化版 L1793-1817)
// ─────────────┼───────────────────┼──────────────────────────────┼─────────────────────
// v2 (この file)| TASK_SERIALIZE    | local std::list に           | DPU から返された
//              | (do_cold=true)    | emplace_back で順次投入      | incision_indices
//              |                   | (find_* 呼び出し時は bridge   | (最適化版と同じ)
//              |                   |  で一時 LinkedList を組む)    |
// ─────────────┼───────────────────┼──────────────────────────────┼─────────────────────
// v1           | 全 cold KV を     | host 側で既存 hot delim の    | host 側が持つ
// (whole-DPU)  | 丸ごと回収        | key で線形走査して分割        | `delims` / HotDelim
// ─────────────┴───────────────────┴──────────────────────────────┴─────────────────────
//
// v1 は DPU 側プロトコルを単純化する代わりに host 側分割が必要。
// v2 は DPU 側プロトコルを残す代わりに host 側分割が不要。
// 「意味論を保ったまま最も愚直」という観点では v1 の方がコードが短いが、
// 最適化版の全体像を学ぶ目的には v2 の方が 1:1 対応が取りやすい。


// ─────────────────────────────────────────────────────────────
// 参考: 最適化版との対応表 (bpforest.ipp の行番号)
// ─────────────────────────────────────────────────────────────
//  愚直版ステップ                      | 最適化版 (bpforest.ipp)
// ────────────────────────────────────┼──────────────────────────
//  1 対象 DPU 選別 + hkeys/kranges     | L1691-1738
//                                      | (`per_dpu_hot_delim_keys`,
//                                      |  `per_dpu_cold_key_ranges` は
//                                      |  最適化版の `hot_delim_keys[]`,
//                                      |  `cold_key_ranges[]` のローカル版)
//  2 serialize_cold_on_touched_dpus   | L1741-1825 (プロトコル維持、
//    _naive                            |  ラッパで包むだけ)
//  3 閾値定数                          | L1887-1899
//  4a incision_indices で origin slice | L1793-1817 を host 側でスライス
//  4b base_npairs / hot_npairs         | L1938-1947
//  4c load 推定                        | L1856-1935 (chunk 線形走査化)
//    └─ (a) 線形 chunk 特定            | L1878, L1884 を置換
//  4d 閾値下早期復帰                   | L1948-1954 (保持)
//  4e Absolute hot 段                  | L1956-1990
//    ├─ bridge (c) std::list 複数片    | L1960 の `begin_part, end_part`
//    ├─ 段間ゲート                     | (最適化版は 4d の後すぐ実行)
//    └─ hot_hook 内ゲート (保持)        | L1989 の return 条件
//  4f cold list rebuild (abs 後)       | L1967-1977, 1991-1996 を置換
//                                      | 愚直化 (d)
//  4g Relative hot 段                  | L1999-2058
//    ├─ 段間ゲート (保持)              | L1999 の条件式
//    ├─ bridge (c) std::list           | L2005 の iter 形を置換
//    └─ hot_hook 内ゲート (保持)        | L2019 の return 条件
//    └─ cold list rebuild (rel 後)     | L2022-2057 を置換 (愚直化 (d))
//  4h bailout                          | L2063-2066
//  5  hot → DPU マッチング             | L2072-2093
//  6  TASK_MOVE_HOT 送信 & re-route    | L2097-2136
//
// 意味論: hot 集合 / delims / hot_delims / nr_pairs / DPU 側 B+ tree
//         いずれも最適化版と一致する (ソート tie-break の違いを除けば)。
//         `cold_endpoint_cnt` の scalar 追跡も最適化版と同じ値を取る。
//
// v1 (incremental_repartition_naive.hpp) との違い:
//   - v2: DPU 側 partial serialize プロトコル維持、
//          incision_indices で cold 片を配列スライス、
//          Phase 5 は TASK_MOVE_HOT で部分更新。
//   - v1: 対象 DPU から cold 丸ごと回収、host 側 key 境界分割、
//          Phase 5 は TASK_INIT 相当で丸ごとリセット。
