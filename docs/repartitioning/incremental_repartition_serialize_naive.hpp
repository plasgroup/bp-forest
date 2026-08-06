/// @file incremental_repartition_serialize_naive.hpp
/// @brief `BPForest::incremental_repartition` +
///        `BPForest::incremental_repartition_worker_cold`
///        (host/inc/bpforest.ipp) を、同一シグネチャ・同一意味論のまま
///        最も愚直に書き直した参照実装 (v2)。
///        参照用であり、ビルド対象ではない。
///
/// スコープ (詳細は docs/repartitioning/README.md §0.1):
///   デフォルトパラメータ (`more_hotness == 1`, `greedy_only == false`)
///   の **cold-carve 経路** かつ predecessor 以外のクエリでのみ
///   「最適化版と同じ最終状態」の契約が成立する。hot partition 自体の
///   分割 (`param.enable_hot_split` / `..._worker_hot`)、
///   `param.more_hotness` 係数、`param.greedy_only` 分岐、predecessor 用
///   `std::lower_bound`、並列 worker 化 (`TmpDataForIncRepartition` +
///   mutex + `parallel_run`) は未反映。
///   また最適化版の戻り値は `Balanced` で、`Balanced::No` を受けて full へ
///   escalate するのは呼出し側の `BPForest::repartition` ラッパ。この愚直版は
///   `void` を返し自分で `full_repartition_naive` を呼ぶが、最終状態は同じ。
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
///       愚直版でも段間 (abs / rel 各段の直前) と hot_hook 内
///       (abs / rel 各 hot_hook の末尾) の両方の
///       `cold_endpoint_cnt > goal` 判定を維持する。
///   (c) cold 範囲列を `LinkedList<ChunkedPairsRange>` ではなく
///       `std::list<ChunkedPairsRange>` で持つ。find_* の呼び出し時
///       だけ full_repartition_naive.hpp の bridge 関数経由で一時
///       LinkedList を組み立てる。
///   (d) carve のたびに cold list を in-place splice で更新するのではなく、
///       `rebuild_cold_list_with_origins_naive` (v1 の helper) で毎回
///       build-from-scratch する。
///   (e) BPForest 側の「一時データ用」メンバ
///       (`chunked_cold_ranges[,_lists]`, `new_hots`, `cold_loads`,
///        `cold_npairs_list`, `input_headers`, `hot_delim_keys`,
///        `base_to_nr_hot_psum`, `nr_extracted_hots`, `incision_indices`,
///        `cold_key_ranges`, `hot_ranges`, `data_buf`, worker 間受け渡しの
///        `any_tmp_data`、および per-thread scratch の `chunk2load`
///        = `inline static thread_local`) を全てローカル変数に置換。
///
/// 保っている意味論 (= 最適化版と同じ):
///   - Phase 1 で対象 DPU を限定 (incremental のアイデンティティ)
///   - Phase 1 の 2 層判定: `overload_threshold.threshold_for(...)` による
///     trigger (rebalance するか) と、goal そのものによる対象選定
///     (`do_cold` = どの DPU を作り直すか)
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
// 最適化版 `incremental_repartition` の Phase 2 (SerializationCommander
// gather_to_dpu -> execute -> SerializaionNrPairsReceiver scatter_from_dpu
// -> "alloc" ブロックの incision スライス -> "recv" ブロック) を
// 1 つにまとめた愚直版エントリポイント (宣言のみ)。
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
// 実装は fake_dpu / upmem 通信層に属するため、本 docs では宣言のみ。
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
// 最適化版 `incremental_repartition` 末尾の input_headers[] 再構築
// (TASK_MOVE_HOT) + UpdatedPartitionsSender gather + execute を 1 つに
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
    // ── 1. Phase 1: trigger 判定と対象 DPU の選別、
    //      および per_dpu_hot_delim_keys の構築 ─────────
    //
    // 最適化版 `incremental_repartition` の Phase 1
    // (ScopedTimer{"retrieve"} 冒頭のループ) に対応。
    //
    // 判定は 2 層 (README §3.3.1):
    //   trigger  … `overload_threshold.threshold_for(...)` 超え。1 つでも
    //               立てば「このバッチは rebalance する」。
    //   do_cold  … goal そのもの超え。作り直す DPU 集合 (= touch) を決める。
    // `threshold_for` は必ず goal 以上を返すので、対象集合は trigger
    // 集合を含む。
    // 「やるかどうか」は保守的に、「やるなら誰を」は緩めに、という非対称。
    //
    // `param.balancing` は CLI で `>= 1` が強制されるので 0 分岐は無い。
    //
    // v2 独自: 対象 DPU については既存 hot の切れ目 key 列
    // (`per_dpu_hot_delim_keys`) と、対応する cold 片の KeyRange 列
    // (`per_dpu_cold_key_ranges`) を同時に組み立てる。これは最適化版の
    // `hot_delim_keys[]` / `cold_key_ranges[]` のローカル版。
    //
    const dpu_id_t nr_existing_hots
        = static_cast<dpu_id_t>(delims.size()) - nr_base_parts;
    // Bonferroni 補正用の検定数 (1 バッチあたり)。
    const unsigned bonferroni_family
        = static_cast<unsigned>(nr_base_parts) + static_cast<unsigned>(nr_existing_hots);
    // 最適化版はこの goal に `param.more_hotness` 係数が乗り、
    // `param.greedy_only` 時は `× param.balancing` の式に変わる。
    const uint32_t cold_partial_cnt_goal
        = nr_queries * std::max(3u, param.balancing + 1) / 3 / nr_base_parts;
    const uint32_t cold_partial_cnt_threshold
        = overload_threshold.threshold_for(nr_queries, cold_partial_cnt_goal, bonferroni_family);

    std::vector<bool> touch(nr_base_parts, false);
    dpu_id_t nr_touched = 0;
    bool trigger = false;
    std::vector<std::vector<key_uint64_t>> per_dpu_hot_delim_keys(nr_base_parts);
    std::vector<std::vector<KeyRange>> per_dpu_cold_key_ranges(nr_base_parts);

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        trigger = trigger || routed.cold[idx_dpu].nr_qrys > cold_partial_cnt_threshold;
        // do_cold: 対象選定は goal (閾値ではない) で行う。
        if (routed.cold[idx_dpu].nr_qrys <= cold_partial_cnt_goal) {
            continue;
        }

        // cold_delims[idx_dpu] から cold_delims[idx_dpu+1] の間の
        // HotPartitionDelim を順に舐め、cold 片の KeyRange と既存 hot の
        // 開始 key を抽出する (最適化版 Phase 1 の `if (do_cold)` ブロック)。
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
        //
        // NOTE: 最適化版は同じ判定を `if (begin_key != KEY_MAX + 1)` +
        //       `if (begin_key <= max_key) ... else incision_count--;`
        //       で書く。最後の base partition (base_max == KEY_MAX) の
        //       末尾が既存 hot に取られているケースでは外側の if が
        //       false になり `incision_count--` に到達しないため、
        //       incision が 1 つ多いまま DPU に送られる。ただし末尾の
        //       incision 位置は cold 総数と一致するので、"recv" の
        //       復元ループが作る cold 片の列は decrement した場合と
        //       同一 — 冗長なだけで意味論は変わらない。
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
    // 最適化版はこの分岐を loop 内で `trigger && !param.enable_incremental`
    // として持つ (立った時点で `Balanced::No` を返し、呼出し側の
    // `repartition()` ラッパが full へ escalate する)。
    if (!param.enable_incremental) {
        if (trigger) {
            return full_repartition_naive(nr_queries, queries, results, routed);
        }
        return;
    }
    if (!trigger) return;  // 最適化版の `return Balanced::Yes;`
    if (nr_touched == 0) return;

    // ── 2. Phase 2: DPU 側 partial serialize ───────────
    //
    // 最適化版 `incremental_repartition` の SerializationCommander gather
    // -> execute -> SerializaionNrPairsReceiver scatter -> "alloc" /
    // incision スライスと同じ意味論で TASK_SERIALIZE を送り、cold 片と
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

    // ── 3. 閾値定数 ────────────────────────────────────
    //
    // 最適化版 `incremental_repartition_worker_cold` 冒頭の hot_load /
    // cold_endpoint_cnt_goal と同式。ただし最適化版はこれに
    // `param.more_hotness` 係数が乗り、`param.greedy_only` が真のとき
    // goal が `× param.balancing` の式に切り替わる。下の式が数値一致
    // するのはデフォルト値のときだけ (README §0.1, §1.3)。
    //
    // (`nr_existing_hots` は Phase 1 で算出済み。最適化版も同様に
    //  `incremental_repartition` 側で求めて `TmpDataForIncRepartition::
    //  nr_existing_hots` 経由で worker に渡している。)
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

        // ── 4b. base_npairs / hot_npairs ────────────────
        //   最適化版 `incremental_repartition_worker_cold` の
        //   cold_npairs / base_npairs / hot_npairs 算出に対応。
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
        // 最適化版 (`incremental_repartition_worker_cold` の chunk load
        // populate ブロック) は cold 片境界の探索表 (同ブロック内の
        // thread_local `hot_delim_keys`; Phase 1 で DPU に送るメンバの
        // `hot_delim_keys` とは別物) への upper_bound で
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

        // ── 4d. 早期復帰 ────────────────────────────────
        //   最適化版 `incremental_repartition_worker_cold` の
        //   `cold_endpoint_cnt <= cold_endpoint_cnt_goal` 分岐
        //   (TASK_NONE に落として list を捨てる) に対応。
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

        // ── 4e. Absolute hot 段 ────────────────────────
        //   最適化版 `incremental_repartition_worker_cold` の
        //   find_absolutely_hot_ranges(begin_part, end_part, ...) に対応。
        //
        // 最適化版は複数 cold 片を 1 回の `find_absolutely_hot_ranges`
        // で処理する (callback が false を返すと全 part を横断して
        // return する)。愚直版も list bridge 経由で一括呼び出しする。
        //
        // hot.key_range.end は、hot の末尾が cold 片の末尾と一致する
        // ときに限り kranges (per-piece) の end を使う (最適化版の
        // hot_hook 内 `new_hot.key_range` 構築、
        // `cold_key_ranges[idx_in_ary].end` と同じロジック)。
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

        // ── 4g. Relative hot 段 ────────────────────────
        //   最適化版 `incremental_repartition_worker_cold` の
        //   find_relatively_hot_ranges(list.begin(), list.end(), ...) に対応。
        //
        // 段間ゲート (呼出し直前の if) と hot_hook 内ゲート (hot_hook 末尾の
        // return) の両方を保持。最適化版の段間ゲートには
        // `!param.greedy_only &&` が前置される。
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

        // ── 4h. bailout ────────────────────────────────
        //   最適化版 `incremental_repartition` の cold pass 直後の
        //   `nr_existing_hots + hot_count > nr_base_parts` 判定に対応
        //   (最適化版はここで `Balanced::No` を返し、full を呼ぶのは
        //    `repartition()` ラッパ)。同じ判定は worker 側の
        //   `get_next_idx_dpu` 冒頭にもあり、超えた時点で残りの DPU を
        //   処理せず打ち切る。
        if (nr_existing_hots + static_cast<dpu_id_t>(new_hots_local.size())
            > nr_base_parts) {
            return full_repartition_naive(nr_queries, queries, results, routed);
        }
    }

    const dpu_id_t hot_count = static_cast<dpu_id_t>(new_hots_local.size());
    if (hot_count == 0) return;

    // ── 5. Phase 4: hot を低負荷 DPU に割り当て ─────────
    //   最適化版 `incremental_repartition` の partial_sort / sort +
    //   マッチングループに対応。
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
    // で cold / hot を部分更新する (最適化版 `incremental_repartition`
    // 末尾の input_headers[] 再構築 + UpdatedPartitionsSender と同じ流れ)。
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
//              |  do_hot=false)    | incision_indices で流し込む   | (最適化版 "recv" ブロック)
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
// 参考: 最適化版との対応表 (関数 + ブロック単位)
// ─────────────────────────────────────────────────────────────
// 最適化版は worker 分割されており行単位の 1:1 対応は付かない。
// 対応は関数名 + ブロック名で見ること (README §6)。
// I = BPForest::incremental_repartition (orchestration)
// C = BPForest::incremental_repartition_worker_cold (DPU ごとの carve)
//
//  愚直版ステップ                      | 最適化版
// ────────────────────────────────────┼──────────────────────────
//  1 対象 DPU 選別 (trigger / do_cold) | I: Phase 1 = ScopedTimer{"retrieve"}
//    + hkeys/kranges                   |    冒頭のループ (`if (do_cold)`
//                                      |    ブロックが hot_delim_keys[] /
//                                      |    cold_key_ranges[] を書き出す)
//                                      | (`per_dpu_hot_delim_keys`,
//                                      |  `per_dpu_cold_key_ranges` は
//                                      |  最適化版の `hot_delim_keys[]`,
//                                      |  `cold_key_ranges[]` のローカル版)
//  2 serialize_cold_on_touched_dpus   | I: 同ブロックの TASK_SERIALIZE
//    _naive                            |    gather/exec/scatter (プロトコル
//                                      |    維持、ラッパで包むだけ)
//  3 閾値定数                          | C: 冒頭の hot_load /
//                                      |    cold_endpoint_cnt_goal
//                                      |    (more_hotness / greedy_only 分岐
//                                      |     は未反映)
//  4a incision_indices で origin slice | I: "alloc" ブロックと直後の
//                                      |    incision スライス (chunked_cold
//                                      |    _ranges pool への流し込み)
//  4b base_npairs / hot_npairs         | C: cold_npairs / base_npairs /
//                                      |    hot_npairs 算出
//  4c load 推定                        | C: chunk2load populate ブロック
//    └─ (a) 線形 chunk 特定            |    (cold 片 upper_bound + chunk
//                                      |     upper_bound を置換)
//  4d 閾値下早期復帰                   | C: cold_endpoint_cnt <= goal 分岐
//                                      |    (TASK_NONE 化。保持)
//  4e Absolute hot 段                  | C: find_absolutely_hot_ranges 呼出
//    ├─ bridge (c) std::list 複数片    |    `begin_part, end_part`
//    ├─ 段間ゲート                     |    (最適化版は 4d の後すぐ実行)
//    └─ hot_hook 内ゲート (保持)        |    hot_hook 末尾の return 条件
//  4f cold list rebuild (abs 後)       | C: hot_hook 内の list.insert /
//                                      |    段直後の空 piece erase を置換
//                                      |    愚直化 (d)
//  4g Relative hot 段                  | C: find_relatively_hot_ranges 呼出
//    ├─ 段間ゲート (保持)              |    呼出し直前の if 条件
//    |                                 |    (最適化版は !greedy_only 付き)
//    ├─ bridge (c) std::list           |    `list.begin(), list.end()`
//    └─ hot_hook 内ゲート (保持)        |    hot_hook 末尾の return 条件
//    └─ cold list rebuild (rel 後)     |    carved_cold_range splice を置換
//                                      |    (愚直化 (d))
//  4h bailout                          | I: cold pass 直後の
//                                      |    nr_existing_hots + hot_count 判定
//                                      |    (C: get_next_idx_dpu 冒頭の
//                                      |     同判定も同じ役割)
//  —  (未対応)                         | I: hot pass ("hot" ブロック,
//                                      |    ..._worker_hot) と その直後の
//                                      |    nr_new_pieces 込み bailout,
//                                      |    hot_split_plans の commit
//  5  hot → DPU マッチング             | I: partial_sort / sort +
//                                      |    delims.emplace ループ
//  6  TASK_MOVE_HOT 送信 & re-route    | I: input_headers[] 再構築 +
//                                      |    UpdatedPartitionsSender +
//                                      |    末尾の route_queries
//
// 意味論: hot 集合 / delims / hot_delims / nr_pairs / DPU 側 B+ tree
//         いずれも最適化版と一致する (ソート tie-break の違いを除けば)。
//         `cold_endpoint_cnt` の scalar 追跡も最適化版と同じ値を取る。
//         ただし成立するのは冒頭 / README §0.1 のスコープ内でのみ。
//
// v1 (incremental_repartition_naive.hpp) との違い:
//   - v2: DPU 側 partial serialize プロトコル維持、
//          incision_indices で cold 片を配列スライス、
//          Phase 5 は TASK_MOVE_HOT で部分更新。
//   - v1: 対象 DPU から cold 丸ごと回収、host 側 key 境界分割、
//          Phase 5 は TASK_INIT 相当で丸ごとリセット。
