/// @file incremental_repartition_naive.hpp
/// @brief `BPForest::incremental_repartition` +
///        `BPForest::incremental_repartition_worker_cold`
///        (host/inc/bpforest.ipp) を、同一シグネチャ・同一意味論のまま
///        最も愚直に書き直した参照実装 (v1)。
///        参照用であり、ビルド対象ではない。
///
/// スコープ (詳細は docs/repartitioning/README.md §0.1):
///   デフォルトパラメータ (`more_hotness == 1`, `greedy_only == false`)
///   の **cold-carve 経路** かつ predecessor 以外のクエリでのみ
///   「最適化版と同じ最終状態」の契約が成立する。hot partition 自体の
///   分割 (`param.enable_hot_split` /
///   `incremental_repartition_worker_cold` と対になる `..._worker_hot`)、
///   `param.more_hotness` 係数、`param.greedy_only` 分岐、predecessor 用
///   `std::lower_bound`、並列 worker 化 (`TmpDataForIncRepartition` +
///   mutex + `parallel_run`) は未反映。
///   また最適化版の戻り値は `Balanced` で、`Balanced::No` を受けて full へ
///   escalate するのは呼出し側の `BPForest::repartition` ラッパ。この愚直版は
///   `void` を返し自分で `full_repartition_naive` を呼ぶが、最終状態は同じ。
///
/// v1 の方針: **対象 DPU から cold + 既存 hot を「丸ごと」回収** する。
///           これにより incision_indices / hot_delim_keys / base_to_nr_hot_psum
///           のような「DPU 側 serialize プロトコル」用の補助データを
///           一切使わずに cold 区間列を host 側で復元できる。
///
///           v2 (`incremental_repartition_serialize_naive.hpp`) は最適化版
///           と同じく DPU 側で cold 片だけ serialize する経路を取る。
///
/// 剥がした最適化 (= 愚直化の内容):
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
///       だけ full_repartition_naive.hpp の bridge 関数経由で
///       LinkedList を組み立て直す。
///   (d) carve のたびに cold list を in-place に splice で更新する
///       のではなく、`origins` (carve 前の cold 片列) と hot 区間の
///       vector から毎回 build-from-scratch で再構築する。
///   (e) BPForest 側の「一時データ用」メンバ
///       (`chunked_cold_ranges[,_lists]`, `new_hots`, `cold_loads`,
///        `cold_npairs_list`, `input_headers`, `hot_delim_keys`,
///        `base_to_nr_hot_psum`, `nr_extracted_hots`, `incision_indices`,
///        `cold_key_ranges`, `hot_ranges`, `data_buf`, worker 間受け渡しの
///        `any_tmp_data`、および per-thread scratch の `chunk2load`
///        = `inline static thread_local`) を全てローカル変数に置換。
///   (f) **DPU 側 partial serialize プロトコルを廃止** (v1 独自):
///       対象 DPU から cold+hot を丸ごと回収し、host 側で
///       既存 hot delim の key 境界で cold 区間列を復元する。
///       incision_indices / hot_delim_keys / base_to_nr_hot_psum の
///       計算と受け渡しが全て消える。
///
/// 保っている意味論 (= 最適化版と同じもの):
///   - Phase 1 で対象 DPU を限定 (= incremental のアイデンティティ)
///   - Phase 1 の 2 層判定: `overload_threshold.threshold_for(...)` による
///     trigger (rebalance するか) と、goal そのものによる対象選定
///     (`do_cold` = どの DPU を作り直すか)
///   - `param.enable_incremental == false` で full に委譲
///   - `hot_load`, `cold_endpoint_cnt_goal`, `hot_npairs` の計算式
///   - find_absolutely → find_relatively の 2 段構え
///   - 段間と hot_hook 内の `cold_endpoint_cnt > goal` 判定
///   - `nr_existing_hots + new_hots > nr_base_parts` での full 委譲
///   - 既存 hot を持つ DPU は新規 hot の配置先候補から除外
///   - `delims` / `hot_delims` / `nr_pairs` の最終状態
///   - 末尾の `route_queries` 再実行

#pragma once

#include "bpforest.hpp"
#include "full_repartition_naive.hpp"  // single-piece / list bridge, helpers

#include <algorithm>
#include <array>
#include <limits>
#include <list>
#include <utility>
#include <vector>


// ─────────────────────────────────────────────────────────────
// helper (v1 固有): 対象 DPU から全 KV ペアを丸ごと取得
// ─────────────────────────────────────────────────────────────
//
// 最適化版 `incremental_repartition` の Phase 2 (SerializationCommander
// gather -> execute -> SerializaionNrPairsReceiver scatter -> "alloc"
// ブロックの incision スライス -> "recv" ブロック) は
// 「cold 部分のみ、incision_indices 付きで」取得するが、
// v1 愚直版はこれを捨て、DPU 側の cold + hot を 1 本の KV ペア列として
// 取得する (concept 上は既存の `retrieve_all_data` のシングル DPU 版)。
//
// 戻り値: [0, nr_cold) が cold、[nr_cold, nr_cold+nr_hot) が hot。
//         cold 部分は base 内の key 昇順に並ぶが、既存 hot 範囲の
//         keys は含まない (= base 内に「穴」がある)。後半 hot は
//         本関数では使わない (incremental_repartition の対象外) が、
//         retrieve する際の副産物として上記の並びで返される。
//
// 実装詳細はこの関数では詰めない (DPU 側 task の追加が必要なので)。
inline std::vector<KVPair> retrieve_dpu_all_pairs_naive(
    BPForest&, dpu_id_t idx_dpu);  // declaration only


// ─────────────────────────────────────────────────────────────
// helper: origin cold 片の情報 (v1 / v2 共通)
// ─────────────────────────────────────────────────────────────
//
// 1 DPU の「carve 開始前の cold 片」を表すレコード。
// - `range`      : KV ペア列のスライス
// - `key_range`  : この cold 片がカバーする key 範囲 (後段で hot の
//                   key_range.end を決めるのに使う)
// - `load_base`  : per-DPU chunk load buffer 内のこの cold 片の slice 先頭
struct IncrementalOriginPiece {
    PairsRange range;
    KeyRange key_range;
    uint32_t* load_base;
};


// ─────────────────────────────────────────────────────────────
// helper: carve 後の cold_list を origins + 全 hot から build-from-
//         scratch で組み立て直す
// ─────────────────────────────────────────────────────────────
//
// 前提:
//   - `origins` は 1 DPU の ORIGINAL cold 片列 (carve 開始前のもの)
//     で、各 origin の `load_base` は populate 済み。
//   - `all_hots_sorted` は「この DPU の origins のいずれかに含まれる」
//     hot の PairsRange を begin ポインタ昇順にソートしたもの。
//   - hot は chunk 境界に alignment されている (find_absolutely /
//     find_relatively が DataChunkIterator 境界で切るため)。
//
// 出力:
//   - `cold_list` は origins ごとに、hot で切り取った残りの sub-piece 列。
//     各 sub-piece の `p_load` は origin の `load_base` + chunk offset。
//   - `piece_key_ranges` は cold_list と並行して各 sub-piece の KeyRange
//     を保持する。hot で切られた sub-piece の KeyRange は:
//       * 先頭 sub-piece:  {origin.kr.begin,         hot.begin->key - 1}
//       * 中間 sub-piece:  {prev_hot.end->key,       next_hot.begin->key - 1}
//       * 末尾 sub-piece:  {prev_hot.end->key (or origin.kr.begin), origin.kr.end}
//     sub-piece が origin の末尾に到達する場合は、KeyRange.end は
//     origin の KeyRange.end をそのまま継承する (= 最適化版の
//     `cold_key_ranges[idx_in_ary].end` の挙動に一致)。
inline void rebuild_cold_list_with_origins_naive(
    std::list<ChunkedPairsRange>& cold_list,
    std::vector<KeyRange>& piece_key_ranges,
    const std::vector<IncrementalOriginPiece>& origins,
    const std::vector<PairsRange>& all_hots_sorted)
{
    cold_list.clear();
    piece_key_ranges.clear();

    for (const IncrementalOriginPiece& o : origins) {
        const KVPair* const ob = o.range.begin();
        const KVPair* const oe = o.range.end();
        const KVPair* cursor = ob;
        key_uint64_t cur_kr_begin = o.key_range.begin;

        const auto push_piece = [&](const KVPair* pb, const KVPair* pe,
                                    const KeyRange& kr) {
            if (pb >= pe) return;
            const size_t off
                = static_cast<size_t>(pb - ob) / KVPairsChunkSize;
            cold_list.emplace_back(PairsRange{pb, pe}, o.load_base + off);
            piece_key_ranges.push_back(kr);
        };

        for (const PairsRange& hot : all_hots_sorted) {
            if (!(ob <= hot.begin() && hot.end() <= oe)) continue;

            push_piece(cursor, hot.begin(),
                {cur_kr_begin, hot.begin()->key - 1});
            cursor = hot.end();
            if (cursor < oe) {
                cur_kr_begin = cursor->key;
            }
        }
        push_piece(cursor, oe, {cur_kr_begin, o.key_range.end});
    }
}


// ─────────────────────────────────────────────────────────────
// Main: incremental_repartition の愚直版 v1 (whole-DPU retrieval)
// ─────────────────────────────────────────────────────────────
template <typename Query, typename Result>
inline void BPForest::incremental_repartition_naive(
    uint32_t nr_queries, const Query queries[], Result results[],
    QueryData<Query, Result>& routed)
{
    // ── 1. Phase 1: 対象 DPU の選別 ─────────────────────
    //
    // 最適化版 `incremental_repartition` の Phase 1 (ScopedTimer{"retrieve"}
    // 冒頭のループ) に対応。incision_count / hot_delim_keys[] 等の
    // 書き出しは v1 では全廃し (愚直化 (f))、判定だけを残す。
    //
    // 判定は 2 層 (README §3.3.1):
    //   trigger  … `overload_threshold.threshold_for(...)` 超え。1 つでも
    //               立てば「このバッチは rebalance する」。
    //   do_cold  … goal そのもの超え。作り直す DPU 集合を決める。
    // `threshold_for` は必ず goal 以上を返すので、対象集合 (touch) は
    // trigger 集合を含む。
    // 「やるかどうか」は保守的に、「やるなら誰を」は緩めに、という非対称。
    //
    // `param.balancing` は CLI で `>= 1` が強制されるので 0 分岐は無い。
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
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        trigger = trigger || routed.cold[idx_dpu].nr_qrys > cold_partial_cnt_threshold;
        if (routed.cold[idx_dpu].nr_qrys > cold_partial_cnt_goal) {
            touch[idx_dpu] = true;
            nr_touched++;
        }
    }
    if (!param.enable_incremental) {
        // 最適化版は trigger が立った時点で `Balanced::No` を返し、
        // `repartition()` ラッパが full を呼ぶ。
        if (trigger) {
            return full_repartition_naive(nr_queries, queries, results, routed);
        }
        return;
    }
    if (!trigger) return;  // 最適化版の `return Balanced::Yes;`
    // trigger ⇒ その DPU は do_cold でもあるので通常ここは通らない。
    if (nr_touched == 0) return;

    // ── 2. Phase 2: 対象 DPU から pair を丸ごと取得 (v1 独自, 愚直化 (f)) ──
    std::vector<std::vector<KVPair>> retrieved(nr_base_parts);
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        if (!touch[idx_dpu]) continue;
        retrieved[idx_dpu] = retrieve_dpu_all_pairs_naive(*this, idx_dpu);
    }

    // ── 3. 閾値定数 ────────────────────────────────────
    //
    // 最適化版 `incremental_repartition_worker_cold` 冒頭の hot_load /
    // cold_endpoint_cnt_goal と同式。ただし最適化版はこれに
    // `param.more_hotness` 係数が乗り、`param.greedy_only` が真のとき
    // goal が `× param.balancing` の式に切り替わる。下の式が数値一致
    // するのはデフォルト値のときだけ (README §0.1, §1.3)。
    //
    // (`nr_existing_hots` は Phase 1 で算出済み。delims はここまで
    //  一切変更していないので同じ値。)
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
            // 触らない DPU: 現在の cold load (nr_qrys * W) を記録するだけ
            cold_loads_local[idx_dpu]
                = {idx_dpu, routed.cold[idx_dpu].nr_qrys * W};
            continue;
        }

        // ── 4a. 取得 pair から origin cold 片を構築 ──────
        //
        // retrieved[idx_dpu] の前半 `nr_pairs[idx_dpu][0]` 個が cold。
        // 後半は DPU が保持する「他 base の hot」で incremental の対象外。
        //
        // 既存 hot delim の key 境界で cold KV 配列を分割する。各片に
        // 対応する KeyRange もここで決定する (最適化版 Phase 1 の
        // `cold_key_ranges[]` / `hot_delim_keys[]` 構築に相当)。
        //
        const uint32_t nr_cold = nr_pairs[idx_dpu].get()[0];
        const KVPair* const cold_begin = retrieved[idx_dpu].data();
        const KVPair* const cold_end = cold_begin + nr_cold;

        auto& origins_here = origins[idx_dpu];
        {
            const key_uint64_t base_min = cold_delims[idx_dpu]->first;
            const key_uint64_t base_max
                = (std::next(cold_delims[idx_dpu]) == delims.cend()
                          || cold_delims[idx_dpu + 1] == delims.cend())
                      ? KEY_MAX
                      : cold_delims[idx_dpu + 1]->first - 1;

            const KVPair* cursor = cold_begin;
            key_uint64_t cur_kr_begin = base_min;

            for (auto it = std::next(cold_delims[idx_dpu]);
                 it != cold_delims[idx_dpu + 1]; ++it) {
                const auto& hd = std::get<HotPartitionDelim>(it->second);
                const key_uint64_t hot_lo = it->first;
                const key_uint64_t hot_hi = hd.max_key;

                const KVPair* split = cursor;
                while (split != cold_end && split->key < hot_lo) ++split;
                if (cursor < split) {
                    origins_here.push_back(IncrementalOriginPiece{
                        PairsRange{cursor, split},
                        {cur_kr_begin, hot_lo - 1},
                        nullptr});
                }
                cursor = split;
                cur_kr_begin = hot_hi + 1;
            }
            if (cursor < cold_end) {
                origins_here.push_back(IncrementalOriginPiece{
                    PairsRange{cursor, cold_end},
                    {cur_kr_begin, base_max},
                    nullptr});
            }
        }

        if (origins_here.empty()) {
            // 対象なのに cold 片が 1 つもない = 全 base が既存 hot に
            // 取られている。何もしない (touch 解除)。
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
        // thread_local `hot_delim_keys`) への upper_bound で
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
        // というケース。これは「途中で切り上げる最適化」ではなく
        // アルゴリズムの不可欠な分岐なので残す。
        //
        if (cold_endpoint_cnt <= cold_endpoint_cnt_goal) {
            cold_npairs_after[idx_dpu] = 0;  // 据え置き (touch off 扱い)
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
        // find_absolutely_hot_ranges は複数 cold 片を一括で処理する
        // (callback が false を返すと全 part を横断して return する)
        // ため、naive でも list bridge 経由で一括呼び出しする。
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
        //
        //   最適化版 `incremental_repartition_worker_cold` の
        //   find_relatively_hot_ranges(list.begin(), list.end(), ...) に対応。
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
        //    `repartition()` ラッパ)。
        //
        // 既存 hot + 今回 carve した hot の合計が nr_base_parts を
        // 超えたら incremental では配置できないので full にフォールバック。
        //
        if (nr_existing_hots + static_cast<dpu_id_t>(new_hots_local.size())
            > nr_base_parts) {
            return full_repartition_naive(nr_queries, queries, results, routed);
        }
    }

    const dpu_id_t hot_count = static_cast<dpu_id_t>(new_hots_local.size());
    if (hot_count == 0) return;

    // ── 5. Phase 4: hot を低負荷 DPU に割り当て ─────────
    //   最適化版 `incremental_repartition` の partial_sort / sort +
    //   delims.emplace ループに対応。
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

    // ── 6. Phase 5: touched DPU を更新 ──────────────────
    //
    // 最適化版は TASK_MOVE_HOT で cold / hot を部分更新するが、
    // 愚直版 v1 は「対象 DPU を丸ごとリセット (TASK_INIT 相当)」に
    // 単純化する。`renew_cold` / `renew_hot` のフラグ分岐は消える。
    //
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        if (touch[idx_dpu]) {
            nr_pairs[idx_dpu].get()[0] = cold_npairs_after[idx_dpu];
        }
    }
    combine_delims();
    initialize_touched_dpus_naive(touch, cold_lists, hot_ranges_local);

    route_queries(nr_queries, queries, results, routed);
}


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
//    (hot_delim_keys 書き出し省略)     |    冒頭のループ
//  2 retrieve_dpu_all_pairs_naive      | I: 同ブロックの TASK_SERIALIZE
//                                      |    gather/exec/scatter + "alloc" +
//                                      |    "recv" を (f) で置換
//  3 閾値定数                          | C: 冒頭の hot_load /
//                                      |    cold_endpoint_cnt_goal
//                                      |    (more_hotness / greedy_only 分岐
//                                      |     は未反映)
//  4a origin 構築 (cold 片 + KeyRange) | I: Phase 1 の cold_key_ranges[] /
//                                      |    "alloc" の incision スライスを
//                                      |    host 側で再現
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
//  —  (未対応)                         | I: hot pass ("hot" ブロック,
//                                      |    ..._worker_hot) と その直後の
//                                      |    nr_new_pieces 込み bailout,
//                                      |    hot_split_plans の commit
//  5  hot → DPU マッチング             | I: partial_sort / sort +
//                                      |    delims.emplace ループ
//  6  DPU 更新 & re-route              | I: TASK_MOVE_HOT 設定 +
//                                      |    UpdatedPartitionsSender +
//                                      |    末尾の route_queries
//
// 意味論: hot 集合 / delims / hot_delims / nr_pairs / DPU 側 B+ tree
//         いずれも最適化版と一致する (ソート tie-break の違いを除けば)。
//         `cold_endpoint_cnt` の scalar 追跡も最適化版と同じ値を取る。
//         ただし成立するのは冒頭 / README §0.1 のスコープ内でのみ。
//
// v2 (incremental_repartition_serialize_naive.hpp) との違い:
//   - v1: 対象 DPU から cold+hot を丸ごと取得し、host 側で既存 hot delim
//         の key 境界で cold 区間列を復元する。incision_indices は使わない。
//   - v2: 最適化版と同じ TASK_SERIALIZE プロトコルで cold 片だけを
//         受け取る (host 側 key 境界分割は不要)。ただし host 側ロジック
//         は引き続き愚直化する。
