/// @file full_repartition_naive.hpp
/// @brief `BPForest::full_repartition` + `BPForest::full_repartition_worker`
///        (host/inc/bpforest.ipp) を、同一シグネチャ・同一意味論のまま
///        最も愚直に書き直した参照実装。
///        参照用であり、ビルド対象ではない。
///
/// スコープ (詳細は docs/repartitioning/README.md §0.1):
///   デフォルトパラメータ (`more_hotness == 1`, `greedy_only == false`)
///   かつ predecessor 以外のクエリでのみ「最適化版と同じ最終状態」の契約が
///   成立する。最適化版の `param.more_hotness` 係数 / `param.greedy_only`
///   分岐 / predecessor 用 `std::lower_bound` / 並列 worker 化
///   (`TmpDataForFullRepartition` + mutex + `parallel_run`) は未反映。
///
/// 剥がした最適化 (= 愚直化の内容):
///   (a) クエリの chunk 位置特定を `std::upper_bound` ではなく
///       chunk 列の線形走査で行う (最適化版 `full_repartition_worker` の
///       chunk load populate ブロック)。
///   (b) 欠番。以前は「hot_hook 内の `cold_endpoint_cnt > goal` 判定
///       を廃止する」と書いていたが、これは誤り — この判定は
///       Algorithm 2/3 のロジックそのものであり、`return true` を
///       返し続けると carve される hot 数・位置・最終パーティションが
///       変わってしまう。愚直版でも段間 (abs / rel 各段の直前) と
///       hot_hook 内 (abs / rel 各 hot_hook の末尾) の両方の
///       `cold_endpoint_cnt > cold_endpoint_cnt_goal` 判定を維持する。
///   (c) cold 範囲列を `LinkedList<ChunkedPairsRange>` ではなく
///       `std::list<ChunkedPairsRange>` で持つ。
///       find_* の呼び出し時だけ、その場限りのスコープで LinkedList に
///       橋渡しする (下の bridge helper 参照)。これにより
///       `chunked_cold_ranges` プール (dpu_id_t 決め打ちインデックス
///       `idx_in_ary`) の管理や、intrusive link を意識したノード確保が
///       呼び出し側から消える。
///   (d) carve のたびに `*left_range = ...` / splice で部分更新する
///       (abs 段 hot_hook 内の `list.insert` と、rel 段直後の
///        `carved_cold_range` を使った左右境界の splice) のではなく、
///       `new_hots_local` に記録された hot 区間列から cold list を
///       一から build-from-scratch で組み立て直す。
///   (e) BPForest 側の「一時データ用」メンバ (`data_buf`,
///        `chunked_cold_ranges[,_lists]`, `new_hots`, `cold_loads`,
///        `input_headers`, `hot_ranges`, および per-thread scratch の
///        `chunk2load` = `inline static thread_local`) を使わず、
///       ローカル変数で完結させる。
///       `parts` / `nr_pairs` は呼び出し間で持ち越す「状態」なので
///       メンバのまま更新する。
///   (g) `get_next_idx_base` の pre-filter skip (point query で
///       `routed.cold[b].nr_qrys <= goal` の base を load 推定ごと飛ばし、
///       `nr_pairs` / `cold_loads` だけ埋める) を持たず、全 base を
///       素通しで回す。skip された base は carve が起きないので最終状態は
///       同じ。
///
/// 保っている意味論 (= 最適化版と同じもの):
///   - `hot_load`, `cold_endpoint_cnt_goal`, `hot_npairs` の計算式
///   - find_absolutely → find_relatively の 2 段構えの適用順と入力
///   - `cold_endpoint_cnt` の scalar 追跡 (carve 時に `load` を引き算)
///   - 選出した hot を load 降順で低負荷 DPU に割り当てる matching
///   - 最終的な `parts` / `nr_pairs` の内容
///   - `initialize_in_dpu` 呼び出しで再構築される DPU 側の状態
///   - 末尾の `route_queries` 再実行

#pragma once

#include "bpforest.hpp"
#include "linked_list.hpp"
#include "pairs_range.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <list>
#include <utility>
#include <vector>


// ─────────────────────────────────────────────────────────────
// helper (a): key を含む chunk を線形走査で特定
// ─────────────────────────────────────────────────────────────
//
// 最適化版 (`full_repartition_worker` の chunk load populate ブロック):
//     upper_bound(left, right, key, [](k, ch){ return k < ch.begin()->key; });
//     --one_after_target_chunk;
//
// 注: 最適化版は predecessor クエリ (`IsPredecessorQuery`) のときだけ
//     `std::lower_bound` に切り替える。この愚直版は upper_bound 系の
//     意味論しか実装していない (README §0.1)。
//
// part 内で `chunk.begin()->key <= key` を満たす最後の chunk を返す。
inline DataChunkIterator find_chunk_containing_key_naive(
    const ChunkedPairsRange& part, key_uint64_t key)
{
    DataChunkIterator target = part.begin();
    for (DataChunkIterator it = part.begin(); it != part.end(); ++it) {
        if (it->begin()->key <= key) {
            target = it;
        } else {
            break;
        }
    }
    return target;
}


// ─────────────────────────────────────────────────────────────
// helper: base partition 単位で chunk load を集計する (愚直版)
// ─────────────────────────────────────────────────────────────
//
// 最適化版 `full_repartition_worker` の chunk load populate ブロックと
// 同等。upper_bound を find_chunk_containing_key_naive に差し替えただけ。
// 呼び出し時点で `base` は単一の ChunkedPairsRange (= base partition
// 全体) であり、その `set_load_ary` 済みの chunk 負荷配列をインクリ
// メントする。
//
// 戻り値: この base partition の cold 側にある query endpoint の総数
//         (= cold_endpoint_cnt の初期値)。
template <typename Query, typename Result>
inline uint32_t estimate_chunk_load_naive(
    const Query queries[],
    const QueryData<Query, Result>& routed,
    dpu_id_t idx_base, dpu_id_t nr_base_parts,
    const ChunkedPairsRange& base)
{
    if constexpr (IsPointQuery<Query>) {
        for (const auto& qry_vec : routed.cold[idx_base].qrys) {
            for (const auto& qry : qry_vec) {
                const key_uint64_t key = PointQueryToKey<Query>{}(qry);
                find_chunk_containing_key_naive(base, key)->load()++;
            }
        }
        return routed.cold[idx_base].nr_qrys;
    } else {
        uint32_t cnt = 0;
        const key_uint64_t base_min = base.PairsRange::begin()->key;
        const key_uint64_t base_max
            = (idx_base + 1 == nr_base_parts)
                ? KEY_MAX
                : base.PairsRange::end()->key;
        for (const auto& idx_vec : routed.cold[idx_base].orig_idxs) {
            for (const auto orig_idx : idx_vec) {
                const KeyRange& range = RangeQueryToRange<Query>{}(queries[orig_idx]);
                for (const auto key : {range.begin, range.end}) {
                    if (base_min <= key && key <= base_max) {
                        find_chunk_containing_key_naive(base, key)->load()++;
                        cnt++;
                    }
                }
            }
        }
        return cnt;
    }
}


// ─────────────────────────────────────────────────────────────
// bridge: 単一 ChunkedPairsRange → find_absolutely_hot_ranges
// ─────────────────────────────────────────────────────────────
//
// 最適化版は
//     find_absolutely_hot_ranges(&base, &base + 1, ...);
// と単一要素の `LinkedChunkedPairsRange*` 範囲で呼ぶ。愚直版でも
// これに倣い、スタック上に 1 要素の temporary `LinkedChunkedPairsRange`
// を置いて渡す。
//
// hot_hook 内での `*part` 書き換え (最適化版 `full_repartition_worker`
// の abs 段 hot_hook にある `part = ChunkedPairsRange{end, part.end()}`) は
// find_absolutely_hot_ranges の内部 loop の進行に影響しない
// (loop は冒頭で捕捉した `end_chunk` と `right` だけを見る)。
// 従って愚直版の hot_hook は tmp を書き換えず、hot 区間と load を
// 外側にコピーするだけでよい。
template <typename HotHook>
inline void find_absolutely_hot_ranges_on_range_naive(
    const ChunkedPairsRange& range,
    uint32_t hot_npairs, uint32_t hot_nqrys,
    HotHook&& hot_hook)
{
    LinkedChunkedPairsRange tmp;
    tmp = range;  // LinkedElement::operator=(const T&) で T 部分だけコピー
    find_absolutely_hot_ranges(&tmp, &tmp + 1, hot_npairs, hot_nqrys,
        std::forward<HotHook>(hot_hook));
}


// ─────────────────────────────────────────────────────────────
// bridge: std::list<ChunkedPairsRange> → find_absolutely_hot_ranges
//         (複数 cold 片を一括で処理)
// ─────────────────────────────────────────────────────────────
//
// 最適化版 `incremental_repartition_worker_cold` の
//     find_absolutely_hot_ranges(begin_part, end_part, ...);
// に対応する、複数 cold 片を 1 回の呼び出しでまとめて処理する bridge。
// full_repartition では cold は 1 片なので使わないが、
// incremental_repartition では cold が複数片あり、かつ最適化版と意味論
// を揃えるには複数片を 1 回で渡す必要がある (find_absolutely_hot_ranges
// は callback が false を返すと全 part を横断して return する
// = 片ごとに分けて呼ぶと余計な carve が発生する)。
//
// hot_hook シグネチャ:
//   bool(size_t piece_idx, ChunkedPairsRange& part,
//        DataChunkIterator chunk_begin, DataChunkIterator chunk_end,
//        uint32_t load)
// piece_idx は呼び出し時の cold_list 内 0-based 位置。
//
// アドレス安定性: `std::vector<LinkedChunkedPairsRange> pool(n)` は
// n 要素を一括構築する (再確保しない) ので、pool.data() の個々の要素
// アドレスは関数スコープ内で安定。
template <typename HotHook>
inline void find_absolutely_hot_ranges_on_list_naive(
    const std::list<ChunkedPairsRange>& cold_list,
    uint32_t hot_npairs, uint32_t hot_nqrys,
    HotHook&& hot_hook)
{
    if (cold_list.empty()) return;
    std::vector<LinkedChunkedPairsRange> pool(cold_list.size());
    size_t i = 0;
    for (const ChunkedPairsRange& r : cold_list) {
        pool[i] = r;
        ++i;
    }
    find_absolutely_hot_ranges(
        pool.data(), pool.data() + pool.size(), hot_npairs, hot_nqrys,
        [&pool, &hot_hook](ChunkedPairsRange& part,
            DataChunkIterator chunk_begin, DataChunkIterator chunk_end,
            uint32_t load) {
            const size_t piece_idx = static_cast<size_t>(
                static_cast<LinkedChunkedPairsRange*>(&part) - pool.data());
            return hot_hook(piece_idx, part, chunk_begin, chunk_end, load);
        });
}


// ─────────────────────────────────────────────────────────────
// bridge: std::list<ChunkedPairsRange> → find_relatively_hot_ranges
// ─────────────────────────────────────────────────────────────
//
// 最適化版は
//     find_relatively_hot_ranges(list.begin(), list.end(), ...);
// と `LinkedList<ChunkedPairsRange>::iterator` 範囲で呼ぶ。愚直版は
// 普段の cold list を std::list で持つため、呼び出し直前にその場で
// LinkedChunkedPairsRange プールを確保して `LinkedList` を組み立て直す。
//
// hot_hook シグネチャ:
//   bool(size_t piece_idx, const ChunkedPairsRange& part,
//        const PairsRange& range, uint32_t load)
// piece_idx は呼び出し時の cold_list 内 0-based 位置。
// full_repartition 側は piece_idx を使わない (単一 cold 片なので常に 0)
// が、incremental_repartition 側は hot.key_range.end を per-piece の
// `cold_key_ranges` から取るために必要。
//
// アドレス安定性: `std::vector<LinkedChunkedPairsRange> pool(n)` は
// n 要素を一括直接構築する (再確保しない) ので、各要素のアドレスは
// 関数スコープ内で安定であり intrusive な next/prev ポインタが
// 壊れない。
//
// 戻り値は discard する: 最適化版は carve 済 cold 境界の iterator 組
// (`carved_cold_range`) を返し、それを元に部分 splice で cold list を
// 更新する。愚直版は hot_hook で記録された hot 区間列
// から cold list を一から再構築するので、境界 iterator は不要。
template <typename HotHook>
inline void find_relatively_hot_ranges_on_list_naive(
    const std::list<ChunkedPairsRange>& cold_list,
    uint32_t hot_npairs, dpu_id_t nr_hots,
    HotHook&& hot_hook)
{
    if (cold_list.empty() || nr_hots == 0) return;

    std::vector<LinkedChunkedPairsRange> pool(cold_list.size());
    LinkedList<ChunkedPairsRange> llist;
    size_t i = 0;
    for (const ChunkedPairsRange& r : cold_list) {
        pool[i] = r;
        llist.push_back(pool[i]);
        ++i;
    }
    (void)find_relatively_hot_ranges(
        llist.begin(), llist.end(), hot_npairs, nr_hots,
        [&pool, &hot_hook](const ChunkedPairsRange& part,
            const PairsRange& range, uint32_t load) {
            const size_t piece_idx = static_cast<size_t>(
                static_cast<const LinkedChunkedPairsRange*>(&part) - pool.data());
            return hot_hook(piece_idx, part, range, load);
        });
}


// ─────────────────────────────────────────────────────────────
// helper: base partition の cold list を hot 区間列から build-from-
//         scratch で組み立て直す
// ─────────────────────────────────────────────────────────────
//
// 前提:
//   - `sorted_hots` は start ポインタ昇順で、全て [base_begin, base_end)
//     に収まっている。
//   - 各 hot は chunk 境界に alignment されている
//     (hot.begin() は base_begin からの chunk 境界、hot.end() も
//      chunk 境界 or base_end)。find_absolutely/find_relatively は
//      `DataChunkIterator` から hot 区間を作るので、この alignment は
//      自動的に守られる。
//
// 各 cold piece の `p_load` は base ごとに 1 本の chunk load 配列
// (`base_load_array`) のスライスを指すように設定する。これにより
// 最初の estimate_chunk_load_naive で populate した load 値を carve
// 後の piece からそのまま読み出せる。
inline void rebuild_cold_list_from_hots_naive(
    std::list<ChunkedPairsRange>& cold_list,
    uint32_t* base_load_array,
    const KVPair* base_begin, const KVPair* base_end,
    const std::vector<PairsRange>& sorted_hots)
{
    cold_list.clear();

    const auto push_piece = [&](const KVPair* pb, const KVPair* pe) {
        if (pb >= pe) return;
        ChunkedPairsRange piece{PairsRange{pb, pe}};
        const size_t chunk_offset
            = static_cast<size_t>(pb - base_begin) / KVPairsChunkSize;
        piece.set_load_ary(base_load_array + chunk_offset);
        cold_list.push_back(piece);
    };

    const KVPair* cursor = base_begin;
    for (const PairsRange& hot : sorted_hots) {
        push_piece(cursor, hot.begin());
        cursor = hot.end();
    }
    push_piece(cursor, base_end);
}


// ─────────────────────────────────────────────────────────────
// helper: パーティション表を作り直す (愚直版)
// ─────────────────────────────────────────────────────────────
//
// 最適化版 `BPForest::rebuild_parts` に対応。base ごとに cold 片の先頭キーと
// その base から切り出された hot の始端キーを集め、鍵順に並べて `parts` を
// 積み直す。最適化版は 2 列を線形マージするが、愚直版は集めてソートする。
//
// `renewed[b]` が偽の base は cold 片を作り直していないので、いま `parts` に
// あるエントリを使う。据え置きの hot も `parts` から拾うので、呼び出し側が
// 渡すのは新設の hot だけでよい。
inline void BPForest::rebuild_parts_naive(
    const std::vector<std::list<ChunkedPairsRange>>& cold_lists,
    const std::vector<HotEntry>& new_hots,
    const std::vector<bool>& renewed)
{
    std::vector<HotEntry> hots = new_hots;
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        if (hot_part[idx_dpu] != INVALID_DPU_ID) {
            hots.push_back({parts.begins[hot_part[idx_dpu]], idx_dpu, parts.origins[hot_part[idx_dpu]]});
        }
    }

    PartitionTable rebuilt{max_nr_parts(nr_base_parts)};
    for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
        std::vector<std::pair<key_uint64_t, QueryDest>> entries;
        if (renewed[idx_base]) {
            for (const ChunkedPairsRange& piece : cold_lists[idx_base]) {
                if (piece.npairs() > 0) {
                    entries.emplace_back(piece.PairsRange::begin()->key, QueryDest{idx_base, false});
                }
            }
        } else {
            for (dpu_id_t i = base_part[idx_base]; i < base_part[idx_base + 1]; i++) {
                if (!parts.dests[i].is_hot) {
                    entries.emplace_back(parts.begins[i], parts.dests[i]);
                }
            }
        }
        for (const HotEntry& hot : hots) {
            if (hot.origin == idx_base) {
                entries.emplace_back(hot.begin, QueryDest{hot.host, true});
            }
        }

        std::sort(entries.begin(), entries.end(),
            [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
        for (const auto& [begin, dest] : entries) {
            rebuilt.assign_from(begin, dest, idx_base);
        }
    }

    parts.swap(rebuilt);
    rebuild_part_indices();
}


// ─────────────────────────────────────────────────────────────
// Main: full_repartition の愚直版
// ─────────────────────────────────────────────────────────────
template <typename Query, typename Result>
inline void BPForest::full_repartition_naive(
    const uint32_t nr_queries, const Query queries[],
    Result* results, QueryData<Query, Result>& routed)
{
    // ── 1. 全 DPU から KV ペアを回収 ─────────────────────
    std::vector<KVPair> local_data_buf;
    const size_t nr_total_pairs = retrieve_all_data_into(local_data_buf);
    const KVPair* const data_end = local_data_buf.data() + nr_total_pairs;

    // ── 2. パーティション表を全クリア ───────────────────
    parts.clear();

    // ── 3. 各 base partition の cold list と chunk load 配列を
    //       ローカルに確保 (愚直化 (c), (e)) ──
    //
    // - cold_lists[idx_base] は `std::list<ChunkedPairsRange>`。
    //   初期状態は「base 全体を覆う 1 要素」。
    // - chunk_loads[idx_base] は base ごとに 1 本 (全長
    //   = nchunks_of_full_base)。最適化版の chunk2load と同じ配置で、
    //   carve 後の cold piece はこの配列の chunk offset スライスを指す。
    // - base_begins/ends は後段の rebuild で必要になる KVPair*境界。
    std::vector<std::list<ChunkedPairsRange>> cold_lists(nr_base_parts);
    std::vector<std::vector<uint32_t>> chunk_loads(nr_base_parts);
    std::vector<const KVPair*> base_begins(nr_base_parts), base_ends(nr_base_parts);

    for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
        const KVPair* const pb
            = local_data_buf.data() + nr_total_pairs * idx_base / nr_base_parts;
        const KVPair* const pe
            = local_data_buf.data() + nr_total_pairs * (idx_base + 1) / nr_base_parts;
        base_begins[idx_base] = pb;
        base_ends[idx_base] = pe;

        ChunkedPairsRange piece{PairsRange{pb, pe}};
        chunk_loads[idx_base].assign(piece.nchunks(), 0u);
        piece.set_load_ary(chunk_loads[idx_base].data());
        cold_lists[idx_base].push_back(piece);

        if (pb < pe) {
            parts.assign_from(pb->key, QueryDest{idx_base, false}, idx_base);
        }
    }

    // ── 4. 初回ルーティング ─────────────────────────────
    rebuild_part_indices();
    route_queries(nr_queries, queries, results, routed);

    std::vector<std::array<uint32_t, 2>> nr_pairs_local(nr_base_parts, {0, 0});
    std::vector<PairsRange> hot_ranges_local(nr_base_parts, PairsRange{nullptr, nullptr});

    // ── 5. 閾値定数 ────────────────────────────────────
    //
    // 最適化版 `full_repartition_worker` 冒頭の hot_load /
    // cold_endpoint_cnt_goal と同式。ただし最適化版はこれに
    // `param.more_hotness` 係数が乗り、`param.greedy_only` が真のとき
    // goal が `× param.balancing` の式に切り替わる。下の式が数値一致
    // するのはデフォルト (more_hotness == 1, greedy_only == false) のとき
    // だけ (README §0.1, §1.3)。
    //
    // `param.balancing` は CLI で `>= 1` が強制されるので 0 分岐は無い。
    constexpr uint32_t W = IsPointQuery<Query> ? 1 : 2;
    const uint32_t hot_load
        = (nr_queries * W + nr_base_parts - 1) / nr_base_parts;
    const uint32_t cold_endpoint_cnt_goal
        = nr_queries * W * std::max(3u, param.balancing + 1) / 3 / nr_base_parts;

    std::vector<NewHotRange> new_hots_local;
    new_hots_local.reserve(nr_base_parts);
    std::vector<std::pair<dpu_id_t, uint32_t>> cold_loads_local(nr_base_parts);
    std::vector<uint32_t> cold_npairs_after(nr_base_parts);

    // ── 6. base ごとに hot を切り出す ────────────────────
    for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
        const KVPair* const pb = base_begins[idx_base];
        const KVPair* const pe = base_ends[idx_base];
        auto& cold_list = cold_lists[idx_base];
        uint32_t* const load_ary = chunk_loads[idx_base].data();

        uint32_t cold_npairs = static_cast<uint32_t>(pe - pb);
        const uint32_t hot_npairs
            = (cold_npairs + param.balancing - 1) / param.balancing;

        // 6a. chunk load を populate し、初期 cold_endpoint_cnt を得る。
        //     この時点で cold_list は「base 全体を覆う 1 要素」なので、
        //     front() への populate がそのまま base 全体への populate。
        uint32_t cold_endpoint_cnt = estimate_chunk_load_naive(
            queries, routed, idx_base, nr_base_parts, cold_list.front());

        // 6b. Absolute hot 段 — 段間ゲート (abs 段の直前) と
        //     hot_hook 内ゲート (hot_hook 末尾の return) の両方を保持する。
        const size_t abs_start = new_hots_local.size();
        if (cold_endpoint_cnt > cold_endpoint_cnt_goal) {
            find_absolutely_hot_ranges_on_range_naive(
                cold_list.front(), hot_npairs, hot_load,
                [&](ChunkedPairsRange&, DataChunkIterator chunk_begin,
                    DataChunkIterator chunk_end, uint32_t load) {
                    NewHotRange hot;
                    hot.pairs_range = {chunk_begin.begin(), chunk_end.begin()};
                    hot.key_range = {hot.pairs_range.begin()->key,
                        (hot.pairs_range.end() == data_end
                                ? KEY_MAX
                                : hot.pairs_range.end()->key - 1)};
                    hot.load = load;
                    new_hots_local.push_back(hot);
                    cold_npairs -= static_cast<uint32_t>(hot.pairs_range.npairs());
                    cold_endpoint_cnt -= load;
                    return cold_endpoint_cnt > cold_endpoint_cnt_goal;
                });
        }

        // 6c. cold list を rebuild (愚直化 (d))。
        //   - この base で carve 済みの hot 区間を集めて昇順にソート
        //   - [pb, pe) から hot 区間を引き算して cold piece を emit
        //   - 各 piece は base 単位の load 配列の chunk offset を指す
        {
            std::vector<PairsRange> hots_in_base;
            hots_in_base.reserve(new_hots_local.size() - abs_start);
            for (size_t i = abs_start; i < new_hots_local.size(); i++) {
                hots_in_base.push_back(new_hots_local[i].pairs_range);
            }
            std::sort(hots_in_base.begin(), hots_in_base.end(),
                [](const PairsRange& a, const PairsRange& b) {
                    return a.begin() < b.begin();
                });
            rebuild_cold_list_from_hots_naive(
                cold_list, load_ary, pb, pe, hots_in_base);
        }

        // 6d. Relative hot 段 (cold_endpoint_cnt が goal を超えるときだけ)。
        //     最適化版はこの外側ゲートに `!param.greedy_only &&` が前置
        //     される (greedy_only 時は rel 段が丸ごと消える)。
        if (cold_endpoint_cnt > cold_endpoint_cnt_goal && !cold_list.empty()) {
            const uint32_t nr_relative_hots = cold_endpoint_cnt / hot_load;

            find_relatively_hot_ranges_on_list_naive(
                cold_list, hot_npairs, nr_relative_hots,
                [&](size_t /*piece_idx*/, const ChunkedPairsRange&,
                    const PairsRange& range, uint32_t load) {
                    NewHotRange hot;
                    hot.pairs_range = range;
                    hot.key_range = {range.begin()->key,
                        (range.end() == data_end
                                ? KEY_MAX
                                : range.end()->key - 1)};
                    hot.load = load;
                    new_hots_local.push_back(hot);
                    cold_npairs -= static_cast<uint32_t>(hot.pairs_range.npairs());
                    cold_endpoint_cnt -= load;
                    return cold_endpoint_cnt > cold_endpoint_cnt_goal;
                });

            // 6e. 全 hot (absolute + relative) から cold list を再 rebuild。
            std::vector<PairsRange> hots_in_base;
            hots_in_base.reserve(new_hots_local.size() - abs_start);
            for (size_t i = abs_start; i < new_hots_local.size(); i++) {
                hots_in_base.push_back(new_hots_local[i].pairs_range);
            }
            std::sort(hots_in_base.begin(), hots_in_base.end(),
                [](const PairsRange& a, const PairsRange& b) {
                    return a.begin() < b.begin();
                });
            rebuild_cold_list_from_hots_naive(
                cold_list, load_ary, pb, pe, hots_in_base);
        }

        cold_npairs_after[idx_base] = cold_npairs;
        cold_loads_local[idx_base] = {idx_base, cold_endpoint_cnt};
    }

    // ── 7. hot を低負荷 DPU に割り当て ──────────────────
    //   最適化版 `full_repartition` の partial_sort / sort +
    //   hot_entries 収集ループに対応。
    for (dpu_id_t idx = 0; idx < nr_base_parts; idx++) {
        nr_pairs_local[idx] = {cold_npairs_after[idx], 0};
    }

    const dpu_id_t hot_count = static_cast<dpu_id_t>(new_hots_local.size());
    if (hot_count > 0) {
        std::partial_sort(cold_loads_local.begin(),
            cold_loads_local.begin() + hot_count, cold_loads_local.end(),
            [](auto& l, auto& r) { return l.second < r.second; });
        std::sort(new_hots_local.begin(), new_hots_local.end(),
            [](auto& l, auto& r) { return l.load > r.load; });

        std::vector<HotEntry> hot_entries_local;
        for (dpu_id_t idx_hot = 0; idx_hot < hot_count; idx_hot++) {
            const dpu_id_t idx_dpu = cold_loads_local[idx_hot].first;
            const NewHotRange& nh = new_hots_local[idx_hot];

            hot_ranges_local[idx_dpu] = nh.pairs_range;
            nr_pairs_local[idx_dpu][1] = static_cast<uint32_t>(nh.pairs_range.npairs());
            hot_entries_local.push_back({nh.key_range.begin, idx_dpu, nh.origin});
        }

        rebuild_parts_naive(cold_lists, hot_entries_local, std::vector<bool>(nr_base_parts, true));
        route_queries(nr_queries, queries, results, routed);
    }

    // ── 8. DPU 側の B+ tree を再構築 ─────────────────────
    initialize_in_dpu_naive(nr_pairs_local, cold_lists, hot_ranges_local);
}


// ─────────────────────────────────────────────────────────────
// 参考: 最適化版との対応表 (関数 + ブロック単位)
// ─────────────────────────────────────────────────────────────
// 最適化版は worker 分割されており行単位の 1:1 対応は付かない。
// 対応は関数名 + ブロック名で見ること (README §6)。
//
//  愚直版ステップ                         | 最適化版
// ────────────────────────────────────────┼──────────────────────────
//  1 retrieve_all_data                    | full_repartition:
//                                         |   retrieve_all_data(data_buf)
//  2 パーティション表クリア               | 同上 直後の parts.clear()
//  3 cold_lists / chunk_loads 初期化      | 同上 chunked_cold_ranges[] と
//                                         | chunked_cold_ranges_lists[] を
//                                         | 埋める base ループ
//                                         | (プール / chunk2load をローカルに)
//  4 初回 route_queries                   | 同上 rebuild_part_indices +
//                                         |   route_queries
//  5 閾値定数                             | full_repartition_worker 冒頭
//                                         | (more_hotness / greedy_only 分岐
//                                         |  は未反映)
//  6a chunk load populate                 | 同 worker の chunk2load ブロック
//    └─ (a) 線形 chunk 特定               | 同ブロックの upper_bound を置換
//  6b Absolute hot 段                     | 同 worker の
//                                         |   find_absolutely_hot_ranges 呼出
//    ├─ 段間ゲート (保持)                 | 呼出し直前の if 条件
//    ├─ bridge (c) 単一要素               | `&base, &base + 1`
//    └─ hot_hook 内ゲート (保持)          | hot_hook 末尾の return 条件
//  6c cold list rebuild (abs 後)          | hot_hook 内の list.insert /
//                                         | 段直後の空 base erase を置換
//                                         | 愚直化 (d)
//  6d Relative hot 段                     | 同 worker の
//                                         |   find_relatively_hot_ranges 呼出
//    ├─ 段間ゲート (保持)                 | 呼出し直前の if 条件
//    |                                    | (最適化版は !greedy_only 付き)
//    ├─ bridge (c) std::list → LinkedList | `list.begin(), list.end()`
//    └─ hot_hook 内ゲート (保持)          | hot_hook 末尾の return 条件
//  6e cold list rebuild (abs + rel 後)    | 段直後の carved_cold_range
//                                         | splice を置換 (愚直化 (d))
//  7  hot → DPU マッチング                | full_repartition の
//                                         | partial_sort / sort +
//                                         | hot_entries 収集 + rebuild_parts
//  8  initialize_in_dpu                   | full_repartition 末尾
//
// 意味論: hot 集合 / parts / nr_pairs / DPU 側 B+ tree
//         いずれも最適化版と一致する (ソート tie-break の違いを除けば)。
//         `cold_endpoint_cnt` の scalar 追跡も最適化版と同じ値を取る
//         (段ごとに `-= load` を累積するため)。
//         ただし成立するのは README §0.1 のスコープ内でのみ。
