`docs/repartitioning/` — `full_repartition` / `incremental_repartition` の愚直参照実装
===

## 0. このドキュメントの位置づけ

本ディレクトリは `BPForest::full_repartition` / `BPForest::incremental_repartition`
について、**最適化版 (`host/inc/bpforest.ipp:1401-2138`) と同じ最終状態を
生成する最も愚直な参照実装**を `.hpp` として置いている。ビルド対象では
なく、最適化版を読むときのマップ・行単位 diff の "理論値" として使う。

- 理論・アルゴリズム・不変条件 → `docs/rebalancing-algorithm.md`
- `find_absolutely_hot_ranges` / `find_relatively_hot_ranges` 単体の
  愚直版 → `docs/hot-range-finding/`
- 本ディレクトリ → **orchestration 層** (全 KV 回収, Phase 管理, DPU 通信,
  hot→DPU マッチング, re-route) の愚直版

本 README は、naive 実装を書く・読む・改変するときに押さえておく必要が
ある「semantics とその実装を切り分ける境界線」と、書いていて踏んだ
実装上の地雷を整理したもの。

---

## 1. Semantics (最優先で保つべきもの)

### 1.1 2 つの関数が外部に保証する最終状態

どちらの関数も戻り値は `void` で、副作用として以下を atomic に更新する。
あとから外部観測できるのは以下の 5 つだけ (DPU 側 tree 含む)。愚直版は
この 5 つを最適化版と一致させれば合格。

| メンバ | 内容 |
|---|---|
| `delims` | `std::map<key_uint64_t, PartitionDelim>` — base/hot 境界 |
| `hot_delims[]` | 各 base partition に対応する hot の iterator (nullopt 可) |
| `nr_pairs[d]` | 各 DPU の `{cold_npairs, hot_npairs}` |
| `combined_delims` | `delims` を key → DPU で引ける形に flatten したもの |
| DPU 側 B+ tree | `initialize_in_dpu` / `TASK_MOVE_HOT` が作る tree 本体 |

**意味論の中心**: 各 base partition 内で "クエリ負荷が高い subrange =
hot range" を 0〜数個抜き出し、別 DPU に再配置する。hot は cold から
除去されるので **複製ではなく移動**。

### 1.2 `full_repartition` と `incremental_repartition` の違い

| 項目 | `full_repartition` | `incremental_repartition` |
|---|---|---|
| 適用範囲 | 全 DPU | 負荷が偏った DPU のみ |
| データ回収 | 全 KV を CPU に pull | 対象 DPU の cold partial だけ (TASK_SERIALIZE) |
| 呼出し元 | 初期構築, incremental の bailout | 定常バッチ (`batch_get`, `batch_range_count`) |
| コスト感 | batched RQ 100 回分 | Full よりはるかに安い |

どちらも **「hot を抜き出す → cold 残余を保持」** という同一の base
partition 単位操作を持つ。違いは "どこまで全体を作り直すか" だけ。
incremental は前バッチの状態を残し、対象 DPU 以外には触らない。

### 1.3 愚直版が絶対に壊してはいけない式

以下はアルゴリズムの core であり、どんなに愚直化しても削ってはいけない:

```cpp
constexpr uint32_t W = IsPointQuery<Query> ? 1 : 2;
hot_load               = (nr_queries * W + nr_base_parts - 1) / nr_base_parts;
cold_endpoint_cnt_goal =  nr_queries * W * max(3u, α + 1) / 3 / nr_base_parts;
hot_npairs             = (base_npairs + α - 1) / α;    // α = param.balancing
nr_relative_hots       = cold_endpoint_cnt / hot_load;  // 論文の β
```

特に `cold_endpoint_cnt_goal` の `max(3, α+1)/3` 係数は Algorithm 3
double-scan 後の **cold 側クエリ負荷の上界** に対応するので、簡略化
しない。詳細は `rebalancing-algorithm.md §1.3`。

### 1.4 aggregate 上限 (不変条件)

`full_repartition` / `incremental_repartition` 一呼び出しで切り出せる
hot range 総数は `nr_base_parts` ($P$) を超えない。`new_hots` (固定
サイズバッファ) の容量 check が無いのはこの不変条件に依存している。
incremental は**事後**の bailout (L2073-2076) を持ち、`nr_existing_hots
+ hot_count > nr_base_parts` なら `full_repartition` に escalate する。

愚直版も `new_hots_local` (ローカル `std::vector`) を使うので
overwrite 事故は無いが、bailout の check 自体は semantic として
残している (削ると Phase 4 で範囲外アクセスになる)。

---

## 2. ファイル一覧

| ファイル | 対応する最適化版 | 役割 |
|---|---|---|
| `full_repartition_naive.hpp` | `bpforest.ipp:1401-1610` | full 経路の愚直版。bridge helper と rebuild helper を定義 |
| `incremental_repartition_naive.hpp` (v1) | `bpforest.ipp:1664-2138` | incremental 経路の愚直版。**DPU 側 partial serialize を廃止** し対象 DPU 丸ごと回収に単純化 |
| `incremental_repartition_serialize_naive.hpp` (v2) | 同上 | incremental 経路の愚直版。**DPU 側 partial serialize はそのまま残し** host 側ロジックだけ愚直化 |

`full_repartition_naive.hpp` は以下の共用ヘルパを定義しており、
v1 / v2 はそれらを再利用する:

- `find_chunk_containing_key_naive(part, key)` — chunk の線形走査
- `estimate_chunk_load_naive(...)` — full 用の chunk load populate
- `find_absolutely_hot_ranges_on_range_naive(range, ...)` — 単一要素 bridge
- `find_absolutely_hot_ranges_on_list_naive(cold_list, ...)` — 複数要素 bridge
- `find_relatively_hot_ranges_on_list_naive(cold_list, ...)` — 複数要素 bridge
- `rebuild_cold_list_from_hots_naive(list, load_ary, begin, end, hots)` — 単一 base 前提の rebuild

`incremental_repartition_naive.hpp` は incremental 特有の追加ヘルパを定義:

- `IncrementalOriginPiece { range, key_range, load_base }` — carve 前の
  cold 片 1 つ分のレコード
- `rebuild_cold_list_with_origins_naive(cold_list, piece_key_ranges, origins, sorted_hots)`
  — 複数 origin 前提の rebuild (v2 からも利用)
- `retrieve_dpu_all_pairs_naive(BPForest&, idx_dpu)` — v1 独自 helper (宣言のみ)

v2 は上記に加えて自分用のヘルパを宣言:

- `serialize_cold_on_touched_dpus_naive(...)` — TASK_SERIALIZE ラッパ (宣言のみ)
- `send_task_move_hot_naive(...)` — TASK_MOVE_HOT ラッパ (宣言のみ)

---

## 3. 「剥がす最適化」と「剥がさない algorithm 本質」の境界線

naive 実装を書くときに最も迷う境界。以下を混同すると最終 partition が
変わる (= 意味論が壊れる)。

### 3.1 剥がしてよい最適化

| 記号 | 内容 | 愚直化の方針 |
|---|---|---|
| (a) | chunk 位置特定の `std::upper_bound` (L1470, L1484, L1878, L1922) | chunk 列の線形走査に置換 |
| (c) | cold 範囲列の `LinkedList<ChunkedPairsRange>` | `std::list<ChunkedPairsRange>`。find_* の直前だけ bridge で一時 LinkedList を組む |
| (d) | carve のたびの in-place splice / `*left_range = ...` | `rebuild_cold_list_from_hots_naive` / `rebuild_cold_list_with_origins_naive` で毎回 build-from-scratch |
| (e) | BPForest 側の一時データ用メンバ (`data_buf`, `chunk2load`, `chunked_cold_ranges[,_lists]`, `new_hots`, `cold_loads`, `input_headers`, `hot_delim_keys`, `base_to_nr_hot_psum`, `incision_indices`, `cold_key_ranges`, `cold_npairs_list` 等) | 全てローカル変数に置換 |
| (f) (v1 のみ) | DPU 側 partial serialize プロトコル (TASK_SERIALIZE + incision_indices) | 対象 DPU 丸ごと回収 (= `retrieve_dpu_all_pairs_naive`) に単純化 |

### 3.2 剥がしてはいけない「algorithm 本質」

次のものは "最適化に見えて実は Algorithm 2/3 のロジックそのもの" な
ので、`return true;` に書き換えたり外側ゲートを消したりすると、
**carve される hot の数・位置・最終パーティションが変わる**。

```cpp
// 段間ゲート (全 4 箇所、愚直版でも保持)
//   bpforest.ipp:1495 (full abs 前), 1523 (full rel 前),
//                1948 (incr abs 前), 1999 (incr rel 前)
if (cold_endpoint_cnt > cold_endpoint_cnt_goal) {
    find_absolutely_hot_ranges(...);  // または find_relatively_hot_ranges
}

// hot_hook 内ゲート (全 4 箇所、愚直版でも保持)
//   bpforest.ipp:1516, 1540, 1989, 2019
return cold_endpoint_cnt > cold_endpoint_cnt_goal;
```

実装上の補足:
- **hot_hook が `return false` を返すと** `find_absolutely_hot_ranges`
  / `find_relatively_hot_ranges` は **即座に関数全体から return する**。
  次の `part` の処理にも進まない (§5.1 参照)。これは `cold_endpoint_cnt`
  が閾値を下回った瞬間に全体の carve を止めるための仕組み。
- 段間ゲート (外側) と hot_hook 内ゲートは **両方とも** algorithm 本質。
  外側だけ残して内側を `return true` にすると、1 回の find_* 呼び出しで
  余計に carve が進む可能性がある。

### 3.3 "ほぼ" 本質的だがよく誤解されるもの

- Phase 1 の `cold_partial_cnt_threshold` (高 watermark)。これは **false
  negative を許容した safety margin** であり、`param.high_watermark_ratio`
  を無理に 1.0 に近づけない。
- `cold_endpoint_cnt` の **scalar 追跡** (carve 時に `-= load` する)
  は `std::count_if` で毎回再計算しても semantics は同じだが、**最適化版
  と同じ値にしたい** ので愚直版でも scalar を使う (差分デバッグが楽)。

---

## 4. 共通 vocabulary (bridge / rebuild / origin)

### 4.1 Bridge パターン — `std::list` ↔ `LinkedList` 変換

`find_absolutely_hot_ranges` / `find_relatively_hot_ranges` は最適化版
では `LinkedChunkedPairsRange*` 範囲 / `LinkedList<ChunkedPairsRange>::iterator`
範囲で呼ばれる。愚直版は cold 列を `std::list` で持ちたいので、呼出し
直前にその場で `std::vector<LinkedChunkedPairsRange> pool(n)` を確保し、
各要素を copy-assign した上で `find_*_hot_ranges(pool.data(), ...)`
または `LinkedList` を組み立て直して `find_*(llist.begin(), llist.end(), ...)`
に渡す。呼出し後 pool は破棄する。

#### 4.1.1 アドレス安定性の条件

`LinkedChunkedPairsRange` は intrusive next/prev ポインタを持つ型なので、
pool 要素が reallocation で移動するとリンクが壊れる。`std::vector<T> pool(n);`
の形で **n 要素を一括直接構築** する場合のみ、要素アドレスは関数
スコープ内で安定。`pool.reserve(n); for (...) pool.emplace_back(...);`
も安定だが、途中で `resize` / 追加の `push_back` をすると壊れうる。

#### 4.1.2 Piece 特定の仕組み

bridge の inner lambda は `find_*_hot_ranges` から `ChunkedPairsRange&`
を受け取るが、このアドレスは pool 配列上の要素アドレスなので、

```cpp
const size_t piece_idx = static_cast<size_t>(
    static_cast<LinkedChunkedPairsRange*>(&part) - pool.data());
```

で元の `cold_list` 内 0-based index を復元できる。これを lambda の
第 1 引数として外の hot_hook に渡す (incremental 経路では
`piece_key_ranges[piece_idx]` を引くのに必要)。

`LinkedChunkedPairsRange = LinkedElement<ChunkedPairsRange>` は
`LinkedElement<T>` が `T` を public 継承しているので、参照の downcast
(`static_cast<LinkedChunkedPairsRange*>`) は元が本当に pool 由来である
限り UB ではない。

### 4.2 Rebuild helper — in-place splice を捨てて毎回組み直す

最適化版は find_* の callback 内で `*left_range = {end, left_range->end()};`
等と部分更新し、find_* 呼出し後に LinkedList の splice で cold 列を
繋ぎ直す (bpforest.ipp:1500-1506, 1543-1578, 1967-1996, 2022-2057)。
愚直版はこれを全部捨てて、`new_hots_local` に溜めた hot 区間列から
cold 列を **毎回 build-from-scratch** で組み立て直す。

- `rebuild_cold_list_from_hots_naive` (full 用) — 単一 base partition を
  仮定する単純版。`(base_begin, base_end)` と sorted hots から cold
  sub-piece を emit し、各 sub-piece の load ポインタは `base_load_ary
  + chunk_offset` で計算。
- `rebuild_cold_list_with_origins_naive` (incremental 用) — 複数 origin
  cold 片を前提とする版。各 origin ごとに内部 hot を subtract し、
  sub-piece の `KeyRange` を `{cur_kr_begin, hot.begin->key-1}` / 末尾は
  `{cur_kr_begin, origin.kr.end}` と計算する。

### 4.3 `IncrementalOriginPiece` — incremental 特有の "carve 前" レコード

```cpp
struct IncrementalOriginPiece {
    PairsRange range;      // 1 cold 片の KV ペアスライス
    KeyRange   key_range;  // この cold 片がカバーする key 範囲
    uint32_t*  load_base;  // per-DPU chunk_load_buf 内のこの片の先頭
};
```

v1 では cold_delims を key で walk して origin を作り、v2 では
incision_indices で配列 offset スライスして origin を作る。作り方は
違うが、**出来上がった origin の interface は共通** なので、その後の
carve ループと rebuild helper を完全に共有できる。

origin の `key_range.end` は後段 rebuild で `{cur_kr_begin, origin.kr.end}`
として tail sub-piece の key 範囲を決めるのに使う。これは最適化版
`cold_key_ranges[idx_in_ary].end` (bpforest.ipp:1980-1983) の愚直化。

---

## 5. 実装上の地雷 (順不同)

### 5.1 `find_absolutely_hot_ranges` の早期終了は **multi-part を横断する**

最適化版 `bpforest.ipp:1175-1204`:

```cpp
for (LinkedChunkedPairsRange* part = begin_part; part != end_part; part++) {
    ...
    while (right != end_chunk) {
        ...
        if (load >= hot_nqrys) {
            if (!hot_hook(*part, left, right, load)) {
                return;  // ← 全 part を横断して関数から return
            }
            ...
        }
    }
}
```

つまり callback が `false` を返した瞬間、次の `part` の処理も走らない。
愚直版で **per-piece に `find_absolutely_hot_ranges(pbase, pbase+1, ...)` を
繰り返し呼ぶのは意味論的に等価ではない** (繰り返し呼ぶと早期終了が
piece をまたがない)。

このために full_repartition_naive.hpp には `find_absolutely_hot_ranges_on_list_naive`
という **複数 piece を 1 回で渡す bridge** を置いている。incremental v1 / v2
もこの bridge を必ず使うこと。

### 5.2 `end_chunk` snapshot と callback 内 mutation

同じ `find_absolutely_hot_ranges` 内:

```cpp
DataChunkIterator left = part->begin(), right = left;
const DataChunkIterator end_chunk = part->end();   // ← snapshot
...
while (right != end_chunk) { ... hot_hook(*part, ...); ... }
```

`end_chunk` と `left`/`right` は `DataChunkIterator` (値型) として捕捉
されるので、callback が `*part` を `{end, part.end()}` に書き換えても
ループ制御は壊れない。愚直版 bridge の callback は `*part` を書き換え
ない形にしており (その代わり rebuild helper で再構築する)、これは
安全側に倒した選択。

**もし将来 `ChunkedPairsRange` を list iterator 越しの live reference 型に
置き換えると両方壊れる**。`util/eval_rebalancing` でも類似 bug (callback
内 mutation → 隣接 heap 破壊) が過去に出ている (`rebalancing-algorithm.md §4`)。

### 5.3 `hot.key_range.end` の 2 パターン

hot を carve したとき、その key_range.end は:

- **hot の末尾が所属 piece の末尾と一致する場合** → piece の `key_range.end`
  をそのまま使う (= piece が元々覆っていた key 範囲の終端)
- **それ以外 (mid-piece で切れる場合)** → `hot.end->key - 1` (hot の次に
  読まれる KV の key の 1 つ前)

```cpp
hot.key_range.end
    = (hot.pairs_range.end() == part.PairsRange::end()
             ? piece_key_ranges[piece_idx].end      // piece 末尾
             : hot.pairs_range.end()->key - 1);      // mid-piece
```

最適化版 (L1980-1983, L2043-2046 等) と同じ式。`piece_key_ranges` は
incremental 経路でだけ必要 (full では 1 piece = base 全体なので
`data_end` 境界との比較 1 つで済む; `full_repartition_naive.hpp` の
callback 参照)。

### 5.4 Phase 1 の `kranges.size() == pieces`, `hkeys.size() == pieces - 1`

v2 の Phase 1 は cold_delims を walk して以下の 2 つを並行構築する:

- `per_dpu_cold_key_ranges[idx_dpu]` — 各 cold 片の KeyRange
- `per_dpu_hot_delim_keys[idx_dpu]` — cold 片と cold 片の間にある
  **内部** 既存 hot の先頭 key

不変条件: `kranges.size() == pieces`, `hkeys.size() == pieces - 1`
(= 非空 cold 片の数, 内部境界の数)。

#### 5.4.1 `base_max == KEY_MAX` オーバーフロー

末尾 hot が base partition の末尾まで届くケースで、`cur_begin = hd.max_key + 1`
が KEY_MAX から 0 にオーバーフローし、`cur_begin <= base_max` が偽陽性
になる。`incremental_repartition_serialize_naive.hpp` では **専用 flag
`tail_taken_by_hot`** で検出する:

```cpp
bool tail_taken_by_hot = false;
for (...) {
    ...
    if (hd.max_key == base_max) tail_taken_by_hot = true;
    cur_begin = hd.max_key + 1;
}
if (tail_taken_by_hot) {
    if (!hkeys.empty()) hkeys.pop_back();  // "末尾の飾り" を除去
} else if (cur_begin <= base_max) {
    kranges.push_back({cur_begin, base_max});
}
```

v1 は cold KV 配列を直接 linear split する方式 (`cursor < cold_end` で
tail 判定) なのでこのオーバーフローは踏まない。

### 5.5 incision_indices の解釈

v2 の Phase 3a で cold 片を復元する部分の semantics:

- `incisions[idx_dpu][k]` = k 番目 incision までの cold pair **累積数**
- 片の境界 = incision 位置 (先頭は 0, 末尾は `nr_cold_total`)
- 片数は cold-ending なら `incisions.size() + 1`, hot-ending なら
  `incisions.size()` (= 末尾 incision がちょうど `nr_cold_total` と一致)

```cpp
for (incision_pos : incisions[idx_dpu]) {
    nr_in_piece = incision_pos - pair_cursor;
    if (nr_in_piece > 0) push_origin(...);
    ...
}
if (pair_cursor < nr_cold_total[idx_dpu]) {
    push_origin(tail_piece);  // cold-ending のみ triggers
}
```

どちらのパターンでも `origins.size() == kranges.size()` となるよう、
origin push のタイミングで `idx_piece = origins.size()` を付番する。

### 5.6 `cold_npairs` / `cold_endpoint_cnt` の scalar 更新

両関数は **carve ごと** に `cold_npairs -= hot.npairs()`, `cold_endpoint_cnt -= hot.load`
を適用する形で scalar を追跡している。naive 版もこれに倣っておくと、
最適化版との行単位対応で数値が一致するので差分デバッグが楽。

- `cold_endpoint_cnt` の初期値は load estimate で cold 側に落ちた endpoint 数
- carve 後に `std::count_if` で全件再計算する方針はセマンティクス的には
  等価だが、最適化版の scalar と 1 ペア違いを生むケースがあり、差分比較が
  ズレる

### 5.7 `LinkedElement` の copy assignment

`LinkedElement<T>` は intrusive node なので copy ctor は delete されて
いるが、`operator=(const T&)` は提供されており `T` 部分だけを copy する
(リンクには触らない)。bridge helper はこれに依存している:

```cpp
std::vector<LinkedChunkedPairsRange> pool(cold_list.size());
size_t i = 0;
for (const ChunkedPairsRange& r : cold_list) {
    pool[i] = r;   // ← LinkedElement<T>::operator=(const T&)
    ++i;
}
```

---

## 6. 対応表: naive ↔ bpforest.ipp

個別対応は各 naive file 末尾の表を参照:

- `full_repartition_naive.hpp` 末尾表 — full 1-step ↔ L1401-1610
- `incremental_repartition_naive.hpp` 末尾表 — incremental 1-step ↔ L1664-2138
- `incremental_repartition_serialize_naive.hpp` 末尾表 — 同上 + v2 固有差分

要点だけ再掲:

| Phase | full | incremental |
|---|---|---|
| 1 DPU 選別 | 無 (全 DPU 対象) | L1691-1738 |
| 2 データ回収 | L1405 (retrieve_all_data) | L1741-1825 (TASK_SERIALIZE) |
| 3a 閾値定数 | L1442-1443 | L1887-1899 |
| 3b load estimation | L1455-1493 | L1856-1935 |
| 3c abs stage | L1495-1521 | L1956-1990 |
| 3d rel stage | L1523-1541 | L1999-2058 |
| 3e bailout | 無 | L2063-2066 |
| 4 hot → DPU 割当 | L1585-1606 | L2072-2093 |
| 5 DPU 送信 | L1609 (initialize_in_dpu) | L2097-2136 (TASK_MOVE_HOT) |
| 6 re-route | L1605 (内部で実施) | L2142-2146 |

---

## 7. 修正時のチェックリスト

naive 参照実装を書き換える / 新しいパターンを追加するときに確認すること:

- [ ] `cold_endpoint_cnt > cold_endpoint_cnt_goal` の **外側ゲート** と
      **hot_hook 内ゲート** を両方残しているか (§3.2)
- [ ] `find_absolutely_hot_ranges` を複数 piece に呼ぶとき、**per-piece loop
      ではなく** `find_absolutely_hot_ranges_on_list_naive` bridge を 1 回だけ
      呼んでいるか (§5.1)
- [ ] hot_hook 内で `*part` を書き換えていないか (書き換えが必要に見えたら
      rebuild helper の方に責務を移せないか検討) (§5.2)
- [ ] `hot.key_range.end` を "piece 末尾 vs mid-piece" の 2 分岐で算出して
      いるか (§5.3)
- [ ] Phase 1 の `kranges` と `hkeys` の同期、および `tail_taken_by_hot`
      分岐 (v2) を崩していないか (§5.4)
- [ ] incremental の `nr_existing_hots + new_hots > nr_base_parts` bailout を
      残しているか (§1.4)
- [ ] `new_hots` を load 降順 / `cold_loads` を load 昇順 sort して matching
      する順序 (`partial_sort` → `sort`) を崩していないか
- [ ] 既に hot を持つ DPU を `cold_loads[idx].second = UINT32_MAX` にして
      matching 候補から除外しているか (incremental のみ)
- [ ] Phase 5 後に `combine_delims()` → `route_queries()` を呼び直して
      いるか
- [ ] `rebalancing-algorithm.md §3` の不変条件 ($P$ 以下の hot total)
      が破れる経路を作っていないか
