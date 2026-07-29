`docs/repartitioning/` — `full_repartition` / `incremental_repartition` の愚直参照実装
===

## 0. このドキュメントの位置づけ

本ディレクトリは `BPForest::full_repartition` / `BPForest::incremental_repartition`
について、**最適化版 (`host/inc/bpforest.ipp`) と同じ最終状態を
生成する最も愚直な参照実装**を `.hpp` として置いている。ビルド対象では
なく、最適化版を読むときのマップ・アルゴリズムの "理論値" として使う。

- 理論・アルゴリズム・不変条件 → `docs/rebalancing-algorithm.md`
- `find_absolutely_hot_ranges` / `find_relatively_hot_ranges` 単体の
  愚直版 → `docs/hot-range-finding/`
- 本ディレクトリ → **orchestration 層** (全 KV 回収, Phase 管理, DPU 通信,
  hot→DPU マッチング, re-route) の愚直版

本 README は、naive 実装を書く・読む・改変するときに押さえておく必要が
ある「semantics とその実装を切り分ける境界線」と、書いていて踏んだ
実装上の地雷を整理したもの。

### 0.1 スコープ — 契約が成立する範囲 (最初に読むこと)

この参照実装群が「最適化版と同じ最終状態を生成する」という契約を
主張できるのは、**デフォルトパラメータの cold-carve 経路** に限る。
具体的には:

- `param.balancing >= 1` (CLI が `>= 1` を強制する)
- `param.more_hotness == 1`
- `param.greedy_only == false`
- クエリが point query (predecessor 以外) または range query
- 判定・carve の対象が **cold partition** であること

これ以降に最適化版へ追加された以下の機能は naive 側に**未反映**であり、
これらが効く設定では上記の契約は成立しない:

| 未反映の機能 | 最適化版での実体 |
|---|---|
| hot partition 自体の分割 (hot split) | `param.enable_hot_split`, `incremental_repartition_worker_hot`, `split_hot_range_equal_load`, `hot_split_plans` / `kept_hot` |
| hotness 係数 | `param.more_hotness` (`hot_load` / goal 式に乗る) |
| greedy 単独モード | `param.greedy_only` (goal 式が変わり relative 段が丸ごと消える) |
| predecessor クエリ | chunk 特定が `upper_bound` ではなく `std::lower_bound` (`IsPredecessorQuery`) |
| 並列 worker 化 | `TmpDataForFullRepartition` / `TmpDataForIncRepartition` + `std::mutex` + `parallel_run` による worker 分割 (§3.1) |

また、この worker 分割によって最適化版の行単位対応は成立しなくなったので、
本 README と naive 実装内のコメントは **行番号ではなく関数名 + phase 名**
で最適化版を指す。対応は §6 の表を参照。

---

## 1. Semantics (最優先で保つべきもの)

### 1.1 2 つの関数が外部に保証する最終状態

`full_repartition` の戻り値は `void`。`incremental_repartition` は
`Balanced` (`Yes` / `No`) を返し、`No` は「incremental では均しきれ
なかった」を意味する — このとき `full_repartition` への escalate を行うのは
呼出し側の `repartition()` ラッパであって、`incremental_repartition`
自身ではない。愚直版は戻り値を持たず自分で `full_repartition_naive` を
呼ぶ形にしているが、最終状態は同じ。

どちらも副作用として以下を atomic に更新する。外部観測できる本質的な
状態は以下の 5 つ (DPU 側 tree 含む)。愚直版はこの 5 つを最適化版と
一致させれば合格。ほかに派生キャッシュとして、`delims` の base partition
先頭 iterator の配列 `cold_delims` と、`combined_delims` と対になる
引き先配列 `combined_delims_dest` も同時に再構築される (どちらも上記から
一意に決まる)。

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
| 呼出し元 | 初期構築, `repartition()` が `Balanced::No` を受けたとき | `repartition()` から毎バッチ |
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

**現行実装との数値一致条件**: 最適化版 (`full_repartition_worker` /
`incremental_repartition_worker_cold` / `..._worker_hot` の冒頭で
定数を作る部分) では上記の式に `param.more_hotness` 係数が乗る。さらに
`param.greedy_only` が真のとき `cold_endpoint_cnt_goal` は
`max(3, α+1)/3` ではなく `× param.balancing` の式に切り替わり、同時に
relative 段の外側ゲートが `!param.greedy_only && ...` で丸ごとスキップ
される。上に書いた式が最適化版と数値一致するのは
**`more_hotness == 1` かつ `greedy_only == false`** (どちらもデフォルト)
のときだけ。

### 1.4 aggregate 上限 (不変条件)

`full_repartition` / `incremental_repartition` 一呼び出しで切り出せる
hot range 総数は `nr_base_parts` ($P$) を超えない。`new_hots` (固定
サイズバッファ) の容量 check が無いのはこの不変条件に依存している。

incremental は**事後**の bailout を持ち、超過したら `Balanced::No` を
返して `repartition()` ラッパに full への escalate を任せる。現行は
2 段構えで、どちらも `incremental_repartition` 本体にある:

- cold pass (`parallel_run(incremental_repartition_worker_cold)`) の直後 —
  `nr_existing_hots + hot_count > nr_base_parts`
- hot pass (`parallel_run(incremental_repartition_worker_hot)`) の直後 —
  `nr_existing_hots + hot_count + nr_new_pieces > nr_base_parts`
  (hot split で増える piece 数を足した版。hot pass はここまで split 計画
  `hot_split_plans` を作るだけで `delims` / `kept_hot` / `hot_delims` /
  `new_hots` を触っていないので、ここでの fallback も安全)

加えて incremental の各 worker の `get_next_idx_dpu` 冒頭にも
同じ上限 check があり、超えていたら新しい DPU を配らずループを畳む。

愚直版は hot split を持たないので 1 段目だけを持ち、`new_hots_local`
(ローカル `std::vector`) を使うので overwrite 事故は無いが、bailout の
check 自体は semantic として残している (削ると Phase 4 で範囲外アクセスに
なる)。

---

## 2. ファイル一覧

| ファイル | 対応する最適化版 | 役割 |
|---|---|---|
| `full_repartition_naive.hpp` | `BPForest::full_repartition` + `BPForest::full_repartition_worker` | full 経路の愚直版。bridge helper と rebuild helper を定義 |
| `incremental_repartition_naive.hpp` (v1) | `BPForest::incremental_repartition` + `..._worker_cold` | incremental 経路の愚直版。**DPU 側 partial serialize を廃止** し対象 DPU 丸ごと回収に単純化 |
| `incremental_repartition_serialize_naive.hpp` (v2) | 同上 | incremental 経路の愚直版。**DPU 側 partial serialize はそのまま残し** host 側ロジックだけ愚直化 |

いずれも `..._worker_hot` (hot split) には対応物を持たない (§0.1)。

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
| (a) | chunk 位置特定の `std::upper_bound` (load 推定パート。`full_repartition_worker` / `incremental_repartition_worker_cold` の chunk load populate ブロックと、後者の cold 片特定 `upper_bound(begin_part, end_part, ...)`) | chunk 列 / cold 片列の線形走査に置換 |
| (b) | 欠番 | 以前は「hot_hook 内の `cold_endpoint_cnt > goal` 判定を廃止する」と書いていたが誤り。この判定は Algorithm 2/3 のロジックそのもので、剥がすと carve される hot が変わる (§3.2) |
| (c) | cold 範囲列の `LinkedList<ChunkedPairsRange>` | `std::list<ChunkedPairsRange>`。find_* の直前だけ bridge で一時 LinkedList を組む |
| (d) | carve のたびの in-place splice / `*left_range = ...` | `rebuild_cold_list_from_hots_naive` / `rebuild_cold_list_with_origins_naive` で毎回 build-from-scratch |
| (e) | BPForest 側の一時データ用メンバ (下記) | 全てローカル変数に置換 |
| (f) (v1 のみ) | DPU 側 partial serialize プロトコル (TASK_SERIALIZE + incision_indices) | 対象 DPU 丸ごと回収 (= `retrieve_dpu_all_pairs_naive`) に単純化 |
| (g) (full のみ) | `get_next_idx_base` の pre-filter skip (point query かつ `routed.cold[b].nr_qrys <= cold_endpoint_cnt_goal` の base を load 推定ごと飛ばし、`nr_pairs` / `cold_loads` だけ埋める) | 愚直版は全 base を素通しで回す (skip された base は carve が起きないので最終状態は同じ) |

incremental 側の `get_next_idx_dpu` は (g) に含めない。こちらが飛ばすのは
Phase 1 で `do_cold` に選ばれなかった DPU であり、愚直版も
`if (!touch[idx_dpu]) { cold_loads_local[idx_dpu] = ...; continue; }` で
同じ skip をしている (剥がしていない)。`get_next_idx_dpu` 固有なのは
lock 下での DPU 配り出しと、その先頭にある
`nr_existing_hots + hot_count > nr_base_parts` の bailout 再チェック
(§1.4) の 2 点だけ。

(e) が指すメンバの現行一覧 (`host/inc/bpforest.hpp`):
`data_buf`, `chunked_cold_ranges` / `chunked_cold_ranges_lists`,
`cold_key_ranges`, `cold_loads`, `cold_npairs_list`, `new_hots`,
`input_headers`, `hot_delim_keys`, `base_to_nr_hot_psum`,
`nr_extracted_hots`, `incision_indices`, `cold_ranges` /
`cold_ranges_lists`, `hot_ranges`, `chunk2load`, および
worker 間受け渡し用の `any_tmp_data`。
hot split 用の `hot_stage1_fired` / `kept_hot` / `hot_split_plans` も
同じ扱いだが、愚直版は hot split 自体を持たない (§0.1)。

注意点 2 つ:

- `chunk2load` だけは `inline static thread_local ExtendableBuffer<uint32_t>`
  (per-thread の scratch)。他のメンバのように「DPU 番号で引く共有配列」では
  ないので、愚直版では単なるローカル `std::vector` になる。
- `hot_delim_keys` という名前は 2 箇所にある。メンバの方は Phase 1 が
  DPU へ送る incision key 列 (`SerializationCommander` が読む)。
  `incremental_repartition_worker_cold` の range query 用 load 推定
  ブロックにある同名の `static thread_local` はそれをシャドウする
  別物で、cold 片の境界 key を並べた探索表。

### 3.1.1 orchestration 層の構造 — 逐次ループ vs 並列 worker

愚直版は「`for (idx_base / idx_dpu = 0 .. nr_base_parts)` の 1 本の
逐次ループ内で全部やる」形だが、最適化版は同じ処理を

- `TmpDataForFullRepartition` / `TmpDataForIncRepartition`
  (バッチ共通の入力 + `std::mutex` + 共有カウンタ `idx_base`/`idx_dpu`,
  `cold_count`, `hot_count`, `nr_new_pieces`) を `any_tmp_data` に載せる
- `parallel_run(&BPForest::full_repartition_worker<Query, Result>)` 等で
  worker を起動し、各 worker が `get_next_idx_*` で次の DPU を取り合う

という形に分割している。DPU ごとの load 推定は lock 無しで走り、
`new_hots` / `chunked_cold_ranges` / `cold_key_ranges` プールを触る
carve 部分だけ `std::lock_guard{tmp.mutex}` の下に入る。

**この分割は carve される hot 集合を変えない**。DPU ごとの carve は
互いに独立で、`new_hots` への push 順だけが変わり、Phase 4 の matching が
load 降順に sort し直すため (同 load の tie-break だけは不定)。愚直版が
逐次ループでよい理由もそこにある。ただし副作用として最適化版とは
**行単位の 1:1 対応が付かない**ので、対応付けは関数名 + phase 単位で
考えること (§6)。

### 3.2 剥がしてはいけない「algorithm 本質」

次のものは "最適化に見えて実は Algorithm 2/3 のロジックそのもの" な
ので、`return true;` に書き換えたり外側ゲートを消したりすると、
**carve される hot の数・位置・最終パーティションが変わる**。

```cpp
// 段間ゲート (全 4 箇所、愚直版でも保持)
//   full_repartition_worker           : abs 段の直前 / rel 段の直前
//   incremental_repartition_worker_cold: abs 段の直前 (= 早期復帰の裏返し)
//                                        / rel 段の直前
if (cold_endpoint_cnt > cold_endpoint_cnt_goal) {
    find_absolutely_hot_ranges(...);  // または find_relatively_hot_ranges
}

// hot_hook 内ゲート (全 4 箇所、愚直版でも保持)
//   上と同じ 2 関数 × abs / rel の hot_hook 末尾
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
- 現行では rel 段の外側ゲートに `!param.greedy_only &&` が前置され、
  greedy_only 時は relative 段そのものが消える。愚直版は
  `greedy_only == false` (デフォルト) 前提なのでこの前置を持たない
  (§0.1)。

### 3.3 "ほぼ" 本質的だがよく誤解されるもの

- Phase 1 の閾値は **false negative を許容した safety margin** であり、
  goal に無理に近づけない。ここは現行実装で構造が変わった箇所なので
  下に詳述する。
- `cold_endpoint_cnt` の **scalar 追跡** (carve 時に `-= load` する)
  は `std::count_if` で毎回再計算しても semantics は同じだが、**最適化版
  と同じ値にしたい** ので愚直版でも scalar を使う (差分デバッグが楽)。

#### 3.3.1 Phase 1 は「trigger 判定」と「対象選定」の 2 層

かつての `param.high_watermark_ratio` による単一閾値 (`goal * ratio` を
超えた DPU だけを touch) は**削除済み**。現行の
`incremental_repartition` の Phase 1 (`ScopedTimer t{Timer, "retrieve"}`
ブロック冒頭のループ) は、同じ `routed.cold[d].nr_qrys` に対して 2 つの
判定を並行して行う:

| 層 | 比較対象 | 結果 | 役割 |
|---|---|---|---|
| trigger | `overload_threshold.threshold_for(nr_queries, cold_cnt_goal, family)` | `trigger_cold` (hot 側は `trigger_hot`) | 1 つでも立てば「このバッチは rebalance する」。誰も立たなければ `Balanced::Yes` で即 return |
| 対象選定 | `cold_cnt_goal` そのもの | `do_cold` (hot 側は `do_hot`) | TASK_SERIALIZE を送る DPU / 送る中身 (`do_cold` / `do_hot`) を決める |

hot 側の 2 つ (`trigger_hot` / `do_hot`) は `param.enable_hot_split &&` で
ゲートされているので、hot split 無効時は cold 側の判定だけが効く。
`hot_cnt_goal` の係数 `2u` は `more_hotness` と違って固定。

`threshold_for` は必ず goal 以上を返す (`HighWatermarkRatio` は CLI が
`r > 1.0` を強制、`FalsePositiveRate` は Bernstein の `lam = goal` に
正の偏差 `t` を足した値の `-1`) ので、**対象集合は trigger 集合を含む**。
`Bcap` 飽和時などに両者が一致することはあるが、逆転はしない。「rebalance
するかどうか」は保守的に、「するなら誰を作り直すか」は緩めに、という
非対称な設計であり、片方だけ真似ると挙動が変わる。

`threshold_for` は `OverloadThresholdSpec` の 2 択で意味が変わる:

- `HighWatermarkRatio{r}` (デフォルト `r = 1.05`) — `min(B, ceil(goal * r))`。
  旧 `high_watermark_ratio` に相当する。
- `FalsePositiveRate{p}` — 「負荷が均等なのに発火する確率」を目標 `p` に
  収める Bernstein 型の閾値。`family` は 1 バッチあたりの検定数
  (`nr_base_parts + nr_existing_hots`) で、Bonferroni 補正に使う。

さらに `goal > B` (バッチサイズ) のときは閾値を `B` に飽和させて
**絶対に発火させない**。詳細は `util/inc/overload_threshold.hpp`。

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
等と部分更新し、find_* 呼出し後に LinkedList の splice / erase で cold 列を
繋ぎ直す (`full_repartition_worker` と
`incremental_repartition_worker_cold` の、abs 段の hot_hook 内 +
段直後の空 piece 除去、および rel 段直後の `carved_cold_range` を
使った左右境界の splice)。
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
`incremental_repartition_worker_cold` の hot_hook が引く
`cold_key_ranges[idx_in_ary].end` の愚直化 (`idx_in_ary` は
`&part - &chunked_cold_ranges[0]`、すなわち cold 片プール上の添字)。

---

## 5. 実装上の地雷 (順不同)

### 5.1 `find_absolutely_hot_ranges` の早期終了は **multi-part を横断する**

最適化版 (`find_absolutely_hot_ranges` 本体、`bpforest.ipp`):

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
置き換えると両方壊れる** (`rebalancing-algorithm.md §4`)。

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

最適化版 (`incremental_repartition_worker_cold` の abs / rel 両 hot_hook
にある `cold_key_ranges[idx_in_ary].end` 分岐) と同じ式。
`piece_key_ranges` は incremental 経路でだけ必要 (full では
1 piece = base 全体なので `&data_buf[nr_total_pairs]` 境界との比較
1 つで済む; `full_repartition_naive.hpp` の callback 参照)。

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
carve 1 回ごとに最適化版と同じ値を取るので差分デバッグが楽。

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

worker 分割により行単位の対応は付かないので、**関数名 + phase / ブロック名**
で対応させる。個別対応は各 naive file 末尾の表も参照。

### 6.1 関数の対応

| naive | 最適化版 (`host/inc/bpforest.ipp`) |
|---|---|
| — (愚直版は escalate を自分で呼ぶ) | `BPForest::repartition` — incremental → `Balanced::No` なら full |
| `full_repartition_naive` の全体 | `BPForest::full_repartition` (orchestration) + `BPForest::full_repartition_worker` (base ごとの carve) |
| `incremental_repartition_naive` / `..._serialize_naive` の全体 | `BPForest::incremental_repartition` (orchestration) + `BPForest::incremental_repartition_worker_cold` (DPU ごとの cold carve) |
| — (未対応) | `BPForest::incremental_repartition_worker_hot` (hot split) |
| `find_absolutely_hot_ranges_on_{range,list}_naive` が包む本体 | `find_absolutely_hot_ranges` (自由関数) |
| `find_relatively_hot_ranges_on_list_naive` が包む本体 | `find_relatively_hot_ranges` (自由関数) |

### 6.2 phase の対応

| Phase | full | incremental |
|---|---|---|
| 1 DPU 選別 | `full_repartition_worker` の `get_next_idx_base` pre-filter (point query のみ) | `incremental_repartition` Phase 1 = `ScopedTimer{"retrieve"}` 冒頭のループ (trigger / do_cold / do_hot 判定 + `cold_key_ranges` / `hot_delim_keys` / `base_to_nr_hot_psum` 構築) |
| 2 データ回収 | `full_repartition` の `retrieve_all_data(data_buf)` | 同ブロックの `SerializationCommander` gather → execute → `SerializaionNrPairsReceiver` scatter → `"alloc"` ブロックの incision スライス → `"recv"` ブロックの `SerializedKVPairReceiver` |
| 2' 初回ルーティング | `full_repartition` の base ループ (等分 `chunked_cold_ranges[]` + `cold_delims` 再構築) 直後の `combine_delims` + `route_queries` | 無 (既存 partition を保つのが incremental) |
| 3a 閾値定数 | `full_repartition_worker` 冒頭の `hot_load` / `cold_endpoint_cnt_goal` | `incremental_repartition_worker_cold` 冒頭の同名 2 定数 |
| 3b load estimation | `full_repartition_worker` の `chunk2load` populate ブロック | `incremental_repartition_worker_cold` の同 populate ブロック (cold 片の特定 + `hot_delim_keys` 探索表) |
| 3c abs stage | `find_absolutely_hot_ranges(&base, &base + 1, ...)` 呼出し + 直後の空 piece 除去 | `find_absolutely_hot_ranges(begin_part, end_part, ...)` 呼出し + 直後の `npairs() == 0` erase ループ |
| 3d rel stage | `find_relatively_hot_ranges(list.begin(), list.end(), ...)` + `carved_cold_range` による splice | 同上 (同じ形の splice) |
| 3e bailout | 無 | `incremental_repartition` の cold pass 直後 (`hot_count`) と hot pass 直後 (`+ nr_new_pieces`) の 2 段 (§1.4) |
| 4 hot → DPU 割当 | `full_repartition` の `partial_sort` / `sort` + `delims.emplace` ループ | `incremental_repartition` の同形ループ (+ 既存 hot / `kept_hot` 保有 DPU を `UINT32_MAX` で除外, + split host の piece[0] 再挿入) |
| 5 DPU 送信 | `full_repartition` 末尾の `initialize_in_dpu` | `incremental_repartition` の `input_header.task_no = TASK_MOVE_HOT` 設定 + `UpdatedPartitionsSender` gather → execute |
| 6 re-route | Phase 4 の `if (hot_count > 0)` 内、`combine_delims` + `ScopedTimer{"re"}` の `route_queries` (hot が 1 つも出なければ Phase 2' の routing のまま) | 末尾 `ScopedTimer{"re"}` の `route_queries` |

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
- [ ] Phase 1 の trigger 判定を `overload_threshold.threshold_for(...)`
      で、対象選定 (`do_cold`) を goal そのもので行う **2 層** を崩して
      いないか (§3.3.1)
- [ ] §0.1 のスコープ外機能 (hot split / `more_hotness` / `greedy_only` /
      predecessor クエリ) に手を出したなら、§0.1 の表と各 naive file 冒頭の
      スコープ注記を同時に更新したか
