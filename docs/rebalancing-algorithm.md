Rebalancing Algorithm
===

BPForest の rebalancing (クエリ負荷を平滑化するための hot range 抽出と
再配置) の仕組みを、**意味 → 実装** の順に整理したドキュメント。

コード source of truth: `bpforest/inc/bpforest.ipp`
理論対応: 論文の §"Query Density-Driven Partitioning" と appendix
関連参照ドキュメント:

- `docs/pairs-range.md` — 型と capability

---

## 1. Semantics

### 1.1 rebalancing が担うもの

各 DPU は base partition を 1 つ持ち、その中にクエリ負荷の高い subrange
(= hot range) が見つかれば、それを**抜き出して別 DPU に移送**することで
DPU 間のクエリ負荷を均す。

- 入力: 今バッチのクエリ (keys / range queries) と各 DPU の現 partition
- 出力: 更新後のパーティション表 `parts` と `hot_ranges`、DPU に送る `TASK_MOVE_HOT` 指示
- 性質: 1 回の再分割で新たに作る hot は $P$ (= DPU 数) 個以下 (§3)

呼び出し位置:

- `BPForest::batch_get` / `batch_pred` / `batch_insert` / `batch_delete` /
  `batch_range_count` / `batch_range_max` が `route_queries` 直後に
  `repartition()` ラッパを呼ぶ
- `repartition()` は `param.enable_dynamic_repartition` (CLI
  `--dynamic-repartition`, デフォルト on) のときだけ動く。まず
  `incremental_repartition` を試し、返り値が `Balanced::No` なら
  `full_repartition` に escalate する
- 初期構築系 (`partition_with_get_batch` など) は直接 `full_repartition`

### 1.2 2 つの経路

| 経路 | 意味 | いつ走るか |
|---|---|---|
| `full_repartition` | 全 KV ペアを CPU に回収して partition を組み直す重い経路 | 初期構築時、および incremental が `Balanced::No` を返した時 |
| `incremental_repartition` | 前バッチの状態を活かして負荷が偏った DPU だけ touch する軽い経路 | 定常バッチ処理。`new_hots` に収まらない場合は `Balanced::No` を返す (§3) |

### 1.3 用語と記号

1 バッチ内のクエリは point-only か range-only のいずれかで、混在しない。
range query の負荷は **begin/end の 2 つの endpoint への point query と
して近似** する (1 本 = 2 本相当, $W = 2$)。以降の式はこの近似を前提に
している。

論文の記号との対応 (`more_hotness = 1`、$W = 1$、既存 hot が無いとき):

| 論文 | コード | 意味 |
|---|---|---|
| $P$ | `nr_base_parts` | DPU 数 (= base partition 数) |
| $Q$ (論文) / $Q \cdot W$ (コード) | `nr_queries * W`, $W \in \{1, 2\}$ | クエリ負荷の総和 |
| $D$ | 総 KV ペア数 | 論文はバイト数、コードはペア件数 |
| $Q/P$ / $\lceil Q/P \rceil$ | `hot_load` | hot 1 つあたりの負荷の目安。split の piece 数の単位 (§4) |
| $\alpha$ | `param.balancing` | balancing factor |
| $\dfrac{1}{\alpha}\dfrac{D}{P}$ | `hot_npairs` | 1 hot range の目標ペア数 (= ブロックの幅; §4) |
| — | `param.more_hotness` | `hot_load` と `cold_endpoint_cnt_goal` に乗る倍率 (CLI `--more-hot`, デフォルト 1) |

代表的な式 (`full_repartition_worker` / `incremental_repartition_worker_cold`
の冒頭; 両者同式):

```cpp
hot_load               = (more_hotness * nr_queries * W + nr_base_parts - 1) / nr_base_parts;
cold_endpoint_cnt_goal = more_hotness * nr_queries * W * (balancing + 1) * (balancing + 1) / balancing / 4 / nr_base_parts;  // cold_load_goal()

// full_repartition_worker: 対象 base の cold ペア数から
hot_npairs       = (cold_npairs + param.balancing - 1) / param.balancing;
// incremental_repartition_worker_cold: cold + 既存 hot を合算した base ペア数から
hot_npairs       = (base_npairs + param.balancing - 1) / param.balancing;
```

`cold_endpoint_cnt_goal` の係数 $\dfrac{(\alpha+1)^2}{4\alpha}$ は、§4 の選び方で cold の負荷を
goal まで下げても切り出す個数が $P$ を超えない (§3)、最小の係数である。

**Phase 1 と Phase 3 (§6) の比較対象は別単位** (incremental 経路):

- Phase 1 (`incremental_repartition` 冒頭の pre-filter) の `cold_cnt_goal`
  は **パーティション境界で切られた range query の断片 (fragment) の本数**
  (`routed.cold[d].nr_qrys`; `W` を乗せない) と比較
- Phase 3 (`incremental_repartition_worker_cold`) の
  `cold_endpoint_cnt_goal` は **原 range query の begin/end endpoint の
  うち cold subrange に落ちた本数** と比較 (`W=2`)

両者は単位も係数も違うので直接比較できない。point query では fragment の数と
endpoint の数はともにクエリ数で、両者は一致する。

**load estimation の近似**: range query の実際の交差 chunk 数は無視され、
begin/end の各 endpoint が対象範囲に落ちた場合のみ chunk の `load()` に
+1 する。full 側 (`full_repartition_worker` の load estimation ブロック)
は `[base_min, base_max]` で判定、incremental 側
(`incremental_repartition_worker_cold`) は `cold_key_ranges` から作った
`cold_range_bounds` 列で「残存 cold subrange に落ちた endpoint」だけ計上し、同一
query が同じ DPU の複数 fragment に routed されていても `orig_idx` で
dedup する。

---

## 2. データ構造

詳細は `docs/pairs-range.md` 参照。ここでは rebalancing 文脈での役割のみ:

- `PairsRange` / `ChunkedPairsRange` — cold subrange を表す非所有ビュー
- `DataChunkIterator` — `KVPairsChunkSize` ペア単位の chunk iterator。ブロック (§4) と split (§6.2) の粒度
- `BPForest::parts` — 鍵順のパーティション表。rebalancing の出力そのもの
- `NewHotRange = {PairsRange, KeyRange, load, origin}` — 抜き出した hot のスロット。`origin` は切り出し元の base partition で、`parts` を組み直すときに要る
- `LinkedList<ChunkedPairsRange>` — 各 DPU の cold range 集合。hot 抜出しに伴う分割を erase/insert で扱う (iterator stable)
- `BPForest::new_hots` — `ExtendableBuffer<NewHotRange>{nr_base_parts + 1}` の **固定サイズバッファ** (§3)。+1 は `carve_new_hots` が併合して消す余分な piece の分
- `BPForest::kept_hot` / `BPForest::hot_split_plans` — hot partition split (§6.2) の中間データ。split 元 DPU に残す piece[0] と、再配置する pieces[1..] の受け渡しに使う

---

## 3. 不変条件

**命題 (full 経路):** `full_repartition` が 1 回走り切ったとき、新たに切り出される
hot の総数 `hot_count` は `nr_base_parts` (= $P$) を超えない。

**証明の要点** (`more_hotness = 1`、丸めは無視する): base partition $b$ の cold の
負荷を $L_b$、$x_b = L_b / (Q/P)$、$c = (\alpha+1)^2/(4\alpha)$、ブロック数を $m$ とする。
cold range は 1 本で、末尾以外のブロックは `hot_npairs` $= \lceil n/\alpha \rceil$ ($n$ は
cold のペア数) ペア以上を持つので、$m \le \alpha$。負荷の大きい順に $k$ 個目のブロックを取るのは、$k - 1$ 個を
取った残りが goal $c \cdot Q/P$ を超えているときで、残りは $L_b (m-k+1)/m$ 以下なので
$k - 1 < \alpha (1 - c/x_b)$。相加相乗平均 $x_b + \alpha c / x_b \ge \alpha + 1$ から
$k < x_b$。split の piece で数えても同じである: 負荷 $l$ のブロックから出る piece は
$\max(1, \lfloor l / \text{hot\_load} \rfloor)$ 個で、$l \ge Q/P$ ならこれは $l/(Q/P)$ 以下。
$l < Q/P$ のブロックは負荷順で後ろに並ぶので、それらだけを対象に、$L_b$ から前者の負荷を
引いた値で同じ議論を繰り返せる。よって base partition $b$ からは $x_b$ 個以下。
$\sum_b L_b \le Q$ から合計は $P$ 以下。

**incremental 経路では成り立たない:** 既存の hot が cold を分断していると
cold range が複数本になり、ブロック数は $\alpha + (\text{その base 由来の既存 hot の数})$
まで増える。既存の hot まで含めて $P$ 以下という保証も無い。

**コード側の担保:**

- `new_hots` は固定サイズ。両 worker の hook は `carve_new_hots` に残り枠
  (incremental は `P − (既存 hot の数) − hot_count`、full は `P − hot_count`) を渡し、
  次のブロックの piece がそこに収まらなければ `carve_new_hots` は何も書かずに 0 を返す
- incremental の hook は 0 を受けたら `overflow` を立てて選択をやめる (そのブロックは
  cold list からは外れているが、full に落ちるので list ごと捨てられる)。
  `incremental_repartition` は、cold pass の直後に `overflow` なら `Balanced::No`、
  hot pass の直後に split 由来の新片も勘定して
  `nr_existing_hots + hot_count + nr_new_pieces > nr_base_parts` なら
  `Balanced::No` を返す。hot split の pieces[1..] は 2 つ目の check を通過した後で
  初めて `new_hots` に書き込まれる
- full の hook は上の命題により 0 を受けない (`assert`)

---

## 4. `find_relatively_hot_ranges` と新しい hot の据え方

### Semantics

1 つの base partition の cold range 集合を `hot_npairs` ペアごとのブロックに区切り、
負荷の大きいブロックから順に hot にしていく。

- **ブロック**: 各 cold range を先頭から区切る。1 ブロックは `hot_npairs` ペアを
  chunk 単位に切り上げた幅で、range の末尾の端数もそのまま 1 ブロックにする。
  ブロックは cold range を跨がない
- **選択**: ブロックを負荷の降順に `hot_hook` へ渡し、hook が
  `false` を返したところで止める。両 worker の hook は「cold の負荷が
  `cold_endpoint_cnt_goal` 以下になった」または「`new_hots` の残り枠に収まらない」
  (§3) で止める
- **cold の残り**: 渡したブロックを list から取り除く。range の残りは元の node に
  残し、取り除いた結果 range がちぎれたときは `new_cold_hook` が返す node に入れる。
  hook が走っている間 list は変わらない

callback 規約: `bool hot_hook(range, begin_chunk, end_chunk, load)`、
`LinkedChunkedPairsRange& new_cold_hook()`。

選ばれる hot は隣り合うとは限らない。ブロックの走査には `DataChunkIterator` を使う。これは
`part_begin/part_end/cursor/p_load` を値として保持し、range の node を live 参照しない
ので、cold の残りを組み直す間に node を書き換えても、記録済みのブロックの境界は壊れない。

テストは `bpforest/test/hot_range_finding.cpp` (`hot_range_finding_test_<target>`)。乱数で作った
cold range 集合について、渡されるブロックとその順、残る cold range を、この節の定義を
そのまま書いた参照実装と突き合わせる。

### 新しい hot の据え方 (`carve_new_hots`)

両 worker の `hot_hook` は選ばれたブロックを `carve_new_hots` に渡す。これは
ブロックを 1 個以上の hot partition (`NewHotRange`) にして `new_hots` に書き、
個数を返す。`param.enable_hot_split` のとき、ブロックの負荷が hot split の goal
(`hot_load_goal()`; §6 Phase 1 の `hot_cnt_goal` を端点の単位で計算した値) を超えるなら
`split_hot_range_equal_load` (§6.2) で `負荷 / hot_load` 個の piece に割る。piece の数が
`new_hots` の残り枠 (§3) を超えるときは何も書かずに 0 を返す。

### 割っても重い hot の扱い (`relieve_overloaded_hots`)

新しい hot を DPU に割り当て、クエリを振り分け直した後、この再分割で据えた hot
(cold から切り出したもの、hot split の pieces[1..]、split 元に残した piece[0]) のうち、
振り分けられたクエリ数 `routed.hot[dpu].nr_qrys` が `hot_cnt_goal` を超えるものについて、
次の順に試みる (`param.enable_hot_split` のとき; 1 つの hot に対する処理は `relieve_overloaded_hot`)。
これは、hot が 1 chunk より大きければ、次のバッチでその hot が hot split の対象
(`do_hot`; §6 Phase 1) になる条件と同じである。

1. **ホスト側キャッシュへの採用** (docs/host_hot_cache.md の「採用」): 採用した
   キーの推定件数をクエリ数から引く
2. **`hot_split_failed`**: それでも `hot_cnt_goal` を超えるなら、その DPU の
   `hot_split_failed` を立てる

1 つでもペアをキャッシュに入れたら、クエリをもう一度振り分け直す。

---

## 5. `full_repartition`

### Semantics

全 KV ペアを CPU に回収し、DPU 数で等分した base partition 上から
§4 で hot を抜き出し、`partial_sort` で低負荷 DPU に
割り当ててから全 DPU の tree を再構築する **一括リビルド** 経路。
論文 Algorithm 1 "Construction of hot/cold partitions" の骨格。

### Implementation (フロー)

`full_repartition_worker` のワーカー間は `TmpDataForFullRepartition` の
mutex と共有カウンタ (`idx_base` / `cold_count` / `hot_count`) で排他する。

`full_repartition` 本体:

1. 全 DPU の `hot_cache` を捨て (docs/host_hot_cache.md の「追い出し」)、`kept_hot` を落とす
2. `retrieve_all_data(data_buf)` で全 DPU から KV ペア pull
3. `data_buf` を `nr_base_parts` 等分、各 base partition を
   `chunked_cold_ranges[]` に詰め、`parts` を base partition だけで作り直す
4. `rebuild_part_indices()` → `route_queries()` で新 partition に re-route
5. **find_hot 段**: `parallel_run(&full_repartition_worker)` (下記)
6. `hot_count > 0` の場合のみ、`new_hots[0..hot_count)` を低負荷 DPU に
   割当:
   - `partial_sort` で `cold_loads` 前 `hot_count` 個を負荷昇順
   - `new_hots` を load 降順 sort
   - 負荷の低い DPU ← 負荷の高い hot の対応付け、`hot_entries[]` に記録
   - `rebuild_parts()` → `route_queries()` で更新後の partition に再 route
   - `relieve_overloaded_hots` (§4)
7. `initialize_in_dpu(...)` で DPU 側 tree 再構築

`full_repartition_worker` (各スレッド、`get_next_idx_base` で base を
1 つずつ取得):

- **base 取得** (`get_next_idx_base`, mutex 下): point query の場合は
  `routed.cold[idx].nr_qrys <= cold_endpoint_cnt_goal` の base をここで
  スキップし `nr_pairs` / `cold_loads` だけ記録する。range query は
  fragment 本数と endpoint 本数が別単位 (§1.3) なのでここではスキップ
  せず、endpoint 計数後に判定する
- `cold_npairs`, `hot_npairs` 確定
- **load estimation** (mutex 外・並列): §1.3 の近似。point の chunk は
  `upper_bound` (predecessor 系は `lower_bound`) で引く
- **hot 選出** (mutex 下; `cold_endpoint_cnt > cold_endpoint_cnt_goal`
  の場合のみ): `find_relatively_hot_ranges` (§4)。選ばれたブロックは
  `carve_new_hots` が `new_hots` に詰める
- `nr_pairs[idx_base]`, `cold_loads[idx_base]` 更新

---

## 6. `incremental_repartition`

### Semantics

前バッチの状態を残したまま、**統計的閾値を超える負荷の偏りが観測された
ときだけ hot を付け替える軽量経路**。返り値は `Balanced` で、`new_hots` に
収まらない場合 (§3) と、トリガが立ったのに `param.enable_incremental == false`
の場合は `Balanced::No` を返す。

### Implementation (フロー)

1. **Phase 1: トリガ判定と serialize 対象の選定**
   - goal は 2 系統:
     - `cold_cnt_goal` — §1.3 の `cold_endpoint_cnt_goal` と同係数だが
       routed fragment 本数用 (`W` を乗せない)
     - `hot_cnt_goal = hot_load_goal(nr_queries)` = `ceil(more_hotness * 2 * nr_queries / P)` —
       hot split の下限。query 種によらず係数 2 固定
   - threshold は
     `overload_threshold.threshold_for(nr_queries, goal, family)` で導出
     (`util/inc/overload_threshold.hpp`)。`OverloadThresholdSpec` は
     `HighWatermarkRatio{r}` (threshold = goal × r; デフォルト r=1.05,
     CLI `--high-watermark`) か `FalsePositiveRate` (Bernstein 閾値,
     CLI `--fp-rate`) の 2 択。後者だけ
     `family = nr_base_parts + 既存 hot 数` の Bonferroni 補正を受ける
   - **トリガと対象選定の分離**: threshold 超過 (`trigger_cold` /
     `trigger_hot`) は「今バッチで rebalancing を起動するか」の判定で、
     どれか 1 DPU でも立てば起動する。実際に serialize する対象は goal
     超過 (`do_cold` / `do_hot`) で選ぶ。トリガより緩い条件なので、
     起動時には goal を超えただけの DPU もまとめて処理される
   - hot 側の条件: `trigger_hot` = `param.enable_hot_split` ∧ hot が 1 chunk より
     大きい ∧ `!hot_split_failed[dpu]` ∧ `nr_qrys > hot_cnt_threshold`。
     `do_hot` は同じで、threshold を goal に置き換え、`hot_split_failed` を見ない。
     つまり `hot_split_failed` (§4、§6.2) が立った hot は自分から再分割を起こさないが、
     他の DPU が起こしたバッチでは再検査される
   - `do_cold` の DPU は `cut_points_of_cold_tree()` で自分の cold
     partition 群を走査し、その境目を `incision_keys[]` に、各 partition
     の鍵範囲を `cold_key_ranges[]` に記録して `TASK_SERIALIZE` を設定。
     `do_cold` / `do_hot` はヘッダに記録し、以後のホスト側の判定はこの 2 つの
     フラグだけを見る (`task_no` は DPU への命令)
   - トリガが立っていて `param.enable_incremental == false` なら
     `Balanced::No` を返す
   - 全 DPU 走査後、トリガ無しなら `Balanced::Yes` で early return
2. **Phase 2: serialize 実行 / 回収 / cold range 再構築**
   - 各対象 DPU に `TASK_SERIALIZE` を gather + execute
   - `incision_indices[]` も同時回収
   - `do_cold` の DPU: KV pairs を `chunked_cold_ranges[]` に詰め直し、
     `chunked_cold_ranges_lists[idx_dpu]` として再連結
   - `do_hot` の DPU: hot の pair 列を `hot_ranges[idx_dpu]` に回収
3. **Phase 3: 2 つの並列 pass**
   - **cold pass**: `parallel_run(&incremental_repartition_worker_cold)`
     (§6.1)。直後に §3 の check #1
   - **hot pass**: `parallel_run(&incremental_repartition_worker_hot)`
     (§6.2)。直後に §3 の check #2。hot pass は `new_hots` に触れない
4. **hot split 計画の確定**: `hot_split_plans[idx_dpu]` が非空の DPU に
   ついて、piece[0] を `kept_hot[idx_dpu]` に残置予約、pieces[1..] を
   `new_hots[hot_count++]` に積む
5. `hot_count == 0` なら `Balanced::Yes` で return
6. `new_hots` を低負荷 DPU に割当。既に hot を持つ DPU (split して
   piece[0] を残す DPU 含む) は候補から除外した上で `partial_sort`。割当後、kept piece[0] を
   元 DPU に再挿入する。左端のデータを既に持っているので移動なしで hot 木
   を再構築できる
7. 据え置きの hot・kept piece[0]・新規割当の hot を `hot_entries[]` に
   集めて鍵順に並べ、`rebuild_parts()` で `parts` を作り直す。各 DPU の
   `input_header` を `TASK_MOVE_HOT` 用に設定
8. DPU への更新 partition 送信と実行
9. `route_queries()` で再 route し、`relieve_overloaded_hots` (§4) の後 `Balanced::Yes` を返す

### 6.1 cold pass (`incremental_repartition_worker_cold`)

対象は `do_cold` の DPU。それ以外は
`cold_npairs_list = 0` と現負荷の `cold_loads` 記録だけ行う。
`get_next_idx_dpu` は `overflow` (§3) が立ったら新しい DPU を掴まない。各対象 DPU に
ついて:

- **load estimation** (mutex 外・並列): §1.3 末尾の「load estimation の
  近似」参照。残存 cold subrange への帰属判定と `orig_idx` dedup
- `hot_npairs` は §1.3 の式 (`base_npairs` = 現 cold ペア数 + その base 由来の
  既存 hot のペア数)
- `cold_endpoint_cnt <= cold_endpoint_cnt_goal` なら `do_cold` を下ろし、cold は
  据え置く (Phase 1 の fragment 単位の判定を endpoint 単位で覆す唯一の方向; §1.3)。
  point query では両単位が一致するので起きない。その DPU が `do_hot` でもあれば
  hot pass (§6.2) の対象のまま残る (テスト: `bpforest/test/hot_pass_targets.cpp`)
- **hot 選出** (mutex 下): full 側 (§5) と同じ流れ
- `cold_npairs_list[idx_dpu]`, `cold_loads[idx_dpu]` 更新

### 6.2 hot pass — hot partition split 機構 (`incremental_repartition_worker_hot`)

`param.enable_hot_split` (デフォルト on) で有効になる、**過熱した既存
hot partition を複数片に割って捌き直す**機構。対象は `do_hot` の DPU。

- 回収済み hot pair 列 (`hot_ranges[idx_dpu]`) に load estimation。
  単一連続 range なので cold pass のような dedup は不要。range query は
  endpoint が `[hot_begin_key, hot_max_key]` 内のもののみ計上
- 分割数は `nr_target_pieces = measured / hot_load`。2 未満なら split
  せず、`hot_ranges[idx_dpu]` を null に戻して hot を保持 (DPU 側の
  hot 木は据え置き)
- `split_hot_range_equal_load` (`bpforest/inc/split_hot_range.hpp`) が
  chunk 粒度で load をほぼ等分 (`ceil(measured / nr_target_pieces)`
  ずつ) に切る。emit 数は、最後の切れ目の後ろに負荷の無い chunk が残ると
  目標より 1 個多く、細分不能な単一巨大 chunk があると目標より少ない。1 個なら
  split を断念して hot を保持し、docs/host_hot_cache.md の「採用」を試み、採用に
  至らなければ `hot_split_failed` を立てる
- pieces は `hot_split_plans[idx_dpu]` に記録し、`nr_new_pieces` に
  `emit_count - 1` を加算する

### バッチ内の実行順 (batch_get など)

```
BPForest::batch_get(nr_queries, keys, results)
  ├─ route_queries(...)
  ├─ repartition(...)                          // param.enable_dynamic_repartition のとき
  │    ├─ incremental_repartition(...)
  │    │    ├─ Phase 1: トリガ判定 + serialize 対象選定 (トリガ無しなら Balanced::Yes)
  │    │    ├─ Phase 2: TASK_SERIALIZE で KV を CPU 回収
  │    │    ├─ Phase 3: cold pass → hot pass
  │    │    │           └─ new_hots に収まらなければ Balanced::No
  │    │    ├─ new_hots を低負荷 DPU に割当 (partial_sort)、kept piece[0] 再挿入
  │    │    ├─ TASK_MOVE_HOT 送信 → DPU 側で木を構築
  │    │    ├─ route_queries() で再 route
  │    │    └─ relieve_overloaded_hots() (採用があればもう一度 route_queries())
  │    └─ Balanced::No なら full_repartition(...)
  ├─ execute_in_dpus()                         // query 実行
  └─ postprocess_of_get()                      // 結果収集
```

他のバッチ操作 (§1.1) も同じ流れ (routed データと postprocess が置き換わるだけ)。

---

## 7. コスト感 (body.tex §"Expense of Full Rebalancing")

- Full rebalancing の大半は KV ペア移動 (~3/4)。partitioning 計算自体は <0.8%
- Full rebalancing 1 回 ≈ batched range query 100 回ぶん
- Incremental は Full よりはるかに安いが、bailout で Full に落ちると
  そのコストを被る

---

## 8. 再実装時の安全チェックポイント

rebalancing を bpforest.ipp の外に再実装するときは、§3 の不変条件と
残り枠の検査、§4 の snapshot 設計 (`DataChunkIterator` が range の node を
live 参照しないこと) を保つこと。
