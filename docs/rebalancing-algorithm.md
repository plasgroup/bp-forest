Rebalancing Algorithm
===

BPForest の rebalancing (クエリ負荷を平滑化するための hot range 抽出と
再配置) の仕組みを、**意味 → 実装** の順に整理したドキュメント。

コード source of truth: `host/inc/bpforest.ipp`
理論対応: 論文の §"Query Density-Driven Partitioning" と appendix
関連参照ドキュメント:

- `docs/pairs-range.md` — 型と capability
- `docs/hot-range-finding/find_absolutely_hot_ranges_naive.hpp` / `..._sliding_window.hpp` — Algorithm 2 の愚直版 → 最適化版の対応
- `docs/hot-range-finding/find_relatively_hot_ranges_naive.hpp` / `..._sliding_window.hpp` / `..._opt1..opt6_*.hpp` — Algorithm 3 の愚直版 → bpforest.ipp 相当までの段階的最適化

---

## 1. Semantics

### 1.1 rebalancing が担うもの

各 DPU は base partition を 1 つ持ち、その中にクエリ負荷の高い subrange
(= hot range) が見つかれば、それを**抜き出して別 DPU に移送**することで
DPU 間のクエリ負荷を均す。論文では "Query Density-Driven Partitioning"
と呼ぶ (body.tex)。

- 入力: 今バッチのクエリ (keys / range queries) と各 DPU の現 partition
- 出力: 更新後の `delims`, `hot_ranges`, `hot_delims` と、DPU に送る `TASK_MOVE_HOT` 指示
- 性質:
  - hot は cold から取り除かれるので "複製" ではなく **移動**
  - 高負荷な base partition の中から hot を最大 $P$ (= DPU 数) 個まで
    抜き出せることが論文で示される (§3 不変条件)

呼び出し位置:

- `BPForest::batch_get` / `batch_pred` / `batch_range_count` /
  `batch_range_max` が `route_queries` 直後に `repartition()` ラッパを呼ぶ
- `repartition()` は `param.enable_dynamic_repartition` (CLI
  `--dynamic-repartition`, デフォルト on) のときだけ動く。まず
  `incremental_repartition` を試し、返り値が `Balanced::No` なら
  `full_repartition` に escalate する
- 初期構築系 (`partition_with_get_batch` など) は直接 `full_repartition`

### 1.2 2 つの経路

| 経路 | 意味 | いつ走るか |
|---|---|---|
| `full_repartition` | 全 KV ペアを CPU に回収して partition を組み直す重い経路 | 初期構築時、および incremental が `Balanced::No` を返して `repartition()` が escalate した時 |
| `incremental_repartition` | 前バッチの状態を活かして負荷が偏った DPU だけ touch する軽い経路 | 定常バッチ処理。不変条件を破りそうな場合は `Balanced::No` を返し、full 行きを呼出し側に委ねる |

どちらも、各 base partition 内で「hot range を抜き出す → 残余 cold を
保持」という同一の semantics を持つ。違いは "どこまで全体を作り直すか"。

### 1.3 用語と記号

1 バッチ内のクエリは point-only か range-only のいずれかで、混在しない。
range query の負荷は **begin/end の 2 つの endpoint への point query と
して近似** する (1 本 = 2 本相当, $W = 2$)。以降の式はこの近似を前提に
している。

| 論文 | コード | 意味 |
|---|---|---|
| $P$ | `nr_base_parts` | DPU 数 (= base partition 数) |
| $Q$ (論文) / $Q \cdot W$ (コード) | `nr_queries * W`, $W \in \{1, 2\}$ | クエリ負荷の総和。point=1, range=2 の重み付け近似 |
| $D$ | 総 KV ペア数 | 論文はバイト数、コードはペア件数 |
| $Q/P$ / $\lceil Q/P \rceil$ | `hot_load` | 1 hot range の最小クエリ負荷 (絶対閾値) |
| $\alpha$ | `param.balancing` | balancing factor |
| $\dfrac{1}{\alpha}\dfrac{D}{P}$ | `hot_npairs` | 1 hot range の目標ペア数 (= sliding window の幅閾値) |
| $\beta$ | `nr_relative_hots` | Algorithm 3 (double scan) で切り出す hot 個数の上限 |
| — | `param.more_hotness` | `hot_load` と `cold_endpoint_cnt_goal` に乗る倍率 (CLI `--more-hot`, デフォルト 1)。大きいほど閾値が上がり hot が切り出されにくくなる |
| — | `param.greedy_only` | 真なら Algorithm 3 (relative phase) を丸ごとスキップし、`cold_endpoint_cnt_goal` も greedy 専用式に切り替える (CLI `--greedy-only`) |

代表的な式 (`full_repartition_worker` / `incremental_repartition_worker_cold`
の冒頭; 両者同式):

```cpp
hot_load               = (more_hotness * nr_queries * W + nr_base_parts - 1) / nr_base_parts;
cold_endpoint_cnt_goal = greedy_only
                             ? more_hotness * nr_queries * W * balancing / nr_base_parts
                             : more_hotness * nr_queries * W * max(3, balancing + 1) / 3 / nr_base_parts;

// full_repartition_worker: 対象 base の cold ペア数から
hot_npairs       = (cold_npairs + param.balancing - 1) / param.balancing;
// incremental_repartition_worker_cold: cold + 既存 hot を合算した base ペア数から
hot_npairs       = (base_npairs + param.balancing - 1) / param.balancing;

// 両 worker の relative phase 直前
nr_relative_hots = cold_endpoint_cnt / hot_load;  // 論文の β
```

`cold_endpoint_cnt_goal` の係数 $\max(3, \alpha+1)/3$ は論文 appendix
の定理「cold 側クエリ負荷の上界 $\le \frac{Q}{P}\max\{\frac{\alpha+1}{3}, 1\}$」
(Alg.3 double-scan 後) に対応。$\alpha \le 2$ では係数が 1 に張り付く。
`param.greedy_only` のときは double-scan という定理の前提が消えるので、
goal は $\alpha \cdot QW/P$ (係数 $\alpha$) の greedy 専用式に切り替わる。
いずれの式も `param.more_hotness` 倍される。

**Phase 1 と Phase 3 の比較対象は意図的に別単位** (incremental 経路):

- Phase 1 (`incremental_repartition` 冒頭の pre-filter) の `cold_cnt_goal`
  は **combined_delim 境界で split された cold partial 本数**
  (`routed.cold[d].nr_qrys`; `W` を乗せない) と比較
- Phase 3 (`incremental_repartition_worker_cold`) の
  `cold_endpoint_cnt_goal` は **原 range query の begin/end endpoint の
  うち cold subrange に落ちた本数** と比較 (`W=2`)

両者は単位も係数も違うので直接比較できない。Phase 1 の起動判定は goal
そのものではなく、`OverloadThreshold` が goal から導く threshold (§7
Phase 1) との比較で行う。threshold は goal より大きい側に張られる safety
margin であり、Phase 1 で `TASK_NONE` と判定された DPU は Phase 3 の
endpoint ベース精査には回らない (Phase 3 は Phase 1 の `TASK_SERIALIZE`
群を `TASK_NONE` に戻す方向にしか動かない; §7.1 の goal 判定)。
false-negative の発生は仕様。

**load estimation の近似**: range query の実際の交差 chunk 数は無視され、
begin/end の各 endpoint が対象範囲に落ちた場合のみ chunk の `load()` に
+1 する。full 側 (`full_repartition_worker` の load estimation ブロック)
は `[base_min, base_max]` で判定、incremental 側
(`incremental_repartition_worker_cold`) は `cold_key_ranges` から作った
delimiter 列で「残存 cold subrange に落ちた endpoint」だけ計上し、同一
query が同じ DPU の複数 fragment に routed されていても `orig_idx` で
dedup する。対象範囲の外に飛び出した endpoint は計上されない。

---

## 2. データ構造

詳細は `docs/pairs-range.md` 参照。ここでは rebalancing 文脈での役割のみ:

- `PairsRange` / `ChunkedPairsRange` — cold subrange を表す非所有ビュー
- `DataChunkIterator` — 256 ペア単位の chunk iterator。sliding window 走査の単位
- `NewHotRange = {PairsRange, KeyRange, load}` — 抜き出した hot のスロット
- `LinkedList<ChunkedPairsRange>` — 各 DPU の cold range 集合。hot 抜出しに伴う分割を erase/insert で扱う (iterator stable)
- `BPForest::new_hots` — `ExtendableBuffer<NewHotRange>{nr_base_parts}` の **固定サイズバッファ**。後述の不変条件を前提にしている
- `BPForest::kept_hot` / `BPForest::hot_split_plans` — hot partition split (§7.2) の中間データ。split 元 DPU に残す piece[0] と、再配置する pieces[1..] の受け渡しに使う

---

## 3. 不変条件 (最重要)

**命題:** `full_repartition` / `incremental_repartition` が 1 回走り
切ったとき、新たに切り出される hot range 総数 `hot_count` は
`nr_base_parts` (= $P$) を超えない。

**論文の証明 (appendix.tex, Theorem):**

- **Algorithm 2 (Greedy):** 各 hot range は `hot_load` $\ge Q/P$ を
  超えてから emit されるので、greedy 全体で切り出せるのは $Q/(Q/P) = P$ 個以下
- **Algorithm 3 (Double-scan):** 呼び出しごとに $\beta_b = \lfloor (\text{残クエリ数})/(Q/P) \rfloor$ 以下に制限され、
  base partition 合計も greedy 残余クエリ数$/(Q/P)$ で抑えられる
- 合わせても $P$ 以下

**コード側の担保:**

- `new_hots` は固定サイズ確保 (bpforest.hpp のメンバ定義)、
  `new_hots[hot_count++]` で index 書込 (`full_repartition_worker` /
  `incremental_repartition_worker_cold` の absolute・relative 両 callback)。
  容量 check なし — 不変条件を**信じて**書いている
- `incremental_repartition` には**事後**の bailout が 2 箇所ある:
  ```cpp
  // cold pass (incremental_repartition_worker_cold) の直後
  if (tmp_data.nr_existing_hots + tmp_data.hot_count > nr_base_parts) {
      return Balanced::No;
  }
  // hot pass (incremental_repartition_worker_hot) の直後: split 由来の新片も勘定
  if (tmp_data.nr_existing_hots + tmp_data.hot_count + tmp_data.nr_new_pieces > nr_base_parts) {
      return Balanced::No;
  }
  ```
  `Balanced::No` を受けた呼出し側 `repartition()` が `full_repartition`
  へ escalate する (§1.1)。hot split の pieces[1..] は 2 つ目の check を
  通過した後で初めて `new_hots` に書き込まれるので固定サイズバッファは
  溢れないが、cold 側の書込は事後検証であり、バッチ内 `hot_count` 単独の
  上限は依然として論文の定理にのみ依存する
- さらに incremental の両 worker (`incremental_repartition_worker_cold` /
  `_hot`) の `get_next_idx_dpu` に**走査中ガード**があり、
  `nr_existing_hots + hot_count > nr_base_parts` に達した時点で新しい
  DPU を掴まなくなる (走査途中の DPU は完走する)
- 別実装 (`util/eval_rebalancing`) は `std::vector` + `reserve()` で
  growable だが、§9 の 3 点を満たさないと同等の安全性は得られない

---

## 4. `find_absolutely_hot_ranges` (Algorithm 2)

### Semantics

`[begin_part, end_part)` の cold range 列を受け取り、各 range 上で
sliding window の右端を 1 chunk ずつ伸ばし、窓内 pair 数が `hot_npairs`
を大きく超えないように「left chunk を 1 つ外すと `hot_npairs` を下回る」
限界まで left 側を前進させながら、窓内 load が `hot_nqrys` に到達した
瞬間に hot range を emit して窓を reset する。したがって emit 幅は load
が閾値に達するまでに必要だった幅で決まり、密度が高ければ `hot_npairs`
未満、低ければ `hot_npairs` に chunk 1 つ分未満の余剰を乗せた程度に
収まる。窓は range を跨がない (range が替わると reset)。

callback 規約: `bool hot_hook(part, left, right, load)` が `false` を
返せば即中断。callback 側は `part` を `{end, part.end()}` に縮めて hot
を取り除き、`new_hots[hot_count++]` に記録する (呼出し側 = §6 / §7.1 の
worker)。

呼び出し方は経路で異なる: `full_repartition_worker` は担当 base 1 つを
`(&base, &base + 1)` の単一 range として渡し、
`incremental_repartition_worker_cold` は対象 DPU の cold range list 全体
(既存 hot で分断された複数 range; `chunked_cold_ranges` プール上で連続)
を渡す。

### Implementation 段階

愚直版 → 最適化版は以下の 2 ファイルで段階を追える:

- `docs/hot-range-finding/find_absolutely_hot_ranges_naive.hpp` — Alg.2 の逐語実装。各 $r$
  で `l'` 候補を右→左に全走査し、`SizeOf` / `NQrys` を毎回積み直す
- `docs/hot-range-finding/find_absolutely_hot_ranges_sliding_window.hpp` — `window_npairs` /
  `window_load` を保持し、$r$ の前進で加算、$l$ の前進で減算することで
  各集約値を amortized $O(1)$ に。これが bpforest.ipp の
  `find_absolutely_hot_ranges` に対応

### callback 内 mutation の安全性

`DataChunkIterator` は `part_begin/part_end/cursor/p_load` を値として
保持し (pairs_range.hpp の `DataChunkIterator` メンバ)、
`part->begin()/end()` を live 参照しない。
加えて、ループ終端 `end_chunk` は各 part の走査開始時に `part->end()`
の snapshot を取っている。

この 2 つの独立した保証により、callback が `part` を
`{end, part.end()}` に縮めても関数内のローカル `left/right/end_chunk`
は壊れない。移植時に `ChunkedPairsRange` を list iterator 越しの live
reference 型に置き換えると両方壊れうる。eval_rebalancing 側で類似 bug
(callback 内 mutation → 隣接ヒープ破壊) の実績あり。

---

## 5. `find_relatively_hot_ranges` (Algorithm 3 第2スキャン)

### Semantics

Greedy pass で取り切れなかった残り cold range 集合に対し、**`nr_hots`
個ぴったり分の hot range を carve-out できる "最大荷重ウィンドウ"**
を見つけ、そのウィンドウを右→左に `hot_npairs` 単位で束ねて emit する
2 フェーズ処理。`param.greedy_only` が真の場合はこの relative phase
自体が呼ばれない (`full_repartition_worker` /
`incremental_repartition_worker_cold` の relative 段が
`!param.greedy_only` でゲートされている)。

- **入力**: `[begin_range, end_range)` — cold range list の半開区間。
  hot はすでに走査空間から消えているので、ウィンドウは cold range を
  **不連続に跨ぐ**
- **出力**: argmax ウィンドウの `[left, right)` 境界 (callback が途中で
  stop を返せばその時点の split 位置)。呼出し側はこの 2 つの境界から
  cold 残余を再構成する

#### Phase 1: argmax スキャン

ウィンドウ `[l, r)` を右に伸ばしつつ、各 $r$ で *window 容量が丁度
`nr_hots` 個の hot range に carve 可能な最大幅* を満たす最右の $l'$ を
取り、`load(l, r)` の最大を記録する。容量判定には論文の `SizeOf` を
cold-range-list に拡張した **effective pair count** を使う:

```
effective_window_pair_count(l, r)
  = raw_npairs(left_partial)
  + Σ slot_rounded_npairs(middle_range)
  + slot_rounded_npairs(right_partial)
where slot_rounded_npairs(n) = ceil(n / hot_npairs) * hot_npairs
```

**なぜ left partial だけ raw か**: carve が range 全体 (middle) や
range 先頭から始まる (right partial) ケースでは `hot_npairs` 単位で
消費するので slot_rounded で測るのが自然。一方、左端は "最後に足した
chunk が hot 1 個分の余剰をどれだけ運ぶか" を chunk 粒度で見たいので
raw。`l_range == r_range` のとき式は raw に退化し、単一 range 版
(`single_range_naive.hpp`) の条件と一致する。

**境界例外**: `cand` が `cand_range->begin()` に達した瞬間、cand_range
全体が left partial になる。cand_range が undersize (raw < slot_rounded)
だと raw 比較ではまだ条件を満たさないが、ここで止めないと次 step で
cand_range が "完全通過 middle" に格下げされ、carve 数が `nr_hots + α`
に膨らむ。**`slot_rounded 下の effective_npairs == required_npairs`
が成立する瞬間だけ例外採用** することで、carve は丁度 `nr_hots` 個で
終わる。`>` のケースは構造的に存在しない (raw 比較で先に検出される)
ので救済不要、`<` は容量不足で採用不可 (証明は
`find_relatively_hot_ranges_naive.hpp` ヘッダ)。

#### Phase 2: carve-out

`[argmax_left, argmax_right)` を右→左に逆走し、`hot_npairs` ペアずつ
束ねて hook に渡す。cold range 境界を跨ぐ時は 1 range ごとに callback
を呼び直す (callback の第 1 引数が `ChunkedPairsRange&` なので)。

論文対応: `body.tex` Algorithm 3 "Double-scan hot range selection"。

### Implementation 段階

`docs/hot-range-finding/find_relatively_hot_ranges_*.hpp` に愚直版 → bpforest.ipp 相当
までの段階的最適化を配置している。学習者はこの順に読むと実装上の
最適化を 1 つずつ追える:

| ファイル | 追加される最適化 |
|---|---|
| `naive.hpp` | Alg.3 の逐語実装 (`effective_window_pair_count` と境界例外を毎回 $O(N)$ で計算) |
| `single_range_naive.hpp` | 単一 cold range 前提の愚直版 (意味論差分なし、論文そのまま) |
| `sliding_window.hpp` | $l$ を単調前進させる 2 ポインタ化。状態変数 (`left_npairs_offcut`, `right_npairs_offcut`, `right_window_offcut`, `non_left_effective`) で `effective_window_pair_count` を差分更新 |
| `opt1_bootstrap_whole_range_advance.hpp` | メインループ前に right を cold range 単位で bulk 進める初期化 |
| `opt2_bootstrap_all_hot_early_exit.hpp` | bootstrap で全 range 吸収なら argmax 確定で carve へ直行 |
| `opt3_single_range_steady_state.hpp` | `left_range == right_range` 用の専用 fast path |
| `opt4_right_quota_threshold_gate.hpp` | 右端 rounded 幅の再計算・左縮小・margin 圧縮を `right_npairs_offcut > right_window_offcut` の成立時だけに遅延 |
| `opt5_right_margin_compression.hpp` | 右 rounded 幅が増えた直後、同 range 内で right を先取りして margin を最小化 |
| `opt6_range_level_left_shrink.hpp` | 左縮小を cold range 単位 (bulk) + 最終 range 内の chunk 単位に分解。opt6 で bpforest.ipp の `find_relatively_hot_ranges` と semantically equivalent |

**opt6 ↔ bpforest.ipp 乖離監視ポイント**: 以下 5 箇所は現時点で一致
しているが、bpforest.ipp 側だけ変更されると silent に乖離しうる
(いずれも bpforest.ipp `find_relatively_hot_ranges` のメインループ内):

1. 閾値ゲート条件 `right_npairs_offcut > right_window_offcut`
2. right margin compression の `<` 境界
   (`right_npairs_offcut + right->npairs() < right_window_offcut`)
3. range-level left shrink 合流
   (`left_window_offcut <= left_window_offcut_to_shrink` ループ) の
   state 畳み込み
4. final chunk-level left shrink の `>=` 境界
   (`left_npairs_offcut - left->npairs() >= left_window_offcut`)
5. single-range fast path (`left_range == right_range` 分岐) — 別
   invariant 系で乖離が目立たない

---

## 6. `full_repartition`

### Semantics

全 KV ペアを CPU に回収し、DPU 数で等分した base partition 上から
greedy + double-scan で hot を抜き出し、`partial_sort` で低負荷 DPU に
割り当ててから全 DPU の tree を再構築する **一括リビルド** 経路。
論文 Algorithm 1 "Construction of hot/cold partitions" の骨格。

### Implementation (フロー)

hot 切り出しの本体は `full_repartition_worker` に分離され、
`parallel_run` でマルチスレッド実行される (かつての base partition
ごとの直列ループから変更)。ワーカー間は `TmpDataForFullRepartition` の
mutex と共有カウンタ (`idx_base` / `cold_count` / `hot_count`) で排他
する。「`param.balancing > 0` のときだけ hot を切る」ゲートは現行には
無く、worker は常に走る。

`full_repartition` 本体:

1. `retrieve_all_data(data_buf)` で全 DPU から KV ペア pull
2. `delims` / `hot_delims` をクリアし、`chunked_cold_ranges_lists[]` の
   クリアを RAII cleanup として仕込む
3. `data_buf` を `nr_base_parts` 等分、各 base partition を
   `chunked_cold_ranges[]` に詰め、base delim を `delims` に登録
4. `combine_delims()` → `route_queries()` で新 partition に re-route
5. **find_hot 段**: `parallel_run(&full_repartition_worker)` (下記)
6. `hot_count > 0` の場合のみ、`new_hots[0..hot_count)` を低負荷 DPU に
   割当:
   - `partial_sort` で `cold_loads` 前 `hot_count` 個を負荷昇順
   - `new_hots` を load 降順 sort
   - 負荷の低い DPU ← 負荷の高い hot の対応付け、hot delim 登録
   - `combine_delims()` → `route_queries()` で更新後 delim に再 route
7. `initialize_in_dpu(...)` で DPU 側 tree 再構築

`full_repartition_worker` (各スレッド、`get_next_idx_base` で base を
1 つずつ取得):

- **base 取得** (`get_next_idx_base`, mutex 下): point query の場合は
  `routed.cold[idx].nr_qrys <= cold_endpoint_cnt_goal` の base をここで
  スキップし `nr_pairs` / `cold_loads` だけ記録する。range query は
  fragment 本数と endpoint 本数が別単位 (§1.3) なのでここではスキップ
  せず、endpoint 計数後に判定する
- `cold_npairs`, `hot_npairs` 確定
- **load estimation** (mutex 外・並列): `chunk2load[]` を 0 初期化し、
  point は `upper_bound` (predecessor 系は `lower_bound`)、range は両
  endpoint のうち `[base_min, base_max]` 内のもののみ計上
- **absolute 段** (mutex 下; `cold_endpoint_cnt > cold_endpoint_cnt_goal`
  の場合のみ): `find_absolutely_hot_ranges(&base, &base + 1, ...)`。
  callback で `part` を縮め `new_hots[hot_count++]` に詰める。抜出し後
  `base.npairs() == 0` なら list から erase
- **relative 段** (mutex 下; `!param.greedy_only` かつまだ goal 超過):
  `nr_relative_hots = cold_endpoint_cnt / hot_load` を上限として
  `find_relatively_hot_ranges`。返り値の `[left_range, left)` と
  `[right, right_range->end())` から cold 残余を再構成
- `nr_pairs[idx_base]`, `cold_loads[idx_base]` 更新

---

## 7. `incremental_repartition`

### Semantics

前バッチの状態を残したまま、**統計的閾値を超える負荷の偏りが観測された
ときだけ hot を付け替える軽量経路**。Phase 1 でトリガ判定と serialize
対象の選定 → Phase 2 で serialize して CPU に KV を持ち出し → Phase 3 で
cold からの hot 選出 (cold pass) と過熱 hot の分割計画 (hot pass)、の
3 段。返り値は `Balanced` で、不変条件を破りそうな場合と、トリガが立った
のに `param.enable_incremental == false` の場合は `Balanced::No` を返す
— full への escalate は呼出し側 `repartition()` の仕事 (§1.1)。

### Implementation (フロー)

1. **Phase 1: トリガ判定と serialize 対象の選定**
   - goal は 2 系統:
     - `cold_cnt_goal` — §1.3 の `cold_endpoint_cnt_goal` と同係数
       (greedy_only 分岐・`more_hotness` 係数込み) だが routed fragment
       本数用 (`W` を乗せない)
     - `hot_cnt_goal = (more_hotness * 2 * nr_queries + P - 1) / P` —
       hot split の下限。query 種によらず係数 2 固定
   - threshold は
     `overload_threshold.threshold_for(nr_queries, goal, family)` で導出
     (`util/inc/overload_threshold.hpp`)。`OverloadThresholdSpec` は
     `HighWatermarkRatio{r}` (threshold = goal × r; デフォルト r=1.05,
     CLI `--high-watermark`) か `FalsePositiveRate` (Bernstein 閾値,
     CLI `--fp-rate`) の 2 択で、
     `family = nr_base_parts + 既存 hot 数` の Bonferroni 補正を受ける
   - **トリガと対象選定の分離**: threshold 超過 (`trigger_cold` /
     `trigger_hot`) は「今バッチで rebalancing を起動するか」の判定で、
     どれか 1 DPU でも立てば起動する。実際に serialize する対象は goal
     超過 (`do_cold` / `do_hot`) で選ぶ — トリガより緩い条件なので、
     起動時には goal を超えただけの DPU もまとめて処理される
   - `trigger_hot` / `do_hot` は `param.enable_hot_split`
     (CLI `--hot-split`) が前提
   - `do_cold` の DPU は `cold_delims` を走査して既存 hot との境界キーを
     `hot_delim_keys[]`, `cold_key_ranges[]` に記録し `TASK_SERIALIZE`
     を設定
   - トリガが立っていて `param.enable_incremental == false` なら
     `Balanced::No` を返す (呼出し側が full へ)
   - 全 DPU 走査後、トリガ無しなら `Balanced::Yes` で early return
2. **Phase 2: serialize 実行 / 回収 / cold range 再構築**
   - 各対象 DPU に `TASK_SERIALIZE` を gather + execute
   - 回収 pair 数を基に `data_buf.reserve()`。`incision_indices[]` も
     同時回収
   - `do_cold` の DPU: KV pairs を `chunked_cold_ranges[]` に詰め直し、
     `chunked_cold_ranges_lists[idx_dpu]` として再連結
   - `do_hot` の DPU: hot の pair 列を `hot_ranges[idx_dpu]` に回収
3. **Phase 3: 2 つの並列 pass**
   - **cold pass**: `parallel_run(&incremental_repartition_worker_cold)`
     (§7.1)。直後に bailout check #1 (§3) — 超過なら `Balanced::No`
   - **hot pass**: `parallel_run(&incremental_repartition_worker_hot)`
     (§7.2)。直後に bailout check #2 (`nr_new_pieces` 込み; §3) — 超過
     なら `Balanced::No`。hot pass は `delims` / `kept_hot` /
     `hot_delims` / `new_hots` に触れないので、この時点の fallback は
     安全
4. **hot split 計画の確定**: `hot_split_plans[idx_dpu]` が非空の DPU に
   ついて、旧 hot delim を erase し、piece[0] を `kept_hot[idx_dpu]` に
   残置予約、pieces[1..] を `new_hots[hot_count++]` に積む
5. `hot_count == 0` なら `Balanced::Yes` で return
6. `new_hots` を低負荷 DPU に割当。既に hot を持つ DPU (split して
   piece[0] を残す DPU 含む) は `cold_loads[idx].second = UINT32_MAX`
   にして候補から除外した上で `partial_sort`。割当後、kept piece[0] を
   元 DPU に再挿入 — 左端のデータを既に持っているので移動なしで hot 木
   を再構築できる
7. `combine_delims()` 更新、各 DPU の `input_header` を `TASK_MOVE_HOT`
   用に設定
8. DPU への更新 partition 送信と実行
9. `route_queries()` で再 route し、`Balanced::Yes` を返す

### 7.1 cold pass (`incremental_repartition_worker_cold`)

対象は `TASK_SERIALIZE` かつ `do_cold` の DPU。それ以外は
`cold_npairs_list = 0` と現負荷の `cold_loads` 記録だけ行う。
`get_next_idx_dpu` の冒頭には走査中ガード (§3) がある。各対象 DPU に
ついて:

- **load estimation** (mutex 外・並列): §1.3 末尾の「load estimation の
  近似」参照 — 残存 cold subrange への帰属判定と `orig_idx` dedup
- `base_npairs` = 現 cold ペア数 + その base 由来の既存 hot のペア数、
  `hot_npairs = ceil(base_npairs / α)`
- `cold_endpoint_cnt <= cold_endpoint_cnt_goal` なら `TASK_NONE` に戻す
  (Phase 1 の粗い判定を endpoint 単位の精査で覆す唯一の方向; §1.3)
- **absolute 段** (mutex 下): `find_absolutely_hot_ranges(begin_part,
  end_part, ...)` に DPU の cold range list 全体を渡す (§4)。callback
  は cold の分割に合わせて `cold_key_ranges` の境界も更新する
- **relative 段** (mutex 下; `!param.greedy_only` かつまだ goal 超過):
  full 側 (§6) と同じ流れ
- `cold_npairs_list[idx_dpu]`, `cold_loads[idx_dpu]` 更新

### 7.2 hot pass — hot partition split 機構 (`incremental_repartition_worker_hot`)

`param.enable_hot_split` (デフォルト on) で有効になる、**過熱した既存
hot partition を複数片に割って捌き直す**機構。対象は `TASK_SERIALIZE`
かつ `do_hot` の DPU。

- 回収済み hot pair 列 (`hot_ranges[idx_dpu]`) に load estimation。
  単一連続 range なので cold pass のような dedup は不要。range query は
  endpoint が `[hot_begin_key, hot_max_key]` 内のもののみ計上
- 分割数は `nr_target_pieces = measured / hot_load`。2 未満なら split
  せず、`hot_ranges[idx_dpu]` を null に戻して hot を保持 (DPU 側の
  hot 木は据え置き)
- `split_hot_range_equal_load` (`host/inc/split_hot_range.hpp`) が
  chunk 粒度で load をほぼ等分 (`ceil(measured / nr_target_pieces)`
  ずつ) に切る。細分不能な単一巨大 chunk があると emit 数は目標より
  減り、1 個なら split を断念して hot を保持
- pieces は `hot_split_plans[idx_dpu]` に記録し、`nr_new_pieces` に
  `emit_count - 1` を加算するだけ — `delims` / `new_hots` への反映は
  bailout check #2 の通過後に本体が行う (piece[0] は `kept_hot` として
  元 DPU に残置、pieces[1..] は `new_hots` 経由で再配置)
- 関連メンバは `hot_stage1_fired` / `kept_hot` / `hot_split_plans`
  (bpforest.hpp)。`hot_stage1_fired` は Phase 1 の `do_hot` 判定の記録
  で、現行コードでは書き込みのみ

### バッチ間の実行順 (batch_get など)

```
BPForest::batch_get(nr_queries, keys, results)
  ├─ route_queries(...)
  ├─ repartition(...)                          // param.enable_dynamic_repartition のとき
  │    ├─ incremental_repartition(...)
  │    │    ├─ Phase 1: トリガ判定 + serialize 対象選定 (トリガ無しなら Balanced::Yes)
  │    │    ├─ Phase 2: TASK_SERIALIZE で KV を CPU 回収
  │    │    ├─ Phase 3: cold pass → hot pass
  │    │    │           └─ 不変条件超過なら Balanced::No
  │    │    ├─ new_hots を低負荷 DPU に割当 (partial_sort)、kept piece[0] 再挿入
  │    │    ├─ TASK_MOVE_HOT 送信 → DPU 側で木を構築
  │    │    └─ route_queries() で再 route
  │    └─ Balanced::No なら full_repartition(...)
  ├─ execute_in_dpus()                         // query 実行
  └─ postprocess_of_get()                      // 結果収集
```

`batch_pred` / `batch_range_count` / `batch_range_max` も同じ流れ
(routed データと postprocess が置き換わるだけ)。

---

## 8. コスト感 (body.tex §"Expense of Full Rebalancing")

- Full rebalancing の大半は KV ペア移動 (~3/4)。partitioning 計算自体は <0.8%
- Full rebalancing 1 回 ≈ batched range query 100 回ぶん
- Incremental は Full よりはるかに安いが、bailout で Full に落ちると
  そのコストを被る

---

## 9. `util/eval_rebalancing` 対応表

`eval_rebalancing/src/main.cpp` は DPU を使わずに rebalancing 相当の
状態遷移を再現する独立シミュレータ。bpforest.ipp の該当関数と 1 対 1
対応を念頭に書かれているが、データ構造は専用の再実装。

| bpforest.ipp | eval_rebalancing/src/main.cpp |
|---|---|
| `find_absolutely_hot_ranges` | 同名の自由関数 (docs/hot-range-finding の sliding_window 版の直写し) |
| `find_relatively_hot_ranges` | 同名の自由関数 (同上) |
| `full_repartition` + `full_repartition_worker` (並列) | `full_repartition` (直列) |
| `incremental_repartition` + worker_cold / worker_hot (並列) | `rebalancing` (直列; hot split 相当は未移植) |
| `new_hots` = `ExtendableBuffer<NewHotRange>{nr_base_parts}` (固定サイズ) | `std::vector<SrcNewHotRange>` + `reserve(ndpus)` (`full_repartition` / `rebalancing` それぞれの冒頭; growable) |
| incremental bailout: `Balanced::No` を返し `repartition()` が full へ | `rebalancing` の DPU ループ内の `nr_existing_hots + new_hots.size() > ndpus` 判定で直接 `full_repartition` を呼ぶ |
| `chunked_cold_ranges_lists` | 専用の `std::list<ChunkedPairsRange>`-相当 |

パラメータ面の差分: シミュレータの pre-filter も `OverloadThreshold`
(`threshold_for`) を使うが family は `ndpus` 固定 (既存 hot 数の
Bonferroni 補正なし)。`more_hotness` / `greedy_only` / hot split は
未反映。

現行 simulator の `new_hots` は growable なので即 heap overwrite には
ならないが、§3 の不変条件が論理的に崩れた移植を書くと次段で別経路の
境界を踏みうる。crash 調査時に最初に確認すべきポイント:

1. `find_absolutely_hot_ranges` の callback 内 mutation が関数のループ
   変数に影響しない snapshot 設計になっているか (`DataChunkIterator`
   相当の型が `pair_begin` を live 参照していないか)
2. callback 1 回あたりの emit 個数 × 全 base partition の合計が `ndpus`
   を超えない不変条件が、移植版でも同じ証明で成立するか
3. incremental 相当の bailout (`incremental_repartition` の 2 段の
   `Balanced::No` check; §3) と同じ check がシミュレータ側にあるか
