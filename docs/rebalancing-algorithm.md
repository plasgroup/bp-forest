Rebalancing Algorithm
===

BPForest の rebalancing (クエリ負荷を平滑化するための hot range 抽出と
再配置) の仕組みを、**意味 → 実装** の順に整理したドキュメント。
将来この repository で作業する AI が読み直すときの参照点。

コード source of truth: `host/inc/bpforest.ipp`
理論対応: 論文 (`~/Documents/hpdc2026/paper/`, `body.tex` §"Query Density-Driven Partitioning", `appendix.tex`)
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
    抜き出せることが論文で示される (§1.4 不変条件)

呼び出し位置:

- `BPForest::batch_get` / `BPForest::batch_range_count` の先頭で
  `incremental_repartition` を呼ぶ (`param.balancing > 0` のとき)
- 初期構築系 (`partition_with_get_batch` など) は直接 `full_repartition`
- `param.balancing == 0` は rebalancing off (関数先頭で即 return)

### 1.2 2 つの経路

| 経路 | 意味 | いつ走るか |
|---|---|---|
| `full_repartition` | 全 KV ペアを CPU に回収して partition を組み直す重い経路 | 初期構築時、および incremental の bailout 時 |
| `incremental_repartition` | 前バッチの状態を活かして負荷が偏った DPU だけ touch する軽い経路 | 定常バッチ処理。不変条件を破りそうなら内部で `full_repartition` に fallback |

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

代表的な式 (bpforest.ipp 原文):

```cpp
// L1442-1443 (full_repartition), L1839-1840 (incremental)
hot_load               = (nr_queries * W + nr_base_parts - 1) / nr_base_parts;
cold_endpoint_cnt_goal =  nr_queries * W * max(3, param.balancing + 1) / 3 / nr_base_parts;

// L1449 (full) / L1957 (incremental)
hot_npairs       = (cold_npairs + param.balancing - 1) / param.balancing;  // full: cold_npairs
// hot_npairs    = (base_npairs + param.balancing - 1) / param.balancing;   // incremental: base_npairs

// L1534 / L2010
nr_relative_hots = cold_endpoint_cnt / hot_load;  // 論文の β
```

`cold_endpoint_cnt_goal` の係数 $\max(3, \alpha+1)/3$ は論文 appendix
の定理「cold 側クエリ負荷の上界 $\le \frac{Q}{P}\max\{\frac{\alpha+1}{3}, 1\}$」
(Alg.3 double-scan 後) に対応。$\alpha \le 2$ では係数が 1 に張り付く。

**Phase 1 と Phase 3 の比較対象は意図的に別単位** (incremental 経路):

- Phase 1 (bpforest.ipp:1701-1702) の `cold_partial_cnt_goal` は
  **combined_delim 境界で split された cold partial 本数** (`W` を乗せない) と比較
- Phase 3 (L1840) の `cold_endpoint_cnt_goal` は **原 range query の
  begin/end endpoint のうち cold subrange に落ちた本数** と比較 (`W=2`)

両者は単位も係数も違うので直接比較できない。`high_watermark_ratio` は
Phase 1 の skip をさらに緩めて rebalance 対象を意図的に減らす safety
margin であり、Phase 1 で `TASK_NONE` と判定された DPU は Phase 3 の
endpoint ベース精査には回らない (Phase 3 は Phase 1 の `TASK_SERIALIZE`
群を `TASK_NONE` に戻す方向にしか動かない; L1958-1963)。false-negative
の発生は仕様。

**load estimation の近似**: range query の実際の交差 chunk 数は無視
され、両 endpoint が `[base_min, base_max]` に落ちた場合のみ chunk の
`load()` に +1 する (full: L1486-1502, incremental: L1894-1938)。base の
外に飛び出した endpoint は cold 側に計上されない。

---

## 2. データ構造

詳細は `docs/pairs-range.md` 参照。ここでは rebalancing 文脈での役割のみ:

- `PairsRange` / `ChunkedPairsRange` — cold subrange を表す非所有ビュー
- `DataChunkIterator` — 256 ペア単位の chunk iterator。sliding window 走査の単位
- `NewHotRange = {PairsRange, KeyRange, load}` — 抜き出した hot のスロット
- `LinkedList<ChunkedPairsRange>` — 各 DPU の cold range 集合。hot 抜出しに伴う分割を erase/insert で扱う (iterator stable)
- `BPForest::new_hots` — `ExtendableBuffer<NewHotRange>{nr_base_parts}` の **固定サイズバッファ**。後述の不変条件を前提にしている

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

- `new_hots` は固定サイズ確保 (bpforest.hpp:189)、`new_hots[hot_count++]` で index 書込 (bpforest.ipp:1517, 1541, 1989, 2019)。容量 check なし — 不変条件を**信じて**書いている
- `incremental_repartition` にだけ**事後**の bailout (L2073-2076):
  ```cpp
  if (nr_existing_hots + hot_count > nr_base_parts) {
      raii.finalize();
      return full_repartition(nr_queries, queries, results, routed);
  }
  ```
  既に書込済みバッファを検証するだけで、`hot_count` 単独の上限は依然と
  して論文の定理にのみ依存する
- 別実装 (`util/eval_rebalancing`) は `std::vector` + `reserve()` で
  growable だが、§8 の 3 点を満たさないと同等の安全性は得られない

---

## 4. `find_absolutely_hot_ranges` (Algorithm 2)

### Semantics

各 base partition 上で sliding window の右端を 1 chunk ずつ伸ばし、
窓内 pair 数が `hot_npairs` を大きく超えないように「left chunk を 1 つ
外すと `hot_npairs` を下回る」限界まで left 側を前進させながら、窓内
load が `hot_nqrys` に到達した瞬間に hot range を emit して窓を reset
する。したがって emit 幅は load が閾値に達するまでに必要だった幅で
決まり、密度が高ければ `hot_npairs` 未満、低ければ `hot_npairs` に
chunk 1 つ分未満の余剰を乗せた程度に収まる。

callback 規約: `bool hot_hook(part, left, right, load)` が `false` を
返せば即中断。callback 側は `part` を `{end, part.end()}` に縮めて hot
を取り除き、`new_hots[hot_count++]` に記録する (呼出し側 §5.1)。

### Implementation 段階

愚直版 → 最適化版は以下の 2 ファイルで段階を追える:

- `docs/hot-range-finding/find_absolutely_hot_ranges_naive.hpp` — Alg.2 の逐語実装。各 $r$
  で `l'` 候補を右→左に全走査し、`SizeOf` / `NQrys` を毎回積み直す
- `docs/hot-range-finding/find_absolutely_hot_ranges_sliding_window.hpp` — `window_npairs` /
  `window_load` を保持し、$r$ の前進で加算、$l$ の前進で減算することで
  各集約値を amortized $O(1)$ に。これが bpforest.ipp:1175-1204 に対応

### callback 内 mutation の安全性

`DataChunkIterator` は `part_begin/part_end/cursor/p_load` を値として
保持し (pairs_range.hpp:39-40)、`part->begin()/end()` を live 参照しない。
加えて、ループ終端 `end_chunk` は関数先頭で `part->end()` の snapshot
を取っている。

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
2 フェーズ処理。

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

| ファイル | 追加される最適化 | 対応 bpforest.ipp 行 |
|---|---|---|
| `naive.hpp` | Alg.3 の逐語実装 (`effective_window_pair_count` と境界例外を毎回 $O(N)$ で計算) | — |
| `single_range_naive.hpp` | 単一 cold range 前提の愚直版 (意味論差分なし、論文そのまま) | — |
| `sliding_window.hpp` | $l$ を単調前進させる 2 ポインタ化。状態変数 (`left_npairs_offcut`, `right_npairs_offcut`, `right_window_offcut`, `non_left_effective`) で `effective_window_pair_count` を差分更新 | — |
| `opt1_bootstrap_whole_range_advance.hpp` | メインループ前に right を cold range 単位で bulk 進める初期化 | L1254-L1308 |
| `opt2_bootstrap_all_hot_early_exit.hpp` | bootstrap で全 range 吸収なら argmax 確定で carve へ直行 | L1271-L1276 |
| `opt3_single_range_steady_state.hpp` | `left_range == right_range` 用の専用 fast path | L1329-L1338 |
| `opt4_right_quota_threshold_gate.hpp` | 右端 rounded 幅の再計算・左縮小・margin 圧縮を `right_npairs_offcut > right_window_offcut` の成立時だけに遅延 | L1355-L1361 |
| `opt5_right_margin_compression.hpp` | 右 rounded 幅が増えた直後、同 range 内で right を先取りして margin を最小化 | L1362-L1367 |
| `opt6_range_level_left_shrink.hpp` | 左縮小を cold range 単位 (bulk) + 最終 range 内の chunk 単位に分解。opt6 で bpforest.ipp と semantically equivalent | L1369-L1398 |

**opt6 ↔ bpforest.ipp 乖離監視ポイント**: 以下 5 箇所は現時点で一致
しているが、bpforest.ipp 側だけ変更されると silent に乖離しうる:

1. 閾値ゲート条件 (L1355) `right_npairs_offcut > right_window_offcut` — 直下に dead comment の代替条件が残っており改変の兆候が出やすい
2. right margin compression (L1363) の `<` 境界
3. range-level left shrink 合流 (L1370-L1388) の state 畳み込み
4. final chunk-level left shrink (L1394) の `>=` 境界
5. single-range fast path (L1329-L1338) — 別 invariant 系で乖離が目立たない

---

## 6. `full_repartition` (bpforest.ipp:1411-1620)

### Semantics

全 KV ペアを CPU に回収し、DPU 数で等分した base partition 上から
greedy + double-scan で hot を抜き出し、`partial_sort` で低負荷 DPU に
割り当ててから全 DPU の tree を再構築する **一括リビルド** 経路。
論文 Algorithm 1 "Construction of hot/cold partitions" の骨格。

### Implementation (フロー)

1. (L1415) `retrieve_all_data(data_buf)` で全 DPU から KV ペア pull
2. (L1417-1424) `delims` と `chunked_cold_ranges_lists[]` をクリア (RAII cleanup 仕込み)
3. (L1426-1432) `data_buf` を `nr_base_parts` 等分、各 base partition を `chunked_cold_ranges[]` に詰める
4. (L1435-1439) `combine_delims()` → `route_queries()` で新 partition に re-route
5. (L1441) `param.balancing > 0` の場合のみ以降の hot 切り出しへ
   - (L1442-1443) `hot_load`, `cold_endpoint_cnt_goal` を計算
6. (L1445-1593) **各 base partition について** (`find_absolutely_hot_ranges` → `find_relatively_hot_ranges`):
   - (L1448-1449) `cold_npairs`, `hot_npairs` 確定
   - (L1451-1455) point かつ `nr_qrys <= cold_endpoint_cnt_goal` ならスキップ
   - (L1461-1502) load estimation (`chunk2load[]` を 0 初期化し、point は `upper_bound`、range は両 endpoint のうち `base_min..base_max` 内のもののみ計上)
   - (L1505-1531) **absolute phase**: `cold_endpoint_cnt > cold_endpoint_cnt_goal` の間 `find_absolutely_hot_ranges`。callback で `part` を縮め `new_hots[hot_count++]` に詰める
   - (L1528-1530) `base.npairs() == 0` なら list から erase
   - (L1533-1589) **relative phase**: まだ `cold_endpoint_cnt > cold_endpoint_cnt_goal` なら `nr_relative_hots = cold_endpoint_cnt / hot_load` を上限として `find_relatively_hot_ranges`。返り値の `[left_range, left)` と `[right, right_range->end())` から cold 残余を再構成
   - (L1591-1592) `nr_pairs[idx_base]`, `cold_loads[idx_base]` 更新
7. (L1595-1608) `new_hots[0..hot_count)` を低負荷 DPU に割当:
   - `partial_sort` で `cold_loads` 前 `hot_count` 個を負荷昇順
   - `new_hots` を load 降順 sort
   - 負荷の低い DPU ← 負荷の高い hot の対応付け
8. (L1610-1615) `combine_delims()` → `route_queries()` で更新後 delim に再 route
9. (L1619) `initialize_in_dpu(...)` で DPU 側 tree 再構築

---

## 7. `incremental_repartition` (bpforest.ipp:1674-2148)

### Semantics

前バッチの状態を残したまま、**負荷の偏りが閾値を超えた DPU についてのみ
hot を付け替える軽量経路**。Phase 1 で対象 DPU を選別 → Phase 2 で
serialize して CPU に cold を持ち出し → Phase 3 で hot 選出、の 3 段。
不変条件を破りそうなら末尾で `full_repartition` に fallback。

### Implementation (フロー)

1. (L1679) `param.balancing == 0` → return
2. **Phase 1: どの DPU を再分割するか決める** (L1689-1749)
   - (L1701-1702) `cold_partial_cnt_goal = nr_queries * max(3, α+1) / 3 / P`,
     `cold_partial_cnt_threshold = cold_partial_cnt_goal * high_watermark_ratio`
     (§1.3 末尾参照 — LHS は Phase 3 と別単位)
   - (L1704-1743) 各 DPU について `routed.cold[idx_dpu].nr_qrys` が threshold 以下なら `TASK_NONE`。超えていたら `cold_delims` を走査して既存 hot との境界キーを `hot_delim_keys[]`, `cold_key_ranges[]` に記録
   - (L1713-1715) `param.enable_incremental == false` なら即 `full_repartition` へ
   - (L1746-1748) `serialized_cold_count == 0` なら early return
3. **Phase 2: serialize 実行 / 回収 / cold range 再構築** (L1751-1836)
   - 各 DPU に `TASK_SERIALIZE` を gather + execute (L1755-1774)
   - 回収した pair 数を基に `data_buf.reserve()` して `hot_ranges` リセット (L1783-1794)。`incision_indices[]` も同時回収
   - KV pairs を `chunked_cold_ranges[]` に詰め直し、`chunked_cold_ranges_lists[idx_dpu]` として再連結 (L1797-1828)
4. **Phase 3: 各 DPU の新 hot を決める** (L1838-2075)
   - (L1838) `nr_existing_hots = delims.size() - nr_base_parts`
   - (L1839-1840) `hot_load`, `cold_endpoint_cnt_goal` 再計算 (full と同式)
   - 各 DPU について:
     - (L1843-1848) `TASK_NONE` は scan せず現負荷を `cold_loads` に記録
     - (L1860-1945) load estimation (endpoint ベース、Phase 1 とは単位が違う点に注意)
     - (L1947-1957) `cold_npairs`, `base_npairs`, `hot_npairs` 算出
     - (L1958-1963) `cold_endpoint_cnt <= cold_endpoint_cnt_goal` なら `TASK_NONE` に戻す
     - (L1966-2007) absolute phase
     - (L2009-2068) relative phase
     - (L2070-2071) `cold_npairs_list[idx_dpu]`, `cold_loads[idx_dpu]` 更新
     - (L2073-2075) **bailout**: `nr_existing_hots + hot_count > nr_base_parts` なら `full_repartition`
5. (L2078-2080) `hot_count == 0` なら何もせず return
6. (L2090-2103) `new_hots` を低負荷 DPU に割当。既に hot を持つ DPU は `cold_loads[idx].second = UINT32_MAX` にして候補から除外した上で `partial_sort`
7. (L2105-2114) `combine_delims()` 更新、各 DPU の `input_header` を `TASK_MOVE_HOT` 用に設定
8. (L2116-2140) DPU への更新 partition 送信と実行
9. (L2142-2146) `route_queries()` で再 route

### バッチ間の実行順 (batch_get / batch_range_count)

```
BPForest::batch_get(nr_queries, keys, results)
  ├─ route_queries(...)                        // L786
  ├─ incremental_repartition(...)              // L789   (param.balancing > 0)
  │    ├─ Phase 1: どの DPU を触るか決定
  │    ├─ Phase 2: TASK_SERIALIZE で KV を CPU 回収
  │    ├─ Phase 3: find_absolutely_hot_ranges → find_relatively_hot_ranges
  │    │           └─ bailout → full_repartition()           (L2075)
  │    ├─ new_hots を低負荷 DPU に割当 (partial_sort)
  │    ├─ TASK_MOVE_HOT 送信 → DPU 側で木を構築
  │    └─ route_queries() で再 route
  ├─ execute_in_dpus()                         // query 実行
  └─ postprocess_of_get()                      // 結果収集
```

`batch_range_count` も同じ流れ (`rcqs` と `postprocess_of_rcq()` に
置き換わるだけ)。

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
| `find_absolutely_hot_ranges` (L1175) | 同名の自由関数 |
| `find_relatively_hot_ranges` (L1207) | 同名の自由関数 |
| `full_repartition` (L1411) | `full_repartition` |
| `incremental_repartition` (L1674) | `rebalancing` (ファイル末尾付近) |
| `new_hots` = `ExtendableBuffer<NewHotRange>{nr_base_parts}` (固定サイズ) | `std::vector<NewHotRange>` + `reserve(ndpus)` (main.cpp:713-714, 1154-1155; growable) |
| incremental bailout `nr_existing_hots + hot_count > nr_base_parts` | `nr_existing_hots + new_hots.size() > ndpus` で `full_repartition` fallback (main.cpp:1283-1284) |
| `chunked_cold_ranges_lists` | 専用の `std::list<ChunkedPairsRange>`-相当 |

現行 simulator の `new_hots` は growable なので即 heap overwrite には
ならないが、§3 の不変条件が論理的に崩れた移植を書くと次段で別経路の
境界を踏みうる。crash 調査時に最初に確認すべきポイント:

1. `find_absolutely_hot_ranges` の callback 内 mutation が関数のループ
   変数に影響しない snapshot 設計になっているか (`DataChunkIterator`
   相当の型が `pair_begin` を live 参照していないか)
2. callback 1 回あたりの emit 個数 × 全 base partition の合計が `ndpus`
   を超えない不変条件が、移植版でも同じ証明で成立するか
3. incremental 相当の bailout (L2073-2075) と同じ check がシミュレータ
   側にあるか
