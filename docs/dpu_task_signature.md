DPU タスクの入出力仕様
===

DPU プログラム (`dpu/src/dpumain.c`) は 1 回の起動で 1 種類のタスクを実行する。
どのタスクを実行するか、およびその引数は、MRAM ヒープ先頭に置かれた `InputHeader`
(`common/inc/input_header.h`, 16 バイト固定) で指定し、結果も MRAM に書き戻す。
この文書は、各タスクの「引数 → 返り値」と、それらの MRAM 上の配置を記述する。

## 記法と共通の約束

*   `(引数, ...) -> (返り値, ...)` の形で書く。
    *   引数のうち先頭のスカラ値は `InputHeader` の union メンバとして渡す。
    *   それに続く配列は**ペイロード**、すなわちヒープ先頭 + `sizeof(InputHeader)` (= 16) から
        始まる領域に置く。
    *   返り値の配置はタスクごとに異なる (下記各項を参照)。
*   `Key` = `key_uint64_t`, `Value` = `value_uint64_t` (`common/inc/workload_types.h`)。
    `KVPair` / `KeyRange` / `RangeCountQuery` / `SummaryBlock` は `common/inc/common.h`。
*   ほとんどのタスクは cold 木と hot 木の両方を対象とする。クエリ・ペアの配列は
    **`[cold | hot]` の連続**で渡し、ヘッダで各々の個数を伝える。
*   タスク ID の一覧は `enum TaskID` (`common/inc/common.h`)。

`InputHeader` の union メンバとタスクの対応:

| union メンバ | 内容 | 使うタスク |
| --- | --- | --- |
| `init` | `uint32_t nr_cold_pairs, nr_hot_pairs` | TASK_INIT |
| `qrys` | `uint32_t nr_cold_qrys, nr_hot_qrys, result_offset` | TASK_GET / PRED / RANGE_COUNT / RANGE_MAX / INSERT / DELETE |
| `move_hot` | `uint32_t nr_cold_pairs, nr_hot_pairs; bool renew_cold, renew_hot` | TASK_MOVE_HOT |
| `serialize` | `uint32_t nr_delims, max_nr_delims; bool do_cold, do_hot` | TASK_SERIALIZE |
| `rmq` | `uint16_t nr_cold_lumps, nr_hot_lumps` | TASK_RANGE_MIN (未修理、後述) |
| `extract`, `restore` | `uint32_t nr_ranges` | 未実装タスク用の残骸 |

`qrys.result_offset` は、返り値を書き込む位置をヒープ先頭からのバイトオフセットで表す。
ホストは全 DPU 中の最大クエリ数 `max_nqrys` を使って
`sizeof(InputHeader) + sizeof(Query) * max_nqrys` を与える
(`BPForest::execute_in_dpus`, `host/inc/bpforest.ipp`)。全 DPU で同じ値にすることで、
結果の一括転送を単一のオフセットからおこなえる。

現行プロトコルの最も読みやすい参照実装は `host/inc/fake_dpu.ipp` (fake_dpu ビルド用の
DPU エミュレータ)。DPU 側の実体は `dpu/src/bplustree.c`。

## 実装済みのタスク

`dpu/src/dpumain.c` の `switch` に分岐が存在するもの。`SUPPORT_*` マクロ付きのタスクは、
そのマクロを定義したビルド (CMake の `CMAKE_C_FLAGS` に `-DSUPPORT_GET` などを渡す) でのみ
DPU バイナリに含まれる。

### TASK_INIT

```
(uint32_t nr_cold_pairs, uint32_t nr_hot_pairs, KVPair[nr_cold_pairs + nr_hot_pairs]) -> ()
```

キー昇順に整列した `[cold | hot]` のペア列から、cold 木と hot 木をボトムアップ構築する。
構築アルゴリズムは `docs/tree_initialization.md`。

### TASK_GET (`SUPPORT_GET`)

```
(uint32_t nr_cold_qrys, uint32_t nr_hot_qrys, uint32_t result_offset,
 Key[nr_cold_qrys + nr_hot_qrys]) -> Value[nr_cold_qrys + nr_hot_qrys]
```

キーが存在しなければ `NOT_FOUND_VALUE` を返す。

### TASK_PRED (`SUPPORT_PRED`)

```
(uint32_t nr_cold_qrys, uint32_t nr_hot_qrys, uint32_t result_offset,
 Key[nr_cold_qrys + nr_hot_qrys]) -> KVPair[nr_cold_qrys + nr_hot_qrys]
```

クエリキー**未満**で最大のキーを持つペア (strict predecessor) を返す。
ホスト側のクエリ振り分けが「答えはこの木の中にある」ことを保証する。
`BPForest::batch_pred` の返り値がキーと値の組なので、DPU も `KVPair` を返す。

### TASK_RANGE_COUNT (`SUPPORT_RANGE_COUNT`)

```
(uint32_t nr_cold_qrys, uint32_t nr_hot_qrys, uint32_t result_offset,
 RangeCountQuery[nr_cold_qrys + nr_hot_qrys]) -> uint64_t[nr_cold_qrys + nr_hot_qrys]
```

`RangeCountQuery = {KeyRange range; Value needle}`。キー範囲 (両端 inclusive) に含まれる
ペアのうち、値が `needle` に等しいものの個数を返す。

### TASK_RANGE_MAX (`SUPPORT_RANGE_MAX`)

```
(uint32_t nr_cold_qrys, uint32_t nr_hot_qrys, uint32_t result_offset,
 KeyRange[nr_cold_qrys + nr_hot_qrys]) -> Value[nr_cold_qrys + nr_hot_qrys]
```

キー範囲 (両端 inclusive) 内の値の最大値を返す。範囲内にペアが無ければ `NOT_FOUND_VALUE` (= 0)。

### TASK_INSERT (`SUPPORT_INSERT`)

```
(uint32_t nr_cold_qrys, uint32_t nr_hot_qrys, uint32_t result_offset,
 KVPair[nr_cold_qrys + nr_hot_qrys]) -> uint32_t[2]
```

既存キーなら値を上書き、無ければ挿入する。返り値は挿入後の (cold 木のペア数, hot 木のペア数)。
ホストはこれで DPU ごとのペア数の記録を更新する (rebalancing の負荷計算や retrieve バッファの確保が参照する)。

### TASK_DELETE (`SUPPORT_DELETE`)

```
(uint32_t nr_cold_qrys, uint32_t nr_hot_qrys, uint32_t result_offset,
 Key[nr_cold_qrys + nr_hot_qrys]) -> uint32_t[2]
```

返り値は削除後の (cold 木のペア数, hot 木のペア数)。TASK_INSERT と同形式。

### TASK_SERIALIZE

```
(uint32_t nr_delims, uint32_t max_nr_delims, bool do_cold, bool do_hot,
 Key[nr_delims]) -> (uint32_t[nr_delims], KVPair[])
```

木の中身を KV ペア列として書き出す。rebalancing でペアを DPU 間で移すときに使う。

*   引数の `Key[nr_delims]` は cold 領域の切れ目 (= hot range の始点キー) を昇順に並べたもの。
*   `do_cold` / `do_hot` で書き出す対象を選ぶ (cold+hot / cold のみ / hot のみ の 3 形態)。
*   返り値の `uint32_t[nr_delims]` (incisions) は、切れ目が書き出した cold ペア配列の何番目に
    あたるかを示す。厳密には `incisions[i]` = 「キーが `delims[i]` より大きい最初のペアの位置」
    (cold 木は切れ目キー自身を含まないので、実質的には切れ目より手前のペア数)。
    **引数の delim 領域を上書きする形で**、ペイロード先頭に書く。
*   返り値の `KVPair[]` は `[cold | hot]` の連続で、ヒープ先頭 +
    `sizeof(InputHeader) + sizeof(Key) * max_nr_delims` から始まる。`nr_delims` ではなく
    DPU 間で共通の `max_nr_delims` を使うことで、ペア配列の開始位置を全 DPU で揃えている。
    `do_cold=false` のときは cold が 0 個なので hot が先頭から始まる。

### TASK_MOVE_HOT

```
(uint32_t nr_cold_pairs, uint32_t nr_hot_pairs, bool renew_cold, bool renew_hot,
 KVPair[nr_cold_pairs + nr_hot_pairs]) -> ()
```

TASK_SERIALIZE で吸い出したペアを移送先 DPU に流し込む。`renew_*` が真ならその木を
渡されたペアから作り直し (TASK_INIT と同じ構築)、偽なら既存の木へ upsert する。

### TASK_NONE

```
() -> ()
```

何もしない。ランク単位で DPU を起動する都合上、そのバッチで仕事の無い DPU に割り当てる。

### TASK_RANGE_MIN (`SUPPORT_RANGE_MIN`) — 現状は未修理

```
(uint16_t nr_cold_lumps, uint16_t nr_hot_lumps, uint16_t[], Key[]) -> Value[]
```

連続・重複するクエリ範囲をまとめた「かたまり (lump)」単位で処理する設計:

*   cold/hot それぞれのかたまりの数を受け取る。
*   cold/hot それぞれについて、次の配列における各かたまりの始点インデックスを受け取る
    (末尾に配列全体の要素数が付く)。
*   かたまりごとに、その中のクエリを処理するのに必要な小範囲を
    (始点[0], 終点[0] = 始点[1]-1, 終点[1] = 始点[2]-1, ..., 終点[n]) の形で受け取る
    (終点は inclusive)。

**このタスクは現状どのビルドでも有効化できない。** 復活させるには最低限:

*   `dpu/src/bplustree.c` の `task_range_min` がペイロード開始位置をヒープ先頭 + 8 と仮定している
    (現行 `InputHeader` は 16 バイト)。
*   同関数が廃止済みマクロ `RESULT_OFFSET` を参照している (現行は `qrys.result_offset` を使う)。
*   ホスト側 `BPForest::batch_range_minimum` も `host/src/host.cpp` ではスタブ (未実装メッセージ)。

## 未実装のタスク

`enum TaskID` には ID があるが、DPU 側に実装が無いもの。将来実装する可能性を残して仕様案を
保存してある。ここに書かれた引数・返り値は**設計案であって現行実装ではない**。

### TASK_SCAN

```
(uint16_t, uint16_t, KeyRange[]) -> (uint32_t, uint32_t, uint32_t[], (uint32_t, uint32_t)[], (uint32_t, uint32_t)[], Value[])
```

*   cold/hot それぞれへのクエリ数を受け取る。
*   cold/hot それぞれから返す値の数を最初に教える。
*   cold range の切れ目 (hot range を取り出したところ) が値配列の何番目にあるかを教える。
*   cold range へのクエリそれぞれの始端・終端の位置を、直前の「切れ目」からのオフセットで教える。
*   hot range へのクエリそれぞれの始端・終端の位置を教える。

ホスト側にも `BPForest::batch_scan` の宣言だけが残っている (定義なし)。

### TASK_RANGE_SUM

キー範囲内の値の総和。CPU ベースライン (`cpudb/`) だけが実装しており、DPU 側は未実装。

### TASK_SUMMARIZE

```
() -> (uint32_t, uint16_t, uint16_t[], SummaryBlock[])
```

*   サマリーの中身は、ある程度の塊に分けてシャッフルしたような並びになっている。
*   全要素数を返す。
*   「塊」の数を返す。
*   それぞれの塊が、正しい順番に並べた時に何個目で終わるかを返す。
    *   例: `(xx, 2, (6, 2, 4), (block0, ..., block5))` は、
        `(block2, block3, block4, block5, block0, block1)` とすれば正しい順番になることを示す。

### TASK_EXTRACT

```
(uint32_t nr_ranges, KeyRange[nr_ranges]) -> (uint32_t[], KVPair[])
```

キー範囲は閉区間。

### TASK_FLATTEN_HOT

```
() -> (uint32_t, KVPair[])
```

### TASK_RESTORE

```
(uint32_t nr_ranges, uint32_t[], KVPair[]) -> ()
```

## tasklet による並列化の可否

現行の各タスクの tasklet 数は `dpu/inc/dpu_params.h` の `TASK_*_NR_TASKLETS` で決まる。
既定では TASK_GET / TASK_PRED / TASK_DELETE / TASK_RANGE_COUNT / TASK_RANGE_MAX と
木の構築 (`TREE_CONSTRUCT_NR_TASKLETS`) が `NR_TASKLETS` 並列、
TASK_INSERT と TASK_SERIALIZE は 1 tasklet に固定されている
(`_Static_assert(TASK_INSERT_NR_TASKLETS == 1)` / `_Static_assert(TASK_SERIALIZE_NR_TASKLETS == 1)`)。

返り値の個数がクエリごとに 1 つでないタスクは、並列化が難しい。
先頭以外の tasklet は、自分の結果を返り値配列のどのオフセットから書き始めればいいか分からないため。
対策は 3 通り考えられる:

1.  並列化しない。
2.  適当な場所に書き込んでおき、あとで詰める。
3.  適当な場所に書き込んでおき、どんな順番になったかをホストに教えることで、DPU から
    転送するときに詰める。

この問題に該当するタスク: TASK_SCAN, TASK_SUMMARIZE, TASK_EXTRACT, TASK_FLATTEN_HOT。
TASK_SERIALIZE も同種の問題を持ち、現状は対策 1 (単一 tasklet) を採っている。

TASK_INSERT が単一 tasklet なのは別の理由による (ノード分割の競合)。並列化の設計案は
`docs/batch_insert_proposal.md`。

TASK_SUMMARIZE は単一 tasklet でも上記の問題が残るため、`uint16_t[4]` + `Key[4]` の
ブロック (`SummaryBlock`) 単位で書き出し、並べ替え情報を別に返す設計になっている。
