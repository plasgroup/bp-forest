# pairs_range.hpp 構造メモ

対象: `bpforest/inc/pairs_range.hpp`

## 型の階層

```
PairsRange                  -- KVPair 連続範囲のビュー
  LinkedPairsRange          -- + LinkedList ノード化
  ChunkedPairsRange         -- + chunk 分割 + chunk ごとの load
    LinkedChunkedPairsRange -- + LinkedList ノード化
DataChunkIterator           -- ChunkedPairsRange 上の chunk 単位 random access iterator (非派生)
NewHotRange                 -- hot range 切り出し結果 (PairsRange + KeyRange + load)
```

## PairsRange

KVPair 連続領域への非所有・読み取り専用ビュー。`begin()`/`end()` で KVPair ポインタ範囲、`npairs()` で要素数を返す。

## ChunkedPairsRange

PairsRange に chunk 分割と chunk ごとの load を加えたビュー。`nchunks()` で chunk 数、`begin()`/`end()` で chunk 単位の `DataChunkIterator` を返す。

コンストラクタ:
1. `(PairsRange, uint32_t* load = nullptr)` -- 対象 KVPair 範囲と chunk load 配列を指定
2. `(DataChunkIterator begin, DataChunkIterator end)` -- 既存の chunk iterator 区間 `[begin, end)` で部分範囲を切り出す

各 chunk の load 配列への参照をあわせて保持する。load 配列は後から
`set_load_ary(uint32_t*)` で差し替えられる。
range を先に組み立てて load 配列を後から割り当てる場面で使う: repartition の worker は
DPU ごとに thread_local バッファ `chunk2load` を必要な chunk 数だけ確保し直し、
その先頭 (incremental の cold 側は、バッファを cold range リストの要素ごとに
切り分けた各位置) を `set_load_ary` で各 range に渡してから負荷を数え上げる。

## DataChunkIterator

ChunkedPairsRange 上の chunk 単位 random access iterator。
`ChunkedPairsRange::begin()`/`end()` が返す。

各 chunk は `KVPairsChunkSize` ペア単位。最終 chunk は端数あり。

主な capability:
- `begin()`/`end()`: この chunk の KVPair 範囲
- `npairs()`: chunk 内の要素数
- `load()`: この chunk の load カウンタへの参照
- `load_ptr()`: load カウンタの生ポインタ (one-past-end 時の UB 回避用)
- `operator++`/`--`/`+=`: chunk 単位の移動
- `operator-`: 2 iterator 間の chunk 数の差
- `operator==`: 同じ位置を指しているかの比較

内部的には、元の PairsRange 内での現在位置と対応する load 配列位置を保持する。

## NewHotRange

hot range 切り出し結果。データ範囲 (`pairs_range`)、キー範囲 (`key_range`)、
推定負荷 (`load`) の 3 つ組。切り出しの起点は 3 か所:

- `find_absolutely_hot_ranges` -- cold 領域から絶対 hot を切り出す
- `find_relatively_hot_ranges` -- cold 領域から相対 hot を切り出す
- `split_hot_range_equal_load` (`bpforest/inc/split_hot_range.hpp`) -- 既存 hot が過熱したとき、
  それを負荷が均等になるよう分割する

いずれも切り出した piece を hook 経由で呼び出し側に渡すだけで、
`NewHotRange` に組み立てるのは repartition worker 側の hook。

## LinkedPairsRange

DPU ごとの cold 領域を並べた列 (`BPForest::cold_ranges`) の要素。
初期データ投入 (`distribute_data_based_on_partitions`) と全データ回収 (`retrieve_all_data`) が使う。
どちらも hot の切り出し判定をしないので、chunk 単位の load を持たない `PairsRange` 版でよい。
`LinkedElement<PairsRange>` の typedef。

## LinkedChunkedPairsRange

各 DPU の cold range リストを構成するノード。
hot 切り出し時に erase/insert される。
`LinkedElement<ChunkedPairsRange>` の typedef。
