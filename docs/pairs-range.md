# pairs_range.hpp 構造メモ

対象: `host/inc/pairs_range.hpp`

## 型の階層

```
PairsRange                  -- KVPair 連続範囲のビュー
  ChunkedPairsRange         -- + chunk 分割 + chunk ごとの load
    LinkedChunkedPairsRange -- + LinkedList ノード化
  DataChunkIterator         -- ChunkedPairsRange 上の chunk 単位 random access iterator
NewHotRange                 -- hot range 切り出し結果 (PairsRange + KeyRange + load)
```

## PairsRange (L13-26)

KVPair 連続領域への非所有・読み取り専用ビュー。`begin()`/`end()` で KVPair ポインタ範囲、`npairs()` で要素数を返す。

## ChunkedPairsRange (L126-141)

PairsRange に chunk 分割と chunk ごとの load を加えたビュー。`nchunks()` で chunk 数、`begin()`/`end()` で chunk 単位の `DataChunkIterator` を返す。

コンストラクタ:
1. `(PairsRange, uint32_t*)` -- 対象 KVPair 範囲と chunk load 配列を指定
2. `(DataChunkIterator begin, DataChunkIterator end)` -- 既存の chunk iterator 区間 `[begin, end)` で部分範囲を切り出す

各 chunk の load 配列への参照をあわせて保持する。

## DataChunkIterator (L27-125)

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

## NewHotRange (L142-146)

hot range 切り出し結果。`find_absolutely_hot_ranges` / `find_relatively_hot_ranges` が返す。
データ範囲 (`pairs_range`)、キー範囲 (`key_range`)、推定負荷 (`load`) の 3 つ組。

## LinkedChunkedPairsRange (L149)

各 DPU の cold range リストを構成するノード。
hot 切り出し時に erase/insert される。
`LinkedElement<ChunkedPairsRange>` の typedef。
