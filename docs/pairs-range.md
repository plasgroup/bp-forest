# pairs_range.hpp structure notes

Subject: `bpforest/inc/pairs_range.hpp`

## Type hierarchy

```
PairsRange                  -- view of a contiguous range of KVPairs
  LinkedPairsRange          -- + as a LinkedList node
  ChunkedPairsRange         -- + chunk division + per-chunk load
    LinkedChunkedPairsRange -- + as a LinkedList node
DataChunkIterator           -- chunk-wise random access iterator over ChunkedPairsRange (not derived)
NewHotRange                 -- result of carving out a hot range (PairsRange + KeyRange + load)
```

## PairsRange

A non-owning, read-only view of a contiguous region of KVPairs. `begin()`/`end()` return the range of KVPair pointers, and `npairs()` returns the number of elements.

## ChunkedPairsRange

A view that adds chunk division and per-chunk load to PairsRange. `nchunks()` returns the number of chunks, and `begin()`/`end()` return chunk-wise `DataChunkIterator`s.

Constructors:
1. `(PairsRange, uint32_t* load = nullptr)` -- specifies the target KVPair range and the chunk load array
2. `(DataChunkIterator begin, DataChunkIterator end)` -- carves out a subrange given by an existing chunk iterator interval `[begin, end)`

It also holds a reference to the load array of its chunks. The load array can be replaced later with
`set_load_ary(uint32_t*)`.
This is used when a range is built first and its load array is assigned later: the repartition workers
reallocate, per DPU, a thread_local buffer `chunk2load` with as many chunks as needed,
pass its start (on the incremental cold side, the position of each slice obtained by
dividing the buffer among the elements of the cold range list) to each range with
`set_load_ary`, and then count up the loads.

## DataChunkIterator

A chunk-wise random access iterator over ChunkedPairsRange.
Returned by `ChunkedPairsRange::begin()`/`end()`.

Each chunk is `KVPairsChunkSize` pairs. The last chunk may be partial.

Main capabilities:
- `begin()`/`end()`: the KVPair range of this chunk
- `npairs()`: the number of elements in the chunk
- `load()`: a reference to this chunk's load counter
- `load_ptr()`: a raw pointer to the load counter (to avoid UB at one-past-end)
- `operator++`/`--`/`+=`: movement in chunks
- `operator-`: the difference in chunks between two iterators
- `operator==`: whether two iterators point to the same position

Internally, it holds the current position within the original PairsRange and the corresponding position in the load array.

## NewHotRange

The result of carving out a hot range. A 4-tuple of the data range (`pairs_range`), the key range (`key_range`),
the estimated load (`load`), and the base partition it was carved from (`origin`). Carving starts from two places:

- `find_relatively_hot_ranges` -- carves hot ranges out of the cold region
- `split_hot_range_equal_load` (`bpforest/inc/split_hot_range.hpp`) -- when an existing hot range overheats,
  splits it into pieces of equal load

Both only pass the carved pieces to the caller through a hook;
the hooks on the repartition worker side assemble them into `NewHotRange`s.

## LinkedPairsRange

An element of the sequence of per-DPU cold regions (`BPForest::cold_ranges`).
Used by the initial data distribution (`distribute_data_based_on_partitions`) and by retrieving all data (`retrieve_all_data`).
Neither decides on carving out hot ranges, so the `PairsRange` version without per-chunk load suffices.
A typedef of `LinkedElement<PairsRange>`.

## LinkedChunkedPairsRange

A node of each DPU's cold range list.
Erased/inserted when hot ranges are carved out.
A typedef of `LinkedElement<ChunkedPairsRange>`.
