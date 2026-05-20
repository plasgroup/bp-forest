#pragma once

#include "common.h"
#include "host_params.hpp"
#include "pairs_range.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>

// Greedily splits a single hot range into pieces of roughly equal load.
//
// `hot` must already have its per-chunk load array populated (via
// `set_load_ary` + accumulation, like cold Stage2). `total_load` is the
// hot's measured endpoint load, `target_pieces` the desired number of
// pieces (the caller's `N`, guaranteed >= 2 for an actual split), and
// `hot_max_key` the upper key bound of the original hot delim (KEY_MAX or
// the delim's max key) used for the trailing piece's KeyRange.
//
// Pieces fully cover the hot with no gaps and contiguous KeyRanges. A
// piece is closed only when its accumulated load reaches `target_load`;
// a single heavy chunk that cannot be subdivided keeps its piece whole,
// so the actual emit count may be < `target_pieces` (and is 1 when the
// hot is a single chunk). Returns the actual number of emitted pieces.
template <typename PieceHook /* void(PairsRange, KeyRange, uint32_t load, uint32_t max_chunk_load) */>
inline dpu_id_t split_hot_range_equal_load(const ChunkedPairsRange& hot,
    uint32_t total_load, dpu_id_t target_pieces, key_uint64_t hot_max_key,
    PieceHook&& emit)
{
    assert(target_pieces >= 1);
    const uint32_t target_load = (total_load + target_pieces - 1) / target_pieces;  // ceil

    dpu_id_t emit_count = 0;
    uint32_t acc_load = 0, piece_max = 0;
    DataChunkIterator piece_begin = hot.begin();
    const DataChunkIterator hot_end = hot.end();

    for (DataChunkIterator chunk = hot.begin(); chunk != hot_end; ++chunk) {
        const uint32_t cl = chunk->load();
        acc_load += cl;
        piece_max = std::max(piece_max, cl);

        DataChunkIterator next = chunk;
        ++next;
        if (acc_load >= target_load && next != hot_end) {
            const KVPair* const pb = piece_begin->begin();
            const KVPair* const pe = next->begin();
            emit(PairsRange{pb, pe}, KeyRange{pb->key, pe->key - 1}, acc_load, piece_max);
            emit_count++;
            piece_begin = next;
            acc_load = piece_max = 0;
        }
    }

    // trailing piece: everything from piece_begin to the end of the hot
    const KVPair* const pb = piece_begin->begin();
    emit(PairsRange{pb, hot_end->begin()}, KeyRange{pb->key, hot_max_key}, acc_load, piece_max);
    emit_count++;

    return emit_count;
}
