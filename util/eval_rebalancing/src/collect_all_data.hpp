#pragma once

#include "common.h"

#include <algorithm>
#include <cstddef>
#include <map>
#include <vector>

using Key = key_uint64_t;
using Value = value_uint64_t;
using DataMap = std::map<Key, Value>;
using MapNode = DataMap::node_type;

struct DistributedData {
    std::vector<DataMap> cold, hot;
};

inline std::vector<MapNode> collect_all_data(DistributedData&& data)
{
    // data.cold[i] と data.hot[i] はいずれも std::map なので key 昇順ソート済み
    // である。各 DPU の cold/hot を 2*ndpus 本のソート済み列とみなし、min-heap
    // による k-way merge で一本のソート済み列に束ねる。
    // 計算量: N 個の要素を k = 2*ndpus 本の列からマージするので O(N log k)。
    // N に対して k は独立な値 (DPU 数) なので N の漸近計算量は O(N) となる。
    //
    // 戻り値は std::map の node_type のベクタ。KVPair へ詰め替えず node の
    // 所有権のまま持ち回すことで、呼び出し側で別 map へ insert(std::move(node))
    // した際に node 再確保 (N 回の heap alloc) を省ける。
    size_t total = 0;
    std::vector<DataMap*> cursors;
    cursors.reserve(data.cold.size() + data.hot.size());
    for (size_t i = 0; i < data.cold.size(); ++i) {
        total += data.cold[i].size();
        if (!data.cold[i].empty()) {
            cursors.push_back(&data.cold[i]);
        }
    }
    for (size_t i = 0; i < data.hot.size(); ++i) {
        total += data.hot[i].size();
        if (!data.hot[i].empty()) {
            cursors.push_back(&data.hot[i]);
        }
    }

    std::vector<MapNode> all;
    all.reserve(total);

    // heap[i] は cursors[i] が指す現在の key の位置を示す index。
    // 比較は key 昇順 (priority_queue はデフォルトで最大値を取り出すので反転)。
    std::vector<size_t> heap;
    heap.reserve(cursors.size());
    for (size_t i = 0; i < cursors.size(); ++i) {
        heap.push_back(i);
    }
    auto cmp = [&](size_t a, size_t b) {
        return cursors[a]->begin()->first > cursors[b]->begin()->first;
    };
    std::make_heap(heap.begin(), heap.end(), cmp);

    while (!heap.empty()) {
        std::pop_heap(heap.begin(), heap.end(), cmp);
        const size_t idx = heap.back();
        DataMap* const cur = cursors[idx];
        auto node = cur->extract(cur->begin());
        all.push_back(std::move(node));
        if (cur->empty()) {
            heap.pop_back();
        } else {
            std::push_heap(heap.begin(), heap.end(), cmp);
        }
    }
    return all;
}
