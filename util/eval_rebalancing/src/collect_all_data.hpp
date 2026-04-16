#pragma once

#include "common.h"

#include <algorithm>
#include <cstddef>
#include <map>
#include <vector>

using Key = key_uint64_t;
using Value = value_uint64_t;

struct DistributedData {
    std::vector<std::map<Key, Value>> cold, hot;
};

inline std::vector<KVPair> collect_all_data(DistributedData&& data)
{
    // data.cold[i] と data.hot[i] はいずれも std::map なので key 昇順ソート済み
    // である。各 DPU の cold/hot を 2*ndpus 本のソート済み列とみなし、min-heap
    // による k-way merge で一本のソート済み列に束ねる。
    // 計算量: N 個の要素を k = 2*ndpus 本の列からマージするので O(N log k)。
    // N に対して k は独立な値 (DPU 数) なので N の漸近計算量は O(N) となる。
    // 以前の std::sort 実装は O(N log N) だったため、この関数内で N に対する
    // 漸近計算量が悪化していた点を改善する。
    size_t total = 0;
    std::vector<std::map<Key, Value>*> cursors;
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

    std::vector<KVPair> all;
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
        std::map<Key, Value>* const cur = cursors[idx];
        const auto node = cur->extract(cur->begin());
        all.push_back(KVPair{node.key(), node.mapped()});
        if (cur->empty()) {
            heap.pop_back();
        } else {
            std::push_heap(heap.begin(), heap.end(), cmp);
        }
    }
    return all;
}
