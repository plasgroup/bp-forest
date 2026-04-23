#pragma once

#include "assert.hpp"
#include "common.h"
#include "partitioning.hpp"

#include <algorithm>
#include <cstddef>
#include <map>
#include <numeric>
#include <utility>
#include <vector>

using Value = value_uint64_t;
using DataMap = std::map<Key, Value>;

struct DistributedData {
    std::vector<DataMap> cold, hot;
};

struct RebuiltData {
    Partitioning parts;
    DistributedData data;
};

// src の全 cold/hot を k-way merge で全体昇順に走査しながら、境界位置
// i*N/ndpus の key を delim に emplace しつつ out.data.cold[part] へ
// DataMap::node_type をそのまま insert する 1-pass 融合。中間バッファを持たない
// ため N 要素分の handle (500M 要素で ~8GB) を節約できる。node は移植 (extract
// + insert) なので RB-tree 実体メモリの複製は発生しない。
inline RebuiltData rebuild_partitioning_nodes(DistributedData&& src, const unsigned ndpus)
{
    size_t total = 0;
    std::vector<DataMap*> cursors;
    cursors.reserve(src.cold.size() + src.hot.size());
    for (auto& m : src.cold) {
        total += m.size();
        if (!m.empty()) {
            cursors.push_back(&m);
        }
    }
    for (auto& m : src.hot) {
        total += m.size();
        if (!m.empty()) {
            cursors.push_back(&m);
        }
    }

    ASSERT(ndpus > 0);
    ASSERT(total >= ndpus);

    RebuiltData out;
    out.data.cold.resize(ndpus);
    out.data.hot.resize(ndpus);

    std::vector<size_t> bounds(ndpus);
    for (unsigned i = 0; i < ndpus; ++i) {
        bounds[i] = static_cast<size_t>(i) * total / ndpus;
    }

    std::vector<size_t> heap(cursors.size());
    std::iota(heap.begin(), heap.end(), size_t{0});
    auto cmp = [&](size_t a, size_t b) {
        return cursors[a]->begin()->first > cursors[b]->begin()->first;
    };
    std::make_heap(heap.begin(), heap.end(), cmp);

    unsigned part = 0;
    size_t cnt = 0;
    while (!heap.empty()) {
        std::pop_heap(heap.begin(), heap.end(), cmp);
        const size_t idx = heap.back();
        DataMap* const cur = cursors[idx];
        auto node = cur->extract(cur->begin());

        // delim 記録は insert より先。insert 後は node が moved-from になり
        // key() が取れない。
        if (part < ndpus && cnt == bounds[part]) {
            const Key key = node.key();
            out.parts.delims.emplace(Delim{key, DelimType::Base}, PartInfo{part});
            out.parts.delims.emplace(Delim{key, DelimType::Cold}, PartInfo{part});
            ++part;
        }

        out.data.cold[part - 1].insert(out.data.cold[part - 1].end(), std::move(node));
        ++cnt;

        if (cur->empty()) {
            heap.pop_back();
        } else {
            std::push_heap(heap.begin(), heap.end(), cmp);
        }
    }

    ASSERT(part == ndpus);
    return out;
}
