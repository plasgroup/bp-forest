#pragma once

#include <cstddef>
#include <vector>


//! @brief Folds any index range of a fixed sequence in O(log n) time, after
//! an O(n) build. `Op` must be associative (min, max, sum, ...).
template <typename T, typename Op>
class SegmentTree
{
    std::vector<T> tree;
    size_t n;
    Op op;
    T identity;

public:
    SegmentTree(const std::vector<T>& data, T identity)
        : tree(2 * data.size(), identity), n{data.size()}, identity{identity}
    {
        for (size_t i = 0; i < n; i++)
            tree[n + i] = data[i];
        for (size_t i = n; i-- > 1;)
            tree[i] = op(tree[2 * i], tree[2 * i + 1]);
    }

    //! Both `left` and `right` are included in the folded range.
    T query(size_t left, size_t right) const
    {
        T res = identity;

        for (size_t l = left + n, r = right + n + 1; l < r; l /= 2, r /= 2) {
            if (l % 2 == 1)
                res = op(res, tree[l++]);
            if (r % 2 == 1)
                res = op(res, tree[--r]);
        }

        return res;
    }
};


template <typename T>
struct MinOp {
    T operator()(T a, T b) const { return a < b ? a : b; }
};

template <typename T>
struct MaxOp {
    T operator()(T a, T b) const { return a > b ? a : b; }
};

template <typename T>
struct SumOp {
    T operator()(T a, T b) const { return a + b; }
};
