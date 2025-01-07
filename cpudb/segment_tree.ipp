#pragma once

#include <iostream>
#include <vector>

template <typename T, typename Op>
class SegmentTree {
public:
    SegmentTree(const std::vector<T>& data, T identity)
        : n(data.size()), identity(identity) {
        tree.resize(2 * n, identity);
        build(data);
    }

    void update(int index, T value) {
        index += n;
        tree[index] = value;
        while (index > 0) {
            index /= 2;
            tree[index] = op(tree[2 * index], tree[2 * index + 1]);
        }
    }

    // query for the sum of the elements in the range [left, right], both inclusive.
    T query(int left, int right) {
        left += n;
        right += n + 1; // adjust the range to [left, right). The following code is for right-open range.
        T res = identity;

        while (left < right) {
            if (left % 2 == 1) {
                res = op(res, tree[left]);
                left++;
            }
            if (right % 2 == 1) {
                right--;
                res = op(res, tree[right]);
            }
            left /= 2;
            right /= 2;
        }
        return res;
    }

private:
    std::vector<T> tree;
    int n;
    Op op;
    T identity;

    void build(const std::vector<T>& data) {
        for (int i = 0; i < n; ++i) {
            tree[n + i] = data[i];
        }
        for (int i = n - 1; i > 0; --i) {
            tree[i] = op(tree[2 * i], tree[2 * i + 1]);
        }
    }
};

template <typename T>
class SumOp {
public:
    T operator ()(T a, T b) {
        return a + b;
    }
};