#include "rebuild_partitioning_nodes.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <map>
#include <utility>
#include <vector>

namespace
{
DistributedData make_dd(size_t ndpus)
{
    DistributedData d;
    d.cold.resize(ndpus);
    d.hot.resize(ndpus);
    return d;
}

size_t count_total(const DistributedData& d)
{
    size_t n = 0;
    for (const auto& m : d.cold) n += m.size();
    for (const auto& m : d.hot) n += m.size();
    return n;
}

std::vector<std::pair<Key, Value>> flatten_sorted(const DistributedData& d)
{
    std::vector<std::pair<Key, Value>> v;
    for (const auto& m : d.cold) {
        for (const auto& kv : m) v.emplace_back(kv.first, kv.second);
    }
    for (const auto& m : d.hot) {
        for (const auto& kv : m) v.emplace_back(kv.first, kv.second);
    }
    std::sort(v.begin(), v.end());
    return v;
}

// 境界同値性テスト用の期待値算出: sorted[i*N/ndpus].key が各 partition の
// 先頭 (delim) となる。
std::vector<Key> expected_delim_keys(const std::vector<std::pair<Key, Value>>& sorted, unsigned ndpus)
{
    const size_t n = sorted.size();
    std::vector<Key> keys(ndpus);
    for (unsigned i = 0; i < ndpus; ++i) {
        keys[i] = sorted[i * n / ndpus].first;
    }
    return keys;
}

void verify_rebuilt(const DistributedData& in, const RebuiltData& rb, unsigned ndpus)
{
    // partition 数の整合
    assert(rb.data.cold.size() == ndpus);
    assert(rb.data.hot.size() == ndpus);

    // 1. マージ正当性: 全要素が保存され、かつ key 全体で昇順 (= 各 cold 内が
    //    昇順 + cold[i] の max < cold[i+1] の min) を flatten_sorted で間接
    //    確認。
    const auto expected = flatten_sorted(in);
    const auto actual_sorted = flatten_sorted(rb.data);
    assert(expected == actual_sorted);

    // rebuild 側は全て cold に入る (hot は空のまま)
    for (const auto& m : rb.data.hot) assert(m.empty());

    // 2. 境界位置: delim の key 列が sorted[i*N/ndpus].key と一致
    auto exp_keys = expected_delim_keys(expected, ndpus);
    std::vector<Key> base_keys, cold_keys;
    for (const auto& [delim, info] : rb.parts.delims) {
        if (delim.type == DelimType::Base) base_keys.push_back(delim.key);
        if (delim.type == DelimType::Cold) cold_keys.push_back(delim.key);
    }
    std::sort(base_keys.begin(), base_keys.end());
    std::sort(cold_keys.begin(), cold_keys.end());
    std::sort(exp_keys.begin(), exp_keys.end());
    assert(base_keys.size() == ndpus);
    assert(cold_keys.size() == ndpus);
    assert(base_keys == exp_keys);
    assert(cold_keys == exp_keys);

    // 3. partition 割当が 0..ndpus-1 に一意
    std::vector<unsigned> base_dpus;
    for (const auto& [delim, info] : rb.parts.delims) {
        if (delim.type == DelimType::Base) base_dpus.push_back(info.dpu);
    }
    std::sort(base_dpus.begin(), base_dpus.end());
    for (unsigned i = 0; i < ndpus; ++i) assert(base_dpus[i] == i);

    // 4. cold[i] のサイズが bounds[i+1] - bounds[i] (または 余り)
    const size_t n = expected.size();
    for (unsigned i = 0; i < ndpus; ++i) {
        const size_t lo = static_cast<size_t>(i) * n / ndpus;
        const size_t hi = static_cast<size_t>(i + 1) * n / ndpus;
        assert(rb.data.cold[i].size() == hi - lo);
    }
}

void assert_src_empty(const DistributedData& d)
{
    for (const auto& m : d.cold) assert(m.empty());
    for (const auto& m : d.hot) assert(m.empty());
}

void run_and_verify(const DistributedData& in, unsigned ndpus)
{
    DistributedData copy = in;
    auto rb = rebuild_partitioning_nodes(std::move(copy), ndpus);
    verify_rebuilt(in, rb, ndpus);
    assert_src_empty(copy);
}

// マージ正当性 (各構成で全要素が昇順に揃い、元 DistributedData が空化すること).
// ndpus は N >= ndpus を満たすように調整。

void test_S1_single_dpu_cold_only()
{
    DistributedData d = make_dd(1);
    d.cold[0][1] = 10;
    d.cold[0][2] = 20;
    d.cold[0][3] = 30;
    run_and_verify(d, 1);
}

void test_S2_single_dpu_hot_only()
{
    DistributedData d = make_dd(1);
    d.hot[0][1] = 10;
    d.hot[0][2] = 20;
    d.hot[0][3] = 30;
    run_and_verify(d, 1);
}

void test_S3_single_dpu_cold_hot_disjoint()
{
    DistributedData d = make_dd(1);
    d.cold[0][1] = 10;
    d.cold[0][3] = 30;
    d.cold[0][5] = 50;
    d.hot[0][2] = 20;
    d.hot[0][4] = 40;
    run_and_verify(d, 1);
}

void test_M1_three_dpus_disjoint()
{
    DistributedData d = make_dd(3);
    d.cold[0][0] = 0;
    d.cold[0][1] = 10;
    d.hot[0][2] = 20;
    d.cold[1][3] = 30;
    d.hot[1][4] = 40;
    d.hot[1][5] = 50;
    d.cold[2][6] = 60;
    d.cold[2][7] = 70;
    run_and_verify(d, 3);
}

void test_M2_reverse_dpu_order()
{
    DistributedData d = make_dd(3);
    d.cold[0][100] = 1000;
    d.cold[0][200] = 2000;
    d.cold[1][50] = 500;
    d.cold[2][10] = 100;
    d.cold[2][20] = 200;
    run_and_verify(d, 3);
}

void test_M3_interleaved_cold_hot()
{
    DistributedData d = make_dd(2);
    d.cold[0][1] = 1;
    d.cold[0][3] = 3;
    d.cold[0][5] = 5;
    d.hot[0][2] = 2;
    d.hot[0][4] = 4;
    d.hot[0][6] = 6;
    d.cold[1][7] = 7;
    d.cold[1][9] = 9;
    d.hot[1][8] = 8;
    d.hot[1][10] = 10;
    run_and_verify(d, 2);
}

void test_B1_one_dpu_large()
{
    constexpr size_t N = 10000;
    DistributedData d = make_dd(8);
    for (size_t i = 0; i < N; ++i) {
        d.cold[3][static_cast<Key>(i)] = static_cast<Value>(i * 2);
    }
    run_and_verify(d, 8);
}

void test_B2_asymmetric_two_dpus()
{
    DistributedData d = make_dd(2);
    d.cold[0][0] = 0;
    d.cold[0][1] = 1;
    d.cold[0][2] = 2;
    for (size_t i = 100; i < 1100; ++i) {
        d.cold[1][static_cast<Key>(i)] = static_cast<Value>(10 * i);
    }
    run_and_verify(d, 2);
}

void test_K1_descending_dpu_id()
{
    DistributedData d = make_dd(4);
    for (int i = 0; i < 3; ++i) {
        d.cold[0][static_cast<Key>(300 + i)] = static_cast<Value>(300 + i);
        d.cold[1][static_cast<Key>(200 + i)] = static_cast<Value>(200 + i);
        d.cold[2][static_cast<Key>(100 + i)] = static_cast<Value>(100 + i);
        d.cold[3][static_cast<Key>(i)] = static_cast<Value>(i);
    }
    run_and_verify(d, 4);
}

void test_K2_consecutive_keys()
{
    DistributedData d = make_dd(3);
    d.cold[0][0] = 0;
    d.cold[1][1] = 1;
    d.cold[2][2] = 2;
    d.hot[0][3] = 3;
    d.hot[1][4] = 4;
    d.hot[2][5] = 5;
    run_and_verify(d, 3);
}

void test_K3_extreme_key_values()
{
    DistributedData d = make_dd(2);
    d.cold[0][0] = 42;
    d.hot[1][std::numeric_limits<Key>::max()] = 43;
    run_and_verify(d, 2);
}

void test_K4_large_k()
{
    DistributedData d = make_dd(16);
    for (size_t i = 0; i < 16; ++i) {
        d.cold[i][static_cast<Key>(2 * i)] = static_cast<Value>(i);
        d.hot[i][static_cast<Key>(2 * i + 1)] = static_cast<Value>(i + 100);
    }
    run_and_verify(d, 16);
}

// 境界同値性 / N%ndpus!=0 / cold-hot 跨ぎ の追加テスト.

// N=10, ndpus=3 → bounds = {0, 3, 6}. sorted[0], sorted[3], sorted[6] の key が
// delim になる.
void test_P1_n_not_divisible()
{
    DistributedData d = make_dd(3);
    for (Key i = 0; i < 10; ++i) {
        d.cold[static_cast<size_t>(i) % 3][i * 7] = i;  // key: 0, 7, 14, 21, 28, 35, 42, 49, 56, 63
    }
    run_and_verify(d, 3);

    // 手動で期待 delim key を確認 (0-index 0, 3, 6 の値 → 0, 21, 42)
    DistributedData copy = d;
    auto rb = rebuild_partitioning_nodes(std::move(copy), 3);
    std::vector<Key> base_keys;
    for (const auto& [delim, info] : rb.parts.delims) {
        if (delim.type == DelimType::Base) base_keys.push_back(delim.key);
    }
    std::sort(base_keys.begin(), base_keys.end());
    assert(base_keys.size() == 3);
    assert(base_keys[0] == 0);
    assert(base_keys[1] == 21);
    assert(base_keys[2] == 42);
}

// cold/hot が複数 DPU 間で key 範囲を跨いで混在している配置.
void test_P2_cold_hot_crossing()
{
    DistributedData d = make_dd(4);
    // DPU0 cold: 奇数キー (1,3,5,7), DPU1 hot: 偶数キー (2,4,6,8),
    // DPU2 cold: 大きい値 (100-103), DPU3 hot: 中間値 (50-53)
    for (Key i = 0; i < 4; ++i) {
        d.cold[0][2 * i + 1] = static_cast<Value>(2 * i + 1);
        d.hot[1][2 * (i + 1)] = static_cast<Value>(2 * (i + 1));
        d.cold[2][100 + i] = static_cast<Value>(100 + i);
        d.hot[3][50 + i] = static_cast<Value>(50 + i);
    }
    run_and_verify(d, 4);
}

// 所有権移送: src が完全に空化し, out の総要素数が元 N と一致.
void test_P3_ownership_transfer()
{
    DistributedData d = make_dd(5);
    for (Key i = 0; i < 100; ++i) {
        (i % 2 == 0 ? d.cold : d.hot)[static_cast<size_t>(i) % 5][i * 3 + 1] = i;
    }
    const size_t total = count_total(d);
    assert(total == 100);

    DistributedData moved = std::move(d);
    auto rb = rebuild_partitioning_nodes(std::move(moved), 5);
    assert_src_empty(moved);
    assert(count_total(rb.data) == total);
}

}  // namespace

int main()
{
    test_S1_single_dpu_cold_only();
    test_S2_single_dpu_hot_only();
    test_S3_single_dpu_cold_hot_disjoint();
    test_M1_three_dpus_disjoint();
    test_M2_reverse_dpu_order();
    test_M3_interleaved_cold_hot();
    test_B1_one_dpu_large();
    test_B2_asymmetric_two_dpus();
    test_K1_descending_dpu_id();
    test_K2_consecutive_keys();
    test_K3_extreme_key_values();
    test_K4_large_k();
    test_P1_n_not_divisible();
    test_P2_cold_hot_crossing();
    test_P3_ownership_transfer();
    std::printf("rebuild_partitioning_nodes_test: all 15 tests passed\n");
    return 0;
}
