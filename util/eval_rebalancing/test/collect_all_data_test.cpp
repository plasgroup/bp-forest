#include "collect_all_data.hpp"

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

void verify(const DistributedData& in, const std::vector<KVPair>& out)
{
    size_t expected = 0;
    for (const auto& m : in.cold) expected += m.size();
    for (const auto& m : in.hot) expected += m.size();
    assert(out.size() == expected);

    for (size_t i = 1; i < out.size(); ++i) {
        assert(out[i - 1].key < out[i].key);
    }

    std::vector<std::pair<Key, Value>> expected_pairs;
    expected_pairs.reserve(expected);
    for (const auto& m : in.cold) {
        for (const auto& kv : m) expected_pairs.emplace_back(kv.first, kv.second);
    }
    for (const auto& m : in.hot) {
        for (const auto& kv : m) expected_pairs.emplace_back(kv.first, kv.second);
    }
    std::sort(expected_pairs.begin(), expected_pairs.end());

    std::vector<std::pair<Key, Value>> actual_pairs;
    actual_pairs.reserve(out.size());
    for (const auto& kv : out) actual_pairs.emplace_back(kv.key, kv.value);
    assert(actual_pairs == expected_pairs);
}

void run_and_verify(const DistributedData& in)
{
    const auto out = collect_all_data(in);
    verify(in, out);
}

void test_E1_ndpus_zero()
{
    DistributedData d = make_dd(0);
    const auto out = collect_all_data(d);
    assert(out.empty());
}

void test_E2_all_empty()
{
    DistributedData d = make_dd(3);
    const auto out = collect_all_data(d);
    assert(out.empty());
}

void test_E3_cold_empty_only_hot()
{
    DistributedData d = make_dd(2);
    d.hot[0][10] = 100;
    d.hot[1][20] = 200;
    const auto out = collect_all_data(d);
    assert(out.size() == 2);
    assert(out[0].key == 10 && out[0].value == 100);
    assert(out[1].key == 20 && out[1].value == 200);
    verify(d, out);
}

void test_E4_hot_empty_only_cold()
{
    DistributedData d = make_dd(2);
    d.cold[0][5] = 50;
    d.cold[1][15] = 150;
    const auto out = collect_all_data(d);
    assert(out.size() == 2);
    assert(out[0].key == 5 && out[0].value == 50);
    assert(out[1].key == 15 && out[1].value == 150);
    verify(d, out);
}

void test_S1_single_dpu_cold_only()
{
    DistributedData d = make_dd(1);
    d.cold[0][1] = 10;
    d.cold[0][2] = 20;
    d.cold[0][3] = 30;
    run_and_verify(d);
}

void test_S2_single_dpu_hot_only()
{
    DistributedData d = make_dd(1);
    d.hot[0][1] = 10;
    d.hot[0][2] = 20;
    d.hot[0][3] = 30;
    run_and_verify(d);
}

void test_S3_single_dpu_cold_hot_disjoint()
{
    DistributedData d = make_dd(1);
    d.cold[0][1] = 10;
    d.cold[0][3] = 30;
    d.cold[0][5] = 50;
    d.hot[0][2] = 20;
    d.hot[0][4] = 40;
    run_and_verify(d);
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
    run_and_verify(d);
}

void test_M2_reverse_dpu_order()
{
    DistributedData d = make_dd(3);
    d.cold[0][100] = 1000;
    d.cold[0][200] = 2000;
    d.cold[1][50] = 500;
    d.cold[2][10] = 100;
    d.cold[2][20] = 200;
    run_and_verify(d);
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
    run_and_verify(d);
}

void test_B1_one_dpu_large()
{
    constexpr size_t N = 10000;
    DistributedData d = make_dd(8);
    for (size_t i = 0; i < N; ++i) {
        d.cold[3][static_cast<Key>(i)] = static_cast<Value>(i * 2);
    }
    const auto out = collect_all_data(d);
    assert(out.size() == N);
    for (size_t i = 0; i < N; ++i) {
        assert(out[i].key == static_cast<Key>(i));
        assert(out[i].value == static_cast<Value>(i * 2));
    }
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
    const auto out = collect_all_data(d);
    assert(out.size() == 1003);
    run_and_verify(d);
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
    run_and_verify(d);
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
    run_and_verify(d);
}

void test_K3_extreme_key_values()
{
    DistributedData d = make_dd(2);
    d.cold[0][0] = 42;
    d.hot[1][std::numeric_limits<Key>::max()] = 43;
    const auto out = collect_all_data(d);
    assert(out.size() == 2);
    assert(out[0].key == 0 && out[0].value == 42);
    assert(out[1].key == std::numeric_limits<Key>::max() && out[1].value == 43);
}

void test_K4_large_k()
{
    DistributedData d = make_dd(16);
    for (size_t i = 0; i < 16; ++i) {
        d.cold[i][static_cast<Key>(2 * i)] = static_cast<Value>(i);
        d.hot[i][static_cast<Key>(2 * i + 1)] = static_cast<Value>(i + 100);
    }
    const auto out = collect_all_data(d);
    assert(out.size() == 32);
    for (size_t i = 0; i < 32; ++i) {
        assert(out[i].key == static_cast<Key>(i));
    }
    run_and_verify(d);
}
}  // namespace

int main()
{
    test_E1_ndpus_zero();
    test_E2_all_empty();
    test_E3_cold_empty_only_hot();
    test_E4_hot_empty_only_cold();
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
    std::printf("collect_all_data_test: all 16 tests passed\n");
    return 0;
}
