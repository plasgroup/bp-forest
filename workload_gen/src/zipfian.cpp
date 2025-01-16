#include "piecewise_constant_workload.hpp"
#include "pimtree_query.hpp"
#include "workload_types.h"

#include <cereal/archives/binary.hpp>

#include <cmdline.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <ios>
#include <iostream>
#include <limits>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>


template <class UIntType>
struct ZipfDistribution {
    using result_type = UIntType;

private:
    constexpr double h_integral(double x)
    {
        using std::log;
        const double log_x = log(x);
        return helper2((1.0 - skew) * log_x) * log_x;
    }
    constexpr double h(double x)
    {
        using std::exp;
        using std::log;
        return exp(-skew * log(x));
    }
    constexpr double h_integral_inv(double x)
    {
        using std::exp;
        using std::max;
        return exp(helper1(max({-1.0, x * (1 - skew)})) * x);
    }
    constexpr static double helper1(double x)
    {
        using std::abs;
        using std::log1p;
        if (abs(x) > 1e-8) {
            return log1p(x) / x;
        } else {
            return 1.0 - x * (0.5 - x * (1.0 / 3 - x / 4));
        }
    }
    constexpr static double helper2(double x)
    {
        using std::abs;
        using std::expm1;
        if (abs(x) > 1e-8) {
            return expm1(x) / x;
        } else {
            return 1.0 + x / 2 * (1.0 + x / 3 * (1.0 + x / 4));
        }
    }

    const size_t nr_elems;
    const double skew;
    const double h_integral_x1 = h_integral(1.5) - 1.0;
    const double h_integral_num_elements = h_integral(static_cast<double>(nr_elems) + 0.5);
    const double s = 2.0 - h_integral_inv(h_integral(2.5) - h(2.0));

    std::uniform_real_distribution<double> real_distribution{-h_integral_num_elements, -h_integral_x1};

public:
    ZipfDistribution(size_t nr_elems, double skew) : nr_elems{nr_elems}, skew{skew}
    {
        assert(nr_elems > 0);
        assert(skew > 0);
    }

    template <class URBG>
    constexpr result_type operator()(URBG& g)
    {
        using std::clamp;
        using std::max;
        for (;;) {
            const double u = -real_distribution(g);
            const double x = h_integral_inv(u);
            const UIntType k = clamp<UIntType>(static_cast<UIntType>(x), 1, nr_elems);
            const double k_double = static_cast<double>(k);

            if (k_double - x <= s || u >= h_integral(k_double + 0.5) - h(k_double)) {
                return UIntType{k - 1};
            }
        }
    }
};


int main(int argc, char* argv[])
{
    using RandSeedType = std::random_device::result_type;

    cmdline::parser a;
    a.add<std::string>("file_prefix", 'f', "prefix of the output workload file (including directory path)", true);
    a.add<std::string>("ops", 'o', "kind of generated operations; either of get, insert, pred, scan", true);
    a.add<size_t>("npairs", 'p', "num of generated key-value pairs", false, 100000000);
    a.add<size_t>("nqueries", 'q', "num of generated operations", false, 20000000);
    a.add<uint64_t>("scan_width", 'w', "expected num of key-value pairs in each scan", false, 100);
    a.add<std::string>("zipf_skewness", 't', "zipfian skewness parameter (often called theta)", false, "0.99");
    a.add<uint64_t>("zipf_nr_cands", 'c', "size of candidates of the zipfian dist.", false, 2500);
    a.add("scramble", 's', "whether scramble or not");
    a.add<RandSeedType>("rand_seed", 'r', "seed for random number generator (the default value on the right is chosen randomly each time)", false, std::random_device{}());
    a.add("showinfo", 'v', "show debug info if true");
    a.parse_check(argc, argv);

    operation_t pimtree_op_tag;
    if (a.get<std::string>("ops") == "get") {
        pimtree_op_tag = get_t;
    } else if (a.get<std::string>("ops") == "insert") {
        pimtree_op_tag = insert_t;
    } else if (a.get<std::string>("ops") == "pred") {
        pimtree_op_tag = predecessor_t;
    } else if (a.get<std::string>("ops") == "scan") {
        pimtree_op_tag = scan_t;
    } else {
        fprintf(stderr, "invalid operation type: %s\n", a.get<std::string>("ops").c_str());
        exit(1);
    }

    const std::string file_prefix = a.get<std::string>("file_prefix");
    const std::string ops = a.get<std::string>("ops");
    const size_t npairs = a.get<size_t>("npairs");
    const size_t nqueries = a.get<size_t>("nqueries");
    const uint64_t scan_width = a.get<uint64_t>("scan_width");
    const std::string zipf_skewness_str = a.get<std::string>("zipf_skewness");
    const uint64_t zipf_nr_cands = a.get<uint64_t>("zipf_nr_cands");
    const bool scramble = a.exist("scramble");
    const auto rand_seed = a.get<RandSeedType>("rand_seed");
    const bool showinfo = a.exist("showinfo");

    std::ostringstream ostr_pairs;
    ostr_pairs << file_prefix
               << "init" << npairs
               << ".datasorted";
    const std::string pairs_file_str = ostr_pairs.str();
    std::ostringstream ostr_queries;
    ostr_queries << file_prefix
                 << ops << nqueries
                 << "_slice" << zipf_nr_cands
                 << (scramble ? "_scramble" : "_ordered")
                 << "_skew" << zipf_skewness_str
                 << ".data";
    const std::string queries_file_str = ostr_queries.str();
    ZipfDistribution<uint64_t> zipf_dist{zipf_nr_cands, std::stod(zipf_skewness_str)};
    std::mt19937_64 rand_gen{rand_seed};


    using Clock = std::chrono::system_clock;
    auto timer_start = Clock::now();


    std::uniform_int_distribution<int64_t> key_value_dist;
    std::vector<int64_t> init_keys;
    init_keys.reserve(npairs);
    for (size_t idx_key = 0; idx_key < npairs; idx_key++) {
        const auto now = Clock::now();
        if (now - timer_start > std::chrono::seconds{3}) {
            std::cout << '[' << pairs_file_str << "] " << (idx_key * 100 / npairs) << "% keys genarated" << std::endl;
            timer_start = now;
        }

        init_keys.push_back(key_value_dist(rand_gen));
    }

    std::cout << '[' << pairs_file_str << "] 100% keys genarated" << std::endl;
    std::cout << '[' << pairs_file_str << "] keys being sorted" << std::endl;

    std::sort(init_keys.begin(), init_keys.end());

    std::cout << '[' << pairs_file_str << "] 100% keys sorted" << std::endl;
    std::cout << '[' << pairs_file_str << "] resolving duplication" << std::endl;
    timer_start = Clock::now();

    std::vector<size_t> dup_idxs;
    std::vector<int64_t> appended_keys;
    for (size_t idx_key = 0; idx_key < npairs - 1; idx_key++) {
        if (init_keys[idx_key] == init_keys[idx_key + 1]) {
            dup_idxs.push_back(idx_key);

            int64_t key = key_value_dist(rand_gen);
            while (!std::binary_search(init_keys.cbegin(), init_keys.cend(), key)
                   && std::all_of(appended_keys.cbegin(), appended_keys.cend(), [&](int64_t init_key) { return key != init_key; })) {
                key = key_value_dist(rand_gen);
            }
            appended_keys.push_back(key);
        }
    }

    if (!dup_idxs.empty()) {
        std::vector<int64_t> tmp_keys;
        tmp_keys.swap(init_keys);
        init_keys.reserve(npairs);

        std::sort(appended_keys.begin(), appended_keys.end());

        size_t idx_tmp_key = 0, idx_dup = 0;
        for (const int64_t next_appended : appended_keys) {
            for (; idx_tmp_key < npairs; idx_tmp_key++) {
                const auto now = Clock::now();
                if (now - timer_start > std::chrono::seconds{3}) {
                    std::cout << '[' << pairs_file_str << "] " << (idx_tmp_key * 100 / npairs) << "% duplication resolved" << std::endl;
                    timer_start = now;
                }

                if (idx_dup < dup_idxs.size() && idx_tmp_key == dup_idxs[idx_dup]) {
                    idx_dup++;
                    continue;
                }
                if (tmp_keys[idx_tmp_key] > next_appended) {
                    break;
                }
                init_keys.push_back(tmp_keys[idx_tmp_key]);
            }
            init_keys.push_back(next_appended);
        }
        for (; idx_tmp_key < npairs; idx_tmp_key++) {
            if (idx_dup < dup_idxs.size() && idx_tmp_key == dup_idxs[idx_dup]) {
                idx_dup++;
                continue;
            }
            init_keys.push_back(tmp_keys[idx_tmp_key]);
        }
    }

    std::cout << '[' << pairs_file_str << "] 100% duplication resolved" << std::endl;
    std::cout << '[' << pairs_file_str << "] generating values" << std::endl;

    std::vector<operation> init_ops;
    init_ops.reserve(npairs);
    for (size_t idx_pair = 0; idx_pair < npairs; idx_pair++) {
        const auto now = Clock::now();
        if (now - timer_start > std::chrono::seconds{3}) {
            std::cout << '[' << pairs_file_str << "] " << (idx_pair * 100 / npairs) << "% values genarated" << std::endl;
            timer_start = now;
        }

        operation op;
        op.tsk.i = {init_keys[idx_pair], key_value_dist(rand_gen)};
        op.type = insert_t;
        init_ops.push_back(op);
    }

    std::cout << '[' << pairs_file_str << "] 100% values generated" << std::endl;
    std::cout << '[' << pairs_file_str << "] file being written" << std::endl;

    std::ofstream pairs_file{pairs_file_str, std::ios_base::binary};
    if (!pairs_file) {
        std::cerr << "cannot open file " << pairs_file_str << std::endl;
        return 1;
    }
    pairs_file.write(reinterpret_cast<std::ofstream::char_type*>(&init_ops[0]), static_cast<std::streamsize>(sizeof(operation) * init_ops.size()));
    pairs_file.close();

    std::cout << '[' << pairs_file_str << "] 100% written" << std::endl;


    timer_start = Clock::now();
    std::optional<std::vector<size_t>> count_per_slice;
    if (showinfo) {
        count_per_slice.emplace(zipf_nr_cands);
    }


    std::vector<uint64_t> scramble_mapping(zipf_nr_cands);
    std::iota(scramble_mapping.begin(), scramble_mapping.end(), uint64_t{0});
    if (scramble) {
        std::shuffle(scramble_mapping.begin(), scramble_mapping.end(), rand_gen);
    }

    const uint64_t avg_slice_width = std::numeric_limits<uint64_t>::max() / zipf_nr_cands,
                   remainder_width = std::numeric_limits<uint64_t>::max() - avg_slice_width * zipf_nr_cands + 1;
    std::vector<operation> query_ops;
    query_ops.reserve(nqueries);
    for (size_t idx_query = 0; idx_query < nqueries; idx_query++) {
        const auto now = Clock::now();
        if (now - timer_start > std::chrono::seconds{3}) {
            std::cout << '[' << queries_file_str << "] " << (idx_query * 100 / nqueries) << "% queries genarated" << std::endl;
            timer_start = now;
        }

        int64_t key;
        const uint64_t idx_slice = scramble_mapping[zipf_dist(rand_gen)];
        if (showinfo) {
            (*count_per_slice)[idx_slice]++;
        }

        switch (pimtree_op_tag) {
        case get_t: {
            std::uniform_int_distribution<size_t> pair_dist{idx_slice * npairs / zipf_nr_cands, (idx_slice + 1) * npairs / zipf_nr_cands - 1};
            const size_t idx_pair = pair_dist(rand_gen);
            key = init_keys[idx_pair];
        } break;
        case predecessor_t:
        case scan_t:
        case insert_t: {
            const int64_t range_begin = std::numeric_limits<int64_t>::min()
                                        + static_cast<int64_t>(avg_slice_width * idx_slice
                                                               + std::min(idx_slice, remainder_width)),
                          range_end = (idx_slice + 1 == zipf_nr_cands ? std::numeric_limits<int64_t>::max()
                                                                      : std::numeric_limits<int64_t>::min()
                                                                            + static_cast<int64_t>(avg_slice_width * (idx_slice + 1)
                                                                                                   - 1
                                                                                                   + std::min(idx_slice + 1, remainder_width)));
            std::uniform_int_distribution<int64_t> key_dist{range_begin, range_end};
            key = key_dist(rand_gen);
        } break;
        default:
            // unreachable
            std::exit(1);
        }

        operation op;
        switch (pimtree_op_tag) {
        case get_t:
            op.tsk.g.key = key;
            break;
        case predecessor_t:
            op.tsk.p.key = key;
            break;
        case scan_t: {
            op.tsk.s.lkey = key;
            const uint64_t scan_range_width = std::numeric_limits<uint64_t>::max() / scan_width;
            op.tsk.s.rkey = std::min(key, std::numeric_limits<int64_t>::max() - static_cast<int64_t>(scan_range_width)) + static_cast<int64_t>(scan_range_width);
        } break;
        case insert_t: {
            op.tsk.i.key = key;
            op.tsk.i.value = key_value_dist(rand_gen);
        } break;
        default:
            // unreachable
            std::exit(1);
        }
        op.type = pimtree_op_tag;

        query_ops.push_back(op);
    }

    std::cout << '[' << queries_file_str << "] 100% queries generated" << std::endl;
    std::cout << '[' << queries_file_str << "] file being written" << std::endl;

    std::ofstream queries_file{queries_file_str, std::ios_base::binary};
    if (!queries_file) {
        std::cerr << "cannot open file " << queries_file_str << std::endl;
        return 1;
    }
    queries_file.write(reinterpret_cast<std::ofstream::char_type*>(&query_ops[0]), static_cast<std::streamsize>(sizeof(operation) * query_ops.size()));
    queries_file.close();

    std::cout << '[' << queries_file_str << "] 100% written" << std::endl;


    if (showinfo) {
        for (uint64_t i = 0; i < zipf_nr_cands; i++) {
            std::cout << "num keys in " << i << "th range: " << (*count_per_slice)[i] << ", " << 100 * static_cast<double>((*count_per_slice)[i]) / (double)nqueries << "%" << std::endl;
        }
    }
    return 0;
}
