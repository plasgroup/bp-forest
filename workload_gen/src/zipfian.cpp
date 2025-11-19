#include "piecewise_constant_workload.hpp"
#include "pimtree_query.hpp"
#include "workload_types.h"

#include "extendable_buffer.hpp"
#include "parallel.hpp"

#include <cereal/archives/binary.hpp>

#include <cmdline.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <initializer_list>
#include <ios>
#include <iostream>
#include <limits>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>


// from https://prng.di.unimi.it/xoshiro256plusplus.c
struct xoshiro256pp {
    using result_type = uint64_t;

    explicit xoshiro256pp(result_type seed);
    constexpr result_type operator()();
    constexpr static result_type min() { return 0; }
    constexpr static result_type max() { return std::numeric_limits<result_type>::max(); }

    // equivalent to 2^128 calls to operator()
    constexpr void jump();

private:
    static constexpr uint64_t uint64_rotl(uint64_t x, int k);

    std::array<uint64_t, 4> states;
};
inline xoshiro256pp::xoshiro256pp(result_type seed)
{
    for (auto& state : states) {
        // SplitMix64 from https://prng.di.unimi.it/splitmix64.c
        uint64_t z = (seed += 0x9e3779b97f4a7c15);
        z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
        z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
        state = z ^ (z >> 31);
    }
}
inline constexpr auto xoshiro256pp::operator()() -> result_type
{
    const uint64_t result = uint64_rotl(states[0] + states[3], 23) + states[0];

    const uint64_t t = states[1] << 17;

    states[2] ^= states[0];
    states[3] ^= states[1];
    states[1] ^= states[2];
    states[0] ^= states[3];

    states[2] ^= t;

    states[3] = uint64_rotl(states[3], 45);

    return result;
}
inline constexpr void xoshiro256pp::jump()
{
    constexpr uint64_t JUMP[] = {0x180ec6d33cfd0aba, 0xd5a61266f0c9392c, 0xa9582618e03fc9aa, 0x39abdc4529b1661c};

    uint64_t s0 = 0;
    uint64_t s1 = 0;
    uint64_t s2 = 0;
    uint64_t s3 = 0;
    for (unsigned i = 0; i < sizeof(JUMP) / sizeof(JUMP[0]); i++)
        for (int b = 0; b < 64; b++) {
            if (JUMP[i] & UINT64_C(1) << b) {
                s0 ^= states[0];
                s1 ^= states[1];
                s2 ^= states[2];
                s3 ^= states[3];
            }
            (*this)();
        }

    states[0] = s0;
    states[1] = s1;
    states[2] = s2;
    states[3] = s3;
}
inline constexpr uint64_t xoshiro256pp::uint64_rotl(uint64_t x, int k)
{
    return (x << k) | (x >> (64 - k));
}


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
        assert(skew >= 0);
    }

    template <class URBG>
    constexpr result_type operator()(URBG& g)
    {
        using std::clamp;
        using std::max;
        for (;;) {
            const double u = -real_distribution(g);
            const double x = h_integral_inv(u);
            const UIntType k = clamp<UIntType>(static_cast<UIntType>(x + 0.5), 1, nr_elems);
            const double k_double = static_cast<double>(k);

            if (k_double - x <= s || u >= h_integral(k_double + 0.5) - h(k_double)) {
                return UIntType{k - 1};
            }
        }
    }
};


template <class UIntType>
struct DegenerateDistribution {
    using result_type = UIntType;

private:
    const UIntType point;

public:
    DegenerateDistribution(UIntType point) : point{point} {}

    template <class URBG>
    constexpr result_type operator()(URBG& /* g */)
    {
        return point;
    }
};


template <class... Components>
struct MixtureDistribution {
    static_assert(sizeof...(Components) > 0);
    using result_type = std::common_type_t<typename Components::result_type...>;
    using components_tuple = std::tuple<Components...>;
    using weights = std::array<double, sizeof...(Components)>;

private:
    std::discrete_distribution<size_t> choice;
    components_tuple components;

public:
    MixtureDistribution(const weights& weights, Components&&... components)
        : choice{weights.cbegin(), weights.cend()},
          components{std::forward<Components>(components)...}
    {
    }

    template <class URBG>
    constexpr result_type operator()(URBG& g)
    {
        return invoke_helper(g, choice(g), std::make_index_sequence<sizeof...(Components)>{});
    }

private:
    template <class URBG, size_t... Is>
    constexpr result_type invoke_helper(URBG& g, size_t idx_component, std::integer_sequence<size_t, Is...>)
    {
        result_type res;
        std::initializer_list<int> tmp{(idx_component == Is ? (res = std::get<Is>(components)(g), 0) : 0)...};
        return res;
    }
};


struct CMDOpt {
    using RandSeedType = std::random_device::result_type;

    std::string pairs_file_str;
    std::string queries_file_str;
    size_t npairs;
    size_t nqueries;
    operation_t pimtree_op_tag;
    uint64_t scan_width;
    uint64_t zipf_nr_cands;
    bool showinfo;
    bool noinit;

    double zipf_skewness;
    std::optional<double> hot_spike_ratio;
    bool scramble;
    RandSeedType rand_seed;
    unsigned nthreads;

    CMDOpt(int argc, char* argv[])
    {
        cmdline::parser parser;
        parser.add<std::string>("file_prefix", 'f', "prefix of the output workload file (including directory path)", true);
        parser.add<size_t>("npairs", 'p', "num of generated key-value pairs", false, 100000000);
        parser.add<size_t>("nqueries", 'q', "num of generated operations", false, 20000000);
        parser.add<std::string>("ops", 'o', "kind of generated operations; either of get, insert, delete, pred, scan", true, "", cmdline::oneof<std::string>("get", "insert", "delete", "pred", "scan"));
        parser.add<uint64_t>("scan_width", 'w', "expected num of key-value pairs in each scan", false, 100);
        parser.add<std::string>("zipf_skewness", 'z', "zipfian skewness parameter (often called theta)", false, "0.99");
        parser.add<uint64_t>("zipf_nr_cands", 'c', "size of candidates of the zipfian dist.", false, 2500);
        parser.add<std::string>("hot_spike_ratio", 'h', "add a spike of hotness to the least popular slice (must be in [0.0, 1.0])", false, "");
        parser.add("scramble", 's', "whether scramble or not");
        parser.add<RandSeedType>("rand_seed", 'r', "seed for random number generator (the default value on the right is chosen randomly each time)", false, std::random_device{}());
        parser.add<unsigned>("num_threads", 't', "num of threads", false, std::numeric_limits<unsigned>::max());
        parser.add("showinfo", 'v', "show debug info if true");
        parser.add("noinit", 0, "do not create init key-value pairs");
        parser.parse_check(argc, argv);

        const std::string file_prefix = parser.get<std::string>("file_prefix");
        npairs = parser.get<size_t>("npairs");
        nqueries = parser.get<size_t>("nqueries");
        const std::string ops = parser.get<std::string>("ops");
        scan_width = parser.get<uint64_t>("scan_width");
        const std::string zipf_skewness_str = parser.get<std::string>("zipf_skewness");
        zipf_nr_cands = parser.get<uint64_t>("zipf_nr_cands");
        const std::string hot_spike_ratio_str = parser.get<std::string>("hot_spike_ratio");
        scramble = parser.exist("scramble");
        rand_seed = parser.get<RandSeedType>("rand_seed");
        nthreads = parser.get<unsigned>("num_threads");
        showinfo = parser.exist("showinfo");
        noinit = parser.exist("noinit");

        const bool has_hot_spike = !hot_spike_ratio_str.empty();
        if (has_hot_spike) {
            hot_spike_ratio = std::stod(hot_spike_ratio_str);
            if (*hot_spike_ratio < 0.0 || *hot_spike_ratio > 1.0) {
                std::cerr << "hot_spike_ratio must be in [0.0, 1.0]" << std::endl;
                throw cmdline::cmdline_error{""};
            }
        }

        std::ostringstream ostr_pairs;
        ostr_pairs << file_prefix
                   << "init" << npairs
                   << ".datasorted";
        pairs_file_str = ostr_pairs.str();
        std::ostringstream ostr_queries;
        ostr_queries << file_prefix
                     << ops << nqueries
                     << "_item" << npairs
                     << "_slice" << zipf_nr_cands
                     << (scramble ? "_scramble" : "_ordered")
                     << "_skew" << zipf_skewness_str
                     << (has_hot_spike ? ("_hotspike" + hot_spike_ratio_str) : "")
                     << ".data";
        queries_file_str = ostr_queries.str();

        if (ops == "get") {
            pimtree_op_tag = get_t;
        } else if (ops == "insert") {
            pimtree_op_tag = insert_t;
        } else if (ops == "delete") {
            pimtree_op_tag = remove_t;
        } else if (ops == "pred") {
            pimtree_op_tag = predecessor_t;
        } else if (ops == "scan") {
            pimtree_op_tag = scan_t;
        }

        zipf_skewness = std::stod(zipf_skewness_str);

        if (nthreads == std::numeric_limits<unsigned>::max()) {
            nthreads = std::thread::hardware_concurrency() + 1;
        }
        if (nthreads == 0) {
            nthreads = 4;
        }
    }
};


template <typename Func>
void create_slice_dict(const CMDOpt& opt, Func&& func)
{
    if (opt.hot_spike_ratio) {
        MixtureDistribution<DegenerateDistribution<size_t>, ZipfDistribution<size_t>> mix_dist(
            {*opt.hot_spike_ratio, 1.0 - *opt.hot_spike_ratio},
            DegenerateDistribution<size_t>{opt.zipf_nr_cands - 1},
            ZipfDistribution<size_t>{opt.zipf_nr_cands, opt.zipf_skewness});
        std::forward<Func>(func)(mix_dist);
    } else {
        ZipfDistribution<size_t> zipf_dist{opt.zipf_nr_cands, opt.zipf_skewness};
        std::forward<Func>(func)(zipf_dist);
    }
}

template <class SliceDist>
struct WorkloadGen;
template <class SliceDist>
static WorkloadGen<SliceDist> instantiate_workload_gen(const CMDOpt& opt, const SliceDist& slice_dist);

template <class SliceDist>
struct WorkloadGen : ParallelManager<WorkloadGen<SliceDist>> {
    friend WorkloadGen<SliceDist> instantiate_workload_gen<SliceDist>(const CMDOpt& opt, const SliceDist& slice_dist);

private:
    explicit WorkloadGen(
        const CMDOpt& opt,
        const SliceDist& slice_dist,
        std::vector<uint64_t>&& scramble_mapping,
        std::vector<xoshiro256pp>&& rand_gens)
        : ParallelManager<WorkloadGen>(opt.nthreads),
          pairs_file_str{opt.pairs_file_str},
          queries_file_str{opt.queries_file_str},
          npairs{opt.npairs},
          nqueries{opt.nqueries},
          pimtree_op_tag{opt.pimtree_op_tag},
          scan_width{opt.scan_width},
          zipf_nr_cands{opt.zipf_nr_cands},
          showinfo{opt.showinfo},
          noinit{opt.noinit},
          slice_dist{slice_dist},
          scramble_mapping{std::move(scramble_mapping)},
          rand_gens{std::move(rand_gens)}
    {
    }

    const std::string pairs_file_str;
    const std::string queries_file_str;
    const size_t npairs;
    const size_t nqueries;
    const operation_t pimtree_op_tag;
    const uint64_t scan_width;
    const uint64_t zipf_nr_cands;
    const bool showinfo;
    const bool noinit;

    SliceDist slice_dist;
    const std::vector<uint64_t> scramble_mapping;
    std::vector<xoshiro256pp> rand_gens;

    using Clock = std::chrono::system_clock;

    ExtendableBuffer<int64_t> init_keys;
    std::uniform_int_distribution<int64_t> key_dist{std::numeric_limits<int64_t>::min()};
    std::uniform_int_distribution<int64_t> value_dist{std::numeric_limits<int64_t>::min() + 1};
    unsigned merge_step;
    ExtendableBuffer<operation> init_ops;
    std::vector<std::vector<size_t>> counts_per_slice;
    ExtendableBuffer<operation> query_ops;
    std::vector<std::uniform_int_distribution<size_t>> query_item_dists;
    std::vector<std::uniform_int_distribution<int64_t>> query_key_dists;

    using ParallelManager<WorkloadGen<SliceDist>>::get_parallelism;
    using ParallelManager<WorkloadGen<SliceDist>>::parallel_run;

public:
    void generate_init_keys_impl(unsigned tid);
    void sort_init_keys_partially(unsigned tid);
    void merge_init_keys(unsigned tid);
    void generate_values_impl(unsigned tid);
    template <operation_t op_tag>
    void generate_queries_impl(unsigned tid);

    void operator()();
};
template <class SliceDist>
void WorkloadGen<SliceDist>::generate_init_keys_impl(unsigned tid)
{
    auto timer_start = Clock::now();
    const size_t idx_key_begin = npairs * tid / get_parallelism(),
                 idx_key_end = npairs * (tid + 1) / get_parallelism();
    for (size_t idx_key = idx_key_begin; idx_key < idx_key_end; idx_key++) {
        if (tid == 0 && idx_key % 0x10000 == 0) {
            const auto now = Clock::now();
            if (now - timer_start > std::chrono::seconds{3}) {
                std::cout << '[' << pairs_file_str << "] " << (idx_key * 100 / idx_key_end) << "% keys generated" << std::endl;
                timer_start = now;
            }
        }

        init_keys[idx_key] = key_dist(rand_gens[tid]);
    }
}
template <class SliceDist>
void WorkloadGen<SliceDist>::sort_init_keys_partially(unsigned tid)
{
    const size_t idx_key_begin = npairs * tid / get_parallelism(),
                 idx_key_end = npairs * (tid + 1) / get_parallelism();
    std::sort(&init_keys[idx_key_begin], &init_keys[idx_key_end]);
}
template <class SliceDist>
void WorkloadGen<SliceDist>::merge_init_keys(unsigned tid)
{
    const unsigned merge_window_size = 2u << merge_step;
    if (tid % merge_window_size == 0) {
        const unsigned tid_prev_window_end = tid + merge_window_size / 2u,
                       tid_window_end = std::min(tid + merge_window_size, get_parallelism());
        if (tid_prev_window_end < tid_window_end) {
            std::inplace_merge(
                &init_keys[npairs * tid / get_parallelism()],
                &init_keys[npairs * tid_prev_window_end / get_parallelism()],
                &init_keys[npairs * tid_window_end / get_parallelism()]);
        }
    }
}
template <class SliceDist>
void WorkloadGen<SliceDist>::generate_values_impl(unsigned tid)
{
    auto timer_start = Clock::now();
    const size_t idx_pair_begin = npairs * tid / get_parallelism(),
                 idx_pair_end = npairs * (tid + 1) / get_parallelism();
    for (size_t idx_pair = idx_pair_begin; idx_pair < idx_pair_end; idx_pair++) {
        if (tid == 0 && idx_pair % 0x10000 == 0) {
            const auto now = Clock::now();
            if (now - timer_start > std::chrono::seconds{3}) {
                std::cout << '[' << pairs_file_str << "] " << (idx_pair * 100 / idx_pair_end) << "% values generated" << std::endl;
                timer_start = now;
            }
        }

        operation& op = init_ops[idx_pair];
        op.tsk.i = {init_keys[idx_pair], value_dist(rand_gens[tid])};
        op.type = insert_t;
    }
}
template <class SliceDist>
template <operation_t op_tag>
void WorkloadGen<SliceDist>::generate_queries_impl(const unsigned tid)
{
    auto timer_start = Clock::now();
    const size_t idx_query_begin = nqueries * tid / get_parallelism(),
                 idx_query_end = nqueries * (tid + 1) / get_parallelism();

    if constexpr (op_tag == get_t || op_tag == remove_t) {
        for (size_t idx_query = idx_query_begin; idx_query < idx_query_end; idx_query++) {
            if (tid == 0 && idx_query % 0x10000 == 0) {
                const auto now = Clock::now();
                if (now - timer_start > std::chrono::seconds{3}) {
                    std::cout << '[' << queries_file_str << "] " << (idx_query * 100 / idx_query_begin) << "% queries generated" << std::endl;
                    timer_start = now;
                }
            }

            const uint64_t idx_slice = scramble_mapping[slice_dist(rand_gens[tid])];
            if (showinfo) {
                counts_per_slice[tid][idx_slice]++;
            }

            operation& op = query_ops[idx_query];
            auto tmp = query_item_dists[idx_slice](rand_gens[tid]);
            if (tmp >= npairs)
                std::cout << "tmp = " << tmp << ", npairs = " << npairs << std::endl;
            if constexpr (op_tag == get_t) {
                op.tsk.g.key = init_keys[tmp];
            } else {
                op.tsk.r.key = init_keys[tmp];
            }
            op.type = op_tag;
        }
    } else {
        const uint64_t scan_range_width = std::numeric_limits<uint64_t>::max() / npairs * scan_width;
        for (size_t idx_query = idx_query_begin; idx_query < idx_query_end; idx_query++) {
            if (tid == 0 && idx_query % 0x10000 == 0) {
                const auto now = Clock::now();
                if (now - timer_start > std::chrono::seconds{3}) {
                    std::cout << '[' << queries_file_str << "] " << (idx_query * 100 / idx_query_end) << "% queries generated" << std::endl;
                    timer_start = now;
                }
            }

            const uint64_t idx_slice = scramble_mapping[slice_dist(rand_gens[tid])];
            if (showinfo) {
                counts_per_slice[tid][idx_slice]++;
            }
            const int64_t key = query_key_dists[idx_slice](rand_gens[tid]);

            operation& op = query_ops[idx_query];
            switch (op_tag) {
            case predecessor_t:
                op.tsk.p.key = key;
                break;
            case scan_t: {
                op.tsk.s.lkey = key;
                op.tsk.s.rkey = std::min(key, std::numeric_limits<int64_t>::max() - static_cast<int64_t>(scan_range_width)) + static_cast<int64_t>(scan_range_width);
            } break;
            case insert_t: {
                op.tsk.i.key = key;
                op.tsk.i.value = value_dist(rand_gens[tid]);
            } break;
            default:
                // unreachable
                std::exit(1);
            }
            op.type = op_tag;
        }
    }
}
template <class SliceDist>
void WorkloadGen<SliceDist>::operator()()
{
    if (!noinit || pimtree_op_tag == get_t || pimtree_op_tag == remove_t) {
        init_keys.reserve(npairs);
        parallel_run(&WorkloadGen::generate_init_keys_impl);
        std::cout << '[' << pairs_file_str << "] 100% keys generated" << std::endl;


        std::cout << '[' << pairs_file_str << "] keys being sorted" << std::endl;
        parallel_run(&WorkloadGen::sort_init_keys_partially);
        for (merge_step = 0; (1u << merge_step) < get_parallelism(); merge_step++) {
            parallel_run(&WorkloadGen::merge_init_keys);
        }
        std::cout << '[' << pairs_file_str << "] 100% keys sorted" << std::endl;


        std::cout << '[' << pairs_file_str << "] resolving duplication" << std::endl;
        {
            std::vector<size_t> dup_idxs;
            std::vector<int64_t> appended_keys;
            for (size_t idx_key = 0; idx_key < npairs - 1; idx_key++) {
                if (init_keys[idx_key] == init_keys[idx_key + 1]) {
                    dup_idxs.push_back(idx_key);

                    int64_t key = key_dist(rand_gens[0]);
                    while (std::binary_search(&init_keys[0], &init_keys[npairs], key)
                           || std::any_of(appended_keys.cbegin(), appended_keys.cend(), [&](int64_t init_key) { return key == init_key; })) {
                        key = key_dist(rand_gens[0]);
                    }
                    appended_keys.push_back(key);
                }
            }

            if (!dup_idxs.empty()) {
                ExtendableBuffer<int64_t> tmp_keys;
                tmp_keys.swap(init_keys);
                init_keys.reserve(npairs);

                std::sort(appended_keys.begin(), appended_keys.end());

                size_t idx_init_key = 0, idx_tmp_key = 0, idx_dup = 0;
                for (const int64_t next_appended : appended_keys) {
                    for (; idx_tmp_key < npairs; idx_tmp_key++) {
                        if (idx_dup < dup_idxs.size() && idx_tmp_key == dup_idxs[idx_dup]) {
                            idx_dup++;
                            continue;
                        }
                        if (tmp_keys[idx_tmp_key] > next_appended) {
                            break;
                        }
                        init_keys[idx_init_key] = tmp_keys[idx_tmp_key];
                        idx_init_key++;
                    }
                    init_keys[idx_init_key] = next_appended;
                    idx_init_key++;
                }
                for (; idx_tmp_key < npairs; idx_tmp_key++) {
                    if (idx_dup < dup_idxs.size() && idx_tmp_key == dup_idxs[idx_dup]) {
                        idx_dup++;
                        continue;
                    }
                    init_keys[idx_init_key] = tmp_keys[idx_tmp_key];
                    idx_init_key++;
                }
            }
        }
        std::cout << '[' << pairs_file_str << "] 100% duplication resolved" << std::endl;
    }


    if (!noinit) {
        std::cout << '[' << pairs_file_str << "] generating values" << std::endl;
        init_ops.reserve(npairs);
        parallel_run(&WorkloadGen::generate_values_impl);
        std::cout << '[' << pairs_file_str << "] 100% values generated" << std::endl;


        std::cout << '[' << pairs_file_str << "] file being written" << std::endl;
        {
            std::ofstream pairs_file{pairs_file_str, std::ios_base::binary};
            if (!pairs_file) {
                std::cerr << "cannot open file " << pairs_file_str << std::endl;
                std::exit(1);
            }
            pairs_file.write(reinterpret_cast<std::ofstream::char_type*>(&init_ops[0]), static_cast<std::streamsize>(sizeof(operation) * npairs));
        }
        std::cout << '[' << pairs_file_str << "] 100% written" << std::endl;
        init_ops.reclaim();
    }


    if (showinfo) {
        counts_per_slice.resize(get_parallelism());
        for (unsigned tid = 0; tid < get_parallelism(); tid++) {
            counts_per_slice[tid].clear();
            counts_per_slice[tid].resize(zipf_nr_cands);
        }
    }

    query_ops.reserve(nqueries);
    if (pimtree_op_tag == get_t || pimtree_op_tag == remove_t) {
        query_item_dists.clear();
        query_item_dists.reserve(zipf_nr_cands);
        for (size_t idx_slice = 0; idx_slice < zipf_nr_cands; idx_slice++) {
            query_item_dists.emplace_back(
                /* min */ idx_slice * npairs / zipf_nr_cands,
                /* max */ (idx_slice + 1) * npairs / zipf_nr_cands - 1);
            if (showinfo) {
                std::cout << idx_slice << "th range = [" << query_item_dists.back().min() << ", " << query_item_dists.back().max() << ']' << std::endl;
            }
        }
        switch (pimtree_op_tag) {
        case get_t:
            parallel_run(&WorkloadGen::generate_queries_impl<get_t>);
            break;
        case remove_t:
            parallel_run(&WorkloadGen::generate_queries_impl<remove_t>);
            break;
        default:
            // unreachable
            std::exit(1);
        }

    } else {
        query_key_dists.clear();
        query_key_dists.reserve(zipf_nr_cands);
        const uint64_t avg_slice_width = std::numeric_limits<uint64_t>::max() / zipf_nr_cands,
                       remainder_width = std::numeric_limits<uint64_t>::max() - avg_slice_width * zipf_nr_cands + 1;
        int64_t range_begin = std::numeric_limits<int64_t>::min();
        for (size_t idx_slice = 0; idx_slice < zipf_nr_cands; idx_slice++) {
            if (idx_slice + 1 != zipf_nr_cands) {
                const int64_t range_end = static_cast<int64_t>(static_cast<uint64_t>(std::numeric_limits<int64_t>::min())
                                                               + avg_slice_width * (idx_slice + 1)
                                                               - 1
                                                               + std::min(idx_slice + 1, remainder_width));
                query_key_dists.emplace_back(range_begin, range_end);
                range_begin = range_end + 1;
            } else {
                const int64_t range_end = std::numeric_limits<int64_t>::max();
                query_key_dists.emplace_back(range_begin, range_end);
            }
            if (showinfo) {
                std::cout << idx_slice << "th range = [" << query_key_dists.back().min() << ", " << query_key_dists.back().max() << ']' << std::endl;
            }
        }
        switch (pimtree_op_tag) {
        case predecessor_t:
            parallel_run(&WorkloadGen::generate_queries_impl<predecessor_t>);
            break;
        case scan_t:
            parallel_run(&WorkloadGen::generate_queries_impl<scan_t>);
            break;
        case insert_t:
            parallel_run(&WorkloadGen::generate_queries_impl<insert_t>);
            break;
        default:
            // unreachable
            std::exit(1);
        }
    }
    std::cout << '[' << queries_file_str << "] 100% queries generated" << std::endl;


    std::cout << '[' << queries_file_str << "] file being written" << std::endl;
    {
        std::ofstream queries_file{queries_file_str, std::ios_base::binary};
        if (!queries_file) {
            std::cerr << "cannot open file " << queries_file_str << std::endl;
            std::exit(1);
        }
        queries_file.write(reinterpret_cast<std::ofstream::char_type*>(&query_ops[0]), static_cast<std::streamsize>(sizeof(operation) * nqueries));
    }
    std::cout << '[' << queries_file_str << "] 100% written" << std::endl;
    init_keys.reclaim();
    query_ops.reclaim();


    if (showinfo) {
        for (uint64_t i = 0; i < zipf_nr_cands; i++) {
            const size_t count = std::accumulate(counts_per_slice.cbegin(), counts_per_slice.cend(), size_t{0},
                [i](size_t tmp, auto& vec) { return tmp + vec[i]; });
            std::cout << "num keys in " << i << "th range: " << count << ", " << 100 * static_cast<double>(count) / (double)nqueries << "%" << std::endl;
        }
    }
}


template <class SliceDist>
static WorkloadGen<SliceDist> instantiate_workload_gen(const CMDOpt& opt, const SliceDist& slice_dist)
{
    xoshiro256pp rand_gen{opt.rand_seed};
    std::vector<xoshiro256pp> rand_gens;
    for (unsigned tid = 0; tid < opt.nthreads; tid++) {
        rand_gens.emplace_back(rand_gen);
        rand_gen.jump();
    }

    std::vector<uint64_t> scramble_mapping(opt.zipf_nr_cands);
    std::iota(scramble_mapping.begin(), scramble_mapping.end(), uint64_t{0});
    if (opt.scramble) {
        std::shuffle(scramble_mapping.begin(), scramble_mapping.end(), rand_gen);
    }

    return WorkloadGen{opt, slice_dist, std::move(scramble_mapping), std::move(rand_gens)};
}


int main(int argc, char* argv[])
{
    CMDOpt opt{argc, argv};
    create_slice_dict(opt, [&](auto&& slice_dist) {
        auto workload_gen = instantiate_workload_gen(opt, std::forward<decltype(slice_dist)>(slice_dist));
        (workload_gen)();
    });
    return 0;
}
