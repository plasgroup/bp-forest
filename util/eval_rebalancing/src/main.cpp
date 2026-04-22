#define NR_RANKS 10

#include "assert.hpp"
#include "collect_all_data.hpp"
#include "common.h"
#include "filesystem.hpp"
#include "host_params.hpp"
#include "noise_params.hpp"
#include "overload.hpp"
#include "pimtree_query.hpp"

#include <cmdline.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>


namespace cmdline
{
template <typename T>
struct default_reader<std::optional<T>> {
    std::optional<T> operator()(const std::string& str)
    {
        return default_reader<T>{}(str);
    }
};
namespace detail
{
template <typename T>
class lexical_cast_t<std::string, std::optional<T>, false>
{
public:
    static std::string cast(const std::optional<T>& opt)
    {
        return opt ? lexical_cast<std::string>(*opt) : "(nullopt)";
    }
};
}  // namespace detail
}  // namespace cmdline


struct CMDOpt {
    std::string data_file;
    std::string workload_file;
    std::variant<size_t /* fixed batch size */, double /* Poisson query rate (op/s) */> batch_spec;
    unsigned ndpus{};
    unsigned balancing = 10;
    std::variant<double /* legacy hwm ratio */, NoiseParams /* noise-aware */> threshold_spec;
    bool commutative = false;
    bool enable_incremental = true;

    CMDOpt() = default;

    CMDOpt(int argc, char* argv[])
    {
        cmdline::parser parser;
        parser.add<std::string>("data", 'd', "insert_t operations binary file path (sorted by key)", true);
        parser.add<std::string>("workload", 'w', "PIM-Tree workload file path", true);
        parser.add<std::optional<size_t>>("batch-size", 'b', "fixed queries per batch", false);
        parser.add<std::optional<double>>("query-rate", 0, "average query arrival rate (op/s), Poisson sampled", false);
        parser.add<unsigned>("ndpus", 'n', "number of base DPUs", true);
        parser.add<unsigned>("balancing", 0, "balancing factor", false, 10);
        parser.add<std::optional<double>>("fp-rate", 0, "target per-batch false-positive rate for overload detection: the probability of firing rebalance when the load is actually balanced. Mutually exclusive with --hwm-ratio. Default: 0.001.", false);
        parser.add<std::optional<double>>("hwm-ratio", 0, "legacy overload threshold = per-DPU goal * r. Mutually exclusive with --fp-rate.", false);
        parser.add("commutative", 'c', "treat scans as commutative");
        parser.add<bool>("incremental", 0, "whether to enable incremental rebalancing", false, true);
        parser.parse_check(argc, argv);

        data_file = parser.get<std::string>("data");
        workload_file = parser.get<std::string>("workload");
        ndpus = parser.get<unsigned>("ndpus");
        balancing = parser.get<unsigned>("balancing");
        commutative = parser.exist("commutative");
        enable_incremental = parser.get<bool>("incremental");

        const auto fpr = parser.get<std::optional<double>>("fp-rate");
        const auto hwm = parser.get<std::optional<double>>("hwm-ratio");
        if (fpr && hwm) {
            std::cerr << "must not set both --fp-rate and --hwm-ratio" << std::endl;
            std::exit(1);
        }
        if (hwm) {
            if (!(*hwm >= 1.0)) {
                std::cerr << "--hwm-ratio must be >= 1.0" << std::endl;
                std::exit(1);
            }
            threshold_spec = *hwm;
        } else {
            const double fp_rate = fpr.value_or(0.001);
            if (!(fp_rate > 0.0 && fp_rate < 1.0)) {
                std::cerr << "--fp-rate must be in (0, 1)" << std::endl;
                std::exit(1);
            }
            threshold_spec = NoiseParams::compute(fp_rate, ndpus, balancing);
        }

        const auto bs = parser.get<std::optional<size_t>>("batch-size");
        const auto qr = parser.get<std::optional<double>>("query-rate");
        if (bs && qr) {
            std::cerr << "must not set both --batch-size and --query-rate" << std::endl;
            std::exit(1);
        }
        if (bs) {
            batch_spec = *bs;
        } else if (qr) {
            batch_spec = *qr;
        } else {
            std::cerr << "must set either --batch-size or --query-rate" << std::endl;
            std::exit(1);
        }
    }
} opt;


using Dur = std::chrono::nanoseconds;

enum class DelimType {
    Base,
    Cold,
    Hot,
};
struct Delim {
    Key key;
    DelimType type;

    friend bool operator<(const Delim& lhs, const Delim& rhs)
    {
        return std::tie(lhs.key, lhs.type) < std::tie(rhs.key, rhs.type);
    }
};
struct PartInfo {
    unsigned dpu;
};
using Delims = std::map<Delim, PartInfo>;

struct Partitioning {
    Delims delims;

    unsigned ndpus() const
    {
        const auto it = std::find_if(delims.rbegin(), delims.rend(), [](const auto& delim) { return delim.first.type == DelimType::Base; });
        ASSERT(it != delims.rend());
        return it->second.dpu + 1;
    }
    Delims::const_iterator get_base(Delims::const_iterator it) const
    {
        while (it != delims.begin() && it->first.type != DelimType::Base) {
            --it;
        }
        ASSERT(it->first.type == DelimType::Base);
        return it;
    }
};

template <typename Query>
struct RoutedQueries {
    struct PerDpu {
        std::vector<Query> sub_qrys;
        std::vector<size_t> orig_idxs;
    };

    std::vector<PerDpu> cold, hot;
};

template <typename Query>
struct QueryHandler;

std::vector<KVPair> load_kv_data(const std::string& path);
Partitioning equal_data_partitions(unsigned ndpus, const KVPair sorted_data[], size_t ndata);
Partitioning equal_data_partitions(unsigned ndpus, const std::vector<MapNode>& sorted);
DistributedData apply_partitioning(const Partitioning& parts, const KVPair sorted_data[], size_t ndata);
DistributedData apply_partitioning_nodes(const Partitioning& parts, std::vector<MapNode>&& sorted);
template <typename Query>
RoutedQueries<Query> route_queries(const Partitioning& parts, QueryHandler<Query>& hdr, const Query qrys[], size_t nqrys);
template <typename Query>
Dur rebalancing(Partitioning& parts, DistributedData& data, const Query qrys[], size_t nqrys, const RoutedQueries<Query>& routed, QueryHandler<Query>& hdr);


namespace
{
constexpr Key kKeyMax = std::numeric_limits<Key>::max();
constexpr uint32_t kPointWeightNs = 8000;
constexpr uint32_t kScanWeightNs = 10000;
constexpr uint32_t kModifyWeightNs = 12000;
constexpr uint32_t cBatchNs = 4'000'000;
constexpr uint32_t kFullRebalancePerPairNs = 7;
constexpr uint32_t kIncRebalancePerPairNs = 80;
constexpr uint32_t cIncRebalance = 700'000'000;

inline Key point_key(const operation& op)
{
    switch (op.type) {
    case get_t:
        return key_int64_to_uint64(op.tsk.g.key);
    case update_t:
        return key_int64_to_uint64(op.tsk.u.key);
    case predecessor_t:
        return key_int64_to_uint64(op.tsk.p.key);
    case insert_t:
        return key_int64_to_uint64(op.tsk.i.key);
    case remove_t:
        return key_int64_to_uint64(op.tsk.r.key);
    default:
        return 0;
    }
}
inline KeyRange scan_range(const operation& op)
{
    return {key_int64_to_uint64(op.tsk.s.lkey), key_int64_to_uint64(op.tsk.s.rkey)};
}
inline uint32_t op_weight(const operation_t type)
{
    switch (type) {
    case get_t:
    case update_t:
    case predecessor_t:
        return kPointWeightNs;
    case scan_t:
        return kScanWeightNs;
    case insert_t:
    case remove_t:
        return kModifyWeightNs;
    default:
        return 0;
    }
}
inline bool is_point_like(const operation_t type)
{
    return type == get_t || type == update_t || type == predecessor_t || type == insert_t || type == remove_t;
}
inline Key prev_key(Key key)
{
    return key == std::numeric_limits<Key>::min() ? key : key - 1;
}
inline bool add_one_safe(Key key, Key& out)
{
    if (key == kKeyMax) {
        return false;
    }
    out = key + 1;
    return true;
}
inline bool ranges_overlap(Key l1, Key r1, Key l2, Key r2)
{
    return l1 <= r2 && l2 <= r1;
}
// ── KVPair 連続領域への非所有ビュー群 ──
// host/inc/pairs_range.hpp と同 semantics (docs/pairs-range.md が仕様書)。
// eval_rebalancing は linked_list.hpp を include しないため pairs_range.hpp 本体は
// 取り込まず、ここで同等の型を再定義する。所有する KVPair / chunk load 配列は
// BaseColdState 側に置く。

struct PairsRange {
private:
    const KVPair *pairs_begin{nullptr}, *pairs_end{nullptr};

public:
    size_t npairs() const { return static_cast<size_t>(pairs_end - pairs_begin); }
    const KVPair* begin() const { return pairs_begin; }
    const KVPair* end() const { return pairs_end; }

    PairsRange() = default;
    PairsRange(const KVPair* begin, const KVPair* end) : pairs_begin{begin}, pairs_end{end} {}
};

struct DataChunkIterator {
    inline static constexpr uintptr_t ChunkSizeInBytes = sizeof(KVPair) * KVPairsChunkSize;

    using value_type = DataChunkIterator;
    using reference = value_type&;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using iterator_category = std::random_access_iterator_tag;
    using const_pointer = const value_type*;

private:
    const KVPair *cursor{nullptr}, *part_begin{nullptr}, *part_end{nullptr};
    uint32_t* p_load{nullptr};

public:
    uint32_t& load() const { return *p_load; }
    uint32_t* load_ptr() const { return p_load; }
    const KVPair* begin() const { return cursor; }
    const KVPair* end() const { return (++DataChunkIterator{*this}).begin(); }
    size_t npairs() const { return static_cast<size_t>(end() - begin()); }

    DataChunkIterator() = default;
    DataChunkIterator(const KVPair* cursor, const KVPair* part_begin, const KVPair* part_end, uint32_t* load)
        : cursor{cursor}, part_begin{part_begin}, part_end{part_end}, p_load{load} {}

    reference operator*() & { return *this; }
    value_type operator*() && { return *this; }
    pointer operator->() { return this; }
    const_pointer operator->() const { return this; }

    DataChunkIterator& operator++()
    {
        const uintptr_t addr = std::min<uintptr_t>(reinterpret_cast<uintptr_t>(part_end),
            reinterpret_cast<uintptr_t>(cursor) + ChunkSizeInBytes);
        cursor = reinterpret_cast<const KVPair*>(addr);
        ++p_load;
        return *this;
    }
    DataChunkIterator& operator--()
    {
        const uintptr_t offset_in_part = reinterpret_cast<uintptr_t>(cursor) - reinterpret_cast<uintptr_t>(part_begin),
                        new_offset = offset_in_part - ChunkSizeInBytes,
                        aligned = (new_offset + ChunkSizeInBytes - 1) / ChunkSizeInBytes * ChunkSizeInBytes;
        cursor = reinterpret_cast<const KVPair*>(aligned + reinterpret_cast<uintptr_t>(part_begin));
        --p_load;
        return *this;
    }
    DataChunkIterator& operator+=(difference_type d)
    {
        const uintptr_t offset_in_part = reinterpret_cast<uintptr_t>(cursor) - reinterpret_cast<uintptr_t>(part_begin),
                        shifted = offset_in_part + ChunkSizeInBytes * static_cast<uintptr_t>(d),
                        aligned = (shifted + ChunkSizeInBytes - 1) / ChunkSizeInBytes * ChunkSizeInBytes,
                        addr = reinterpret_cast<uintptr_t>(part_begin) + aligned,
                        clamped = std::min<uintptr_t>(addr, reinterpret_cast<uintptr_t>(part_end));
        cursor = reinterpret_cast<const KVPair*>(clamped);
        p_load += static_cast<uint32_t>(d);
        return *this;
    }
    DataChunkIterator& operator-=(difference_type d) { return (*this) += (-d); }
    DataChunkIterator operator++(int)
    {
        DataChunkIterator tmp{*this};
        ++*this;
        return tmp;
    }
    DataChunkIterator operator--(int)
    {
        DataChunkIterator tmp{*this};
        --*this;
        return tmp;
    }

    [[maybe_unused]] friend bool operator==(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return lhs.cursor == rhs.cursor; }
    [[maybe_unused]] friend bool operator!=(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return !(lhs == rhs); }
    [[maybe_unused]] friend difference_type operator-(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return lhs.p_load - rhs.p_load; }
    [[maybe_unused]] friend DataChunkIterator operator+(const DataChunkIterator& it, difference_type d)
    {
        DataChunkIterator tmp{it};
        tmp += d;
        return tmp;
    }
    [[maybe_unused]] friend DataChunkIterator operator+(difference_type d, const DataChunkIterator& it) { return it + d; }
    [[maybe_unused]] friend DataChunkIterator operator-(const DataChunkIterator& it, difference_type d)
    {
        DataChunkIterator tmp{it};
        tmp -= d;
        return tmp;
    }
    [[maybe_unused]] friend bool operator<(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return (rhs - lhs) > 0; }
    [[maybe_unused]] friend bool operator>(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return rhs < lhs; }
    [[maybe_unused]] friend bool operator<=(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return !(lhs > rhs); }
    [[maybe_unused]] friend bool operator>=(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return !(lhs < rhs); }
};

struct ChunkedPairsRange : PairsRange {
private:
    uint32_t* p_load{nullptr};

public:
    size_t nchunks() const { return (npairs() + KVPairsChunkSize - 1) / KVPairsChunkSize; }
    DataChunkIterator begin() const { return {PairsRange::begin(), PairsRange::begin(), PairsRange::end(), p_load}; }
    DataChunkIterator end() const { return {PairsRange::end(), PairsRange::begin(), PairsRange::end(), p_load + nchunks()}; }
    uint32_t* load_ptr() const { return p_load; }

    ChunkedPairsRange() = default;
    ChunkedPairsRange(const PairsRange& range, uint32_t* load) : PairsRange{range}, p_load{load} {}
    ChunkedPairsRange(const DataChunkIterator& begin, const DataChunkIterator& end)
        : PairsRange{begin->begin(), end->begin()}, p_load{begin.load_ptr()} {}
};

struct NewHotRange {
    PairsRange pairs_range;
    KeyRange key_range;
    uint32_t load;

    size_t npairs() const { return pairs_range.npairs(); }
};

// simulator 固有: どの DPU から hot を切り出したかを追跡するラッパ。
// host の NewHotRange はこの情報を持たないので、ここだけ eval_rebalancing 独自。
struct SrcNewHotRange {
    NewHotRange hot;
    unsigned src_dpu;

    size_t npairs() const { return hot.npairs(); }
};

// hot range 内の "fully covered" fragment が消費する effective pair 数。
// slot 単位 (= hot_npairs) で切り上げる。Algorithm 3 の SizeOf を
// cold range list (不連続ウィンドウ) に拡張するための helper。
inline uint32_t slot_rounded_npairs(const uint32_t npairs, const uint32_t hot_npairs)
{
    return (npairs + hot_npairs - 1) / hot_npairs * hot_npairs;
}

// ── Algorithm 2: Greedy hot range selection (sliding window) ──
// docs/hot-range-finding/find_absolutely_hot_ranges_sliding_window.hpp の直写し。
// 論文の閉区間 {c_i}_{i=l}^{r} を半開区間 [l, r+1) として扱う。
template <typename HotHook>
void find_absolutely_hot_ranges(std::list<ChunkedPairsRange>::iterator begin_part,
    std::list<ChunkedPairsRange>::iterator end_part,
    const uint32_t hot_npairs, const uint32_t hot_nqrys,
    HotHook&& hot_hook)
{
    for (auto part = begin_part; part != end_part; ++part) {
        const DataChunkIterator s = part->begin();
        const DataChunkIterator e = part->end();
        DataChunkIterator l = s;

        uint32_t window_npairs = 0;
        uint32_t window_load = 0;

        for (DataChunkIterator r = s; r != e; ++r) {
            window_npairs += static_cast<uint32_t>(r->npairs());
            window_load += r->load();

            while (window_npairs - static_cast<uint32_t>(l->npairs()) >= hot_npairs) {
                window_npairs -= static_cast<uint32_t>(l->npairs());
                window_load -= l->load();
                ++l;
            }

            if (window_load >= hot_nqrys) {
                if (!hot_hook(part, l, r + 1, window_load)) {
                    return;
                }
                l = r + 1;
                window_npairs = 0;
                window_load = 0;
            }
        }
    }
}

// ── Algorithm 3 第2スキャン: argmax + carve-out (sliding window 版) ──
// docs/hot-range-finding/find_relatively_hot_ranges_sliding_window.hpp の直写し。
//
// 論文の閉区間 {c_i}_{i=l}^{r} を半開区間 [l, r+1) として扱う。ウィンドウは
// cold range list 上の不連続 [l_range, l, r_range, r+1) で表現する。
//
// callback 規約: bool(std::list<ChunkedPairsRange>::iterator, PairsRange, load)
//   - iterator  : hot_range を切り出した cold range
//   - PairsRange: 実際の hot range
//   - load      : その hot range の推定 load
// iterator を受け取るのは eval_rebalancing 側で state.parts.insert(iter, ...) を
// 呼ぶため (host は単一 iter_base を閉じ込むので 3 引数で済む)。
template <typename HotHook>
std::array<std::pair<std::list<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>
find_relatively_hot_ranges(std::list<ChunkedPairsRange>::iterator begin_range,
    std::list<ChunkedPairsRange>::iterator end_range,
    const uint32_t hot_npairs, const unsigned nr_hots,
    HotHook&& hot_hook)
{
    using RangeIter = std::list<ChunkedPairsRange>::iterator;

    ASSERT(begin_range != end_range);
    ASSERT(nr_hots != 0);

    RangeIter argmax_left_range = begin_range, argmax_right_range = begin_range;
    DataChunkIterator argmax_left = begin_range->begin(), argmax_right = argmax_left;

    // ── Phase 2: Carve-out (Alg.3 L1399-L1404) ──
    // argmax 位置の区間 [argmax_left, argmax_right) を右→左に hot_npairs ペアずつ
    // 束ねて hot_hook に渡す。cold range 境界を跨ぐときは 1 range ごとに
    // callback を呼び直す。
    const auto carve_out_hot_ranges = [&] {
        RangeIter range = argmax_right_range;
        DataChunkIterator chunk = argmax_right;

        for (;;) {
            const DataChunkIterator limit = (range == argmax_left_range ? argmax_left : range->begin());
            while (limit < chunk) {
                uint32_t load = 0;
                PairsRange hot_range{chunk->begin(), chunk->begin()};
                while (limit < chunk && hot_range.npairs() < hot_npairs) {
                    --chunk;
                    hot_range = PairsRange{chunk->begin(), hot_range.end()};
                    load += chunk->load();
                }
                if (!hot_hook(range, hot_range, load)) {
                    return std::array<std::pair<RangeIter, DataChunkIterator>, 2>{
                        {{range, chunk}, {argmax_right_range, argmax_right}}};
                }
            }

            if (range == argmax_left_range) {
                break;
            }

            --range;
            chunk = range->end();
        }

        return std::array<std::pair<RangeIter, DataChunkIterator>, 2>{
            {{argmax_left_range, argmax_left}, {argmax_right_range, argmax_right}}};
    };

    // ── Phase 1: Argmax スキャン (Alg.3 L1391-L1397) ──
    //
    // 状態変数と不変条件 (ウィンドウ [l, r_end) に対して):
    //   nqrys_in_window = Σ chunk->load() for chunks in [l, r_end)
    //
    //   単一 range (l_range == r_range) の場合:
    //     left_npairs_offcut   = raw pair count of [l, r_end)
    //     non_left_effective   = 0
    //     window_effective     = left_npairs_offcut
    //
    //   複数 range (l_range != r_range) の場合:
    //     left_npairs_offcut   = raw pair count of [l, l_range->end())
    //     right_npairs_offcut  = raw pair count of [r_range->begin(), r_end)
    //     right_window_offcut  = slot_rounded_npairs(right_npairs_offcut)
    //     non_left_effective   = right_window_offcut
    //                          + Σ_{m = next(l_range)}^{prev(r_range)} slot_rounded(m->npairs())
    //     window_effective     = left_npairs_offcut + non_left_effective
    //
    // 判定:
    //   raw 条件      : window_effective >= required_npairs
    //   boundary 例外 : 新 l が (新) l_range->begin() に位置し、
    //                   slot_rounded_eff(after pop) == required_npairs のとき。
    //                   carve 数を丁度 nr_hots に抑えるための救済
    //                   (find_relatively_hot_ranges_sliding_window.hpp 参照)。

    const uint32_t required_npairs = static_cast<uint32_t>(nr_hots) * hot_npairs;

    RangeIter l_range = begin_range;
    DataChunkIterator l = l_range->begin();

    uint32_t nqrys_in_window = 0;
    uint32_t best_load = 0;

    uint32_t left_npairs_offcut = 0;
    uint32_t right_npairs_offcut = 0;
    uint32_t right_window_offcut = 0;
    uint32_t non_left_effective = 0;

    for (RangeIter r_range = begin_range; r_range != end_range; ++r_range) {
        // 前の r_range の slot_rounded 寄与は non_left_effective に積まれているため、
        // 新 r_range の right_partial はゼロから数え直す。
        right_npairs_offcut = 0;
        right_window_offcut = 0;

        for (DataChunkIterator r = r_range->begin(); r != r_range->end(); ++r) {
            const DataChunkIterator r_end = r + 1;

            // ── Step A: r を 1 chunk 伸ばす (状態の差分更新) ──
            nqrys_in_window += r->load();

            if (l_range == r_range) {
                left_npairs_offcut += static_cast<uint32_t>(r->npairs());
            } else {
                right_npairs_offcut += static_cast<uint32_t>(r->npairs());
                const uint32_t new_right_window_offcut = slot_rounded_npairs(right_npairs_offcut, hot_npairs);
                non_left_effective += new_right_window_offcut - right_window_offcut;
                right_window_offcut = new_right_window_offcut;
            }

            // ── Step B: l を単調前進させる (Alg.3 L1393-L1394 : l ← max{l, l'}) ──
            for (;;) {
                //  ── 単一 range (l_range == r_range) 内での前進 ──
                if (l_range == r_range) {
                    const uint32_t popped = static_cast<uint32_t>(l->npairs());
                    if (left_npairs_offcut - popped < required_npairs) {
                        break;
                    }
                    nqrys_in_window -= l->load();
                    left_npairs_offcut -= popped;
                    ++l;
                    continue;
                }

                //  ── 複数 range, l が l_range の途中にある ──
                if (left_npairs_offcut > static_cast<uint32_t>(l->npairs())) {
                    const uint32_t popped = static_cast<uint32_t>(l->npairs());
                    if (left_npairs_offcut - popped + non_left_effective < required_npairs) {
                        break;
                    }
                    nqrys_in_window -= l->load();
                    left_npairs_offcut -= popped;
                    ++l;
                    continue;
                }

                //  ── 複数 range, l が l_range の最終 chunk にある (前進で境界を跨ぐ) ──
                //  (a) next(l_range) == r_range → 単一 range に合流
                //  (b) next(l_range) != r_range → 依然複数 range
                const RangeIter next_l_range = std::next(l_range);

                if (next_l_range == r_range) {
                    const uint32_t new_window_effective = right_npairs_offcut;
                    bool commit;
                    if (new_window_effective >= required_npairs) {
                        commit = true;
                    } else {
                        // exception: slot_rounded_eff == right_window_offcut.
                        // 丁度 required なら carve 数が nr_hots を超えない。
                        commit = (right_window_offcut == required_npairs);
                    }
                    if (!commit) {
                        break;
                    }
                    for (DataChunkIterator c = l; c != l_range->end(); ++c) {
                        nqrys_in_window -= c->load();
                    }
                    l_range = next_l_range;
                    l = l_range->begin();
                    left_npairs_offcut = right_npairs_offcut;
                    right_npairs_offcut = 0;
                    right_window_offcut = 0;
                    non_left_effective = 0;
                    continue;
                }

                const uint32_t next_raw = static_cast<uint32_t>(next_l_range->npairs());
                const uint32_t next_slot_rounded = slot_rounded_npairs(next_raw, hot_npairs);
                const uint32_t new_non_left_effective = non_left_effective - next_slot_rounded;
                const uint32_t new_window_effective = next_raw + new_non_left_effective;
                bool commit;
                if (new_window_effective >= required_npairs) {
                    commit = true;
                } else {
                    // exception: slot_rounded_eff == non_left_effective。
                    commit = (non_left_effective == required_npairs);
                }
                if (!commit) {
                    break;
                }
                for (DataChunkIterator c = l; c != l_range->end(); ++c) {
                    nqrys_in_window -= c->load();
                }
                l_range = next_l_range;
                l = l_range->begin();
                left_npairs_offcut = next_raw;
                non_left_effective = new_non_left_effective;
                continue;
            }

            // ── Step C: argmax 更新 ──
            if (nqrys_in_window > best_load) {
                best_load = nqrys_in_window;
                argmax_left_range = l_range;
                argmax_left = l;
                argmax_right_range = r_range;
                argmax_right = r_end;
            }
        }
    }

    return carve_out_hot_ranges();
}

struct BaseColdState {
    // 非所有ビュー parts が指す所有ストレージ。std::list<vector> は insert/erase で
    // 既存要素のアドレスが動かないため、storage のポインタを ChunkedPairsRange に
    // 渡したあとも安全。chunk_loads も初回 assign 以降 resize しない前提で使う。
    std::list<std::vector<KVPair>> pair_storages;
    std::vector<uint32_t> chunk_loads;

    std::list<ChunkedPairsRange> parts;
    std::vector<KeyRange> cold_key_ranges;
    std::vector<Delim> cold_start_delims;
    std::map<Delim, size_t> start_to_part_idx;
    // Per-`parts` entry の delim 側右境界キー (closed interval の end)。
    // ChunkedPairsRange 自身は data 側の KVPair ポインタしか持たないため、
    // hot range が cold partition 終端に達した時に使うべき "partition の
    // 実際の右端キー" を別途保持する。std::list のアドレスは stable なので、
    // list 要素の &*it をキーにすればよい。insert/erase で同期させる。
    std::map<const ChunkedPairsRange*, Key> part_end_keys;
    uint32_t cold_load = 0;
    uint32_t cold_npairs = 0;
    uint32_t base_npairs = 0;
    Delims::const_iterator base_begin{};
    Delims::const_iterator base_end{};
};

template <typename Query>
void build_overloaded_base_state(const Partitioning& parts, const DistributedData& data, const Query qrys[], const RoutedQueries<Query>& routed,
    QueryHandler<Query>& hdr, const unsigned idx_dpu, BaseColdState& state)
{
    state.base_begin = parts.delims.end();
    for (auto it = parts.delims.begin(); it != parts.delims.end(); ++it) {
        if (it->first.type == DelimType::Base && it->second.dpu == idx_dpu) {
            state.base_begin = it;
            break;
        }
    }
    ASSERT(state.base_begin != parts.delims.end());
    state.base_end = std::next(state.base_begin);
    while (state.base_end != parts.delims.end() && !(state.base_end->first.type == DelimType::Base && state.base_end->second.dpu != idx_dpu)) {
        ++state.base_end;
    }

    const auto& cold_map = data.cold[idx_dpu];
    const auto count_pairs = [&](Key begin_key, Key end_key) {
        const auto first = cold_map.lower_bound(begin_key);
        const auto last = cold_map.upper_bound(end_key);
        return static_cast<uint32_t>(std::distance(first, last));
    };
    const auto fill_storage = [&](std::vector<KVPair>& dst, Key begin_key, Key end_key) {
        const auto first = cold_map.lower_bound(begin_key);
        const auto last = cold_map.upper_bound(end_key);
        dst.reserve(static_cast<size_t>(std::distance(first, last)));
        for (auto it = first; it != last; ++it) {
            dst.push_back(KVPair{it->first, it->second});
        }
    };

    bool in_cold = false;
    Key cold_begin_key = 0;
    for (auto it = state.base_begin; it != state.base_end; ++it) {
        if (it->first.type == DelimType::Base || it->first.type == DelimType::Cold) {
            if (!in_cold) {
                cold_begin_key = it->first.key;
                in_cold = true;
            }
            continue;
        }
        if (it->first.type == DelimType::Hot && in_cold) {
            const Key cold_end_key = prev_key(it->first.key);
            if (cold_begin_key <= cold_end_key) {
                const uint32_t nr_pairs = count_pairs(cold_begin_key, cold_end_key);
                if (nr_pairs > 0) {
                    state.cold_key_ranges.push_back(KeyRange{cold_begin_key, cold_end_key});
                    state.cold_start_delims.push_back(Delim{cold_begin_key, DelimType::Cold});
                    state.cold_npairs += nr_pairs;
                }
            }
            in_cold = false;
        }
    }
    if (in_cold) {
        const Key cold_end_key = state.base_end == parts.delims.end() ? kKeyMax : prev_key(state.base_end->first.key);
        if (cold_begin_key <= cold_end_key) {
            const uint32_t nr_pairs = count_pairs(cold_begin_key, cold_end_key);
            if (nr_pairs > 0) {
                state.cold_key_ranges.push_back(KeyRange{cold_begin_key, cold_end_key});
                state.cold_start_delims.push_back(Delim{cold_begin_key, DelimType::Cold});
                state.cold_npairs += nr_pairs;
            }
        }
    }

    size_t total_chunks = 0;
    for (const auto& range : state.cold_key_ranges) {
        state.pair_storages.emplace_back();
        auto& storage = state.pair_storages.back();
        fill_storage(storage, range.begin, range.end);
        total_chunks += (storage.size() + KVPairsChunkSize - 1) / KVPairsChunkSize;
    }
    state.chunk_loads.assign(total_chunks, 0);

    size_t load_offset = 0;
    auto storage_it = state.pair_storages.begin();
    for (size_t i = 0; i < state.cold_key_ranges.size(); ++i, ++storage_it) {
        const KVPair* const b = storage_it->data();
        const KVPair* const e = b + storage_it->size();
        uint32_t* const load_ptr = state.chunk_loads.data() + load_offset;
        state.parts.emplace_back(PairsRange{b, e}, load_ptr);
        load_offset += state.parts.back().nchunks();
        state.start_to_part_idx.emplace(state.cold_start_delims[i], i);
        // 初期 parts の delim 側右端キーは cold_key_ranges の end (= partition
        // の元々の右境界) と一致する。以降 insert/erase でこの map を同期する。
        state.part_end_keys.emplace(&state.parts.back(), state.cold_key_ranges[i].end);
    }

    state.base_npairs = state.cold_npairs;
    for (auto it = std::next(state.base_begin); it != state.base_end; ++it) {
        if (it->first.type != DelimType::Hot) {
            continue;
        }
        state.base_npairs += static_cast<uint32_t>(data.hot[it->second.dpu].size());
    }

    // 非可換 scan が同一 base 内の複数 cold piece (= 既存 hot で分割された
    // 断片) を跨ぐと、同じ orig_idx が orig_idxs に連続して現れる。
    // locate_loaded は base 全体に対して endpoint count を加算するため、
    // 重複呼び出しは naive incremental (incremental_repartition_naive.hpp:358)
    // と同様、直前値スキップで除去する。
    size_t prev_orig_idx = std::numeric_limits<size_t>::max();
    for (size_t k = 0; k < routed.cold[idx_dpu].orig_idxs.size(); ++k) {
        const size_t orig_idx = routed.cold[idx_dpu].orig_idxs[k];
        if (orig_idx == prev_orig_idx) {
            continue;
        }
        prev_orig_idx = orig_idx;
        hdr.locate_loaded(qrys[orig_idx], state.base_begin, state.base_end, [&](auto delim_it, const uintmax_t load) {
            if (delim_it->first.type == DelimType::Hot) {
                return;
            }
            const Delim start{delim_it->first.key, DelimType::Cold};
            const auto found = state.start_to_part_idx.find(start);
            if (found == state.start_to_part_idx.end()) {
                return;
            }
            const size_t part_idx = found->second;
            auto part_it = state.parts.begin();
            std::advance(part_it, static_cast<std::ptrdiff_t>(part_idx));
            const auto& range = state.cold_key_ranges[part_idx];
            const auto query_range = scan_range(qrys[orig_idx]);
            const Key key = is_point_like(qrys[orig_idx].type) ? point_key(qrys[orig_idx]) : (query_range.begin == delim_it->first.key ? query_range.begin : query_range.end);
            if (key < range.begin || range.end < key) {
                return;
            }
            auto one_after_target = std::upper_bound(part_it->begin(), part_it->end(), key,
                [](Key target, const DataChunkIterator& chunk) { return target < chunk.begin()->key; });
            ASSERT(one_after_target != part_it->begin());
            --one_after_target;
            one_after_target->load() += static_cast<uint32_t>(load);
            state.cold_load += static_cast<uint32_t>(load);
        });
    }
}

void move_hot_range(DistributedData& data, const unsigned src_dpu, const unsigned dst_dpu, const Key begin_key, const Key end_key)
{
    auto first = data.cold[src_dpu].lower_bound(begin_key);
    const auto last = data.cold[src_dpu].upper_bound(end_key);
    while (first != last) {
        const auto next = std::next(first);
        auto node = data.cold[src_dpu].extract(first);
        data.hot[dst_dpu].insert(data.hot[dst_dpu].end(), std::move(node));
        first = next;
    }
}

template <typename Query>
Dur full_repartition(Partitioning& parts, DistributedData& data, const Query qrys[], const size_t nqrys, QueryHandler<Query>& hdr)
{
    std::vector<MapNode> all = collect_all_data(std::move(data));
    parts = equal_data_partitions(parts.ndpus(), all);
    data = apply_partitioning_nodes(parts, std::move(all));

    QueryHandler<Query> reroute_hdr = hdr;
    reroute_hdr.on_batch_begin();
    RoutedQueries<Query> rerouted = route_queries(parts, reroute_hdr, qrys, nqrys);
    reroute_hdr.on_batch_end();

    std::vector<BaseColdState> states(parts.ndpus());
    std::vector<SrcNewHotRange> new_hots;
    new_hots.reserve(parts.ndpus());
    std::vector<std::pair<unsigned, uint32_t>> cold_loads(parts.ndpus());

    const uint32_t total_load = static_cast<uint32_t>(reroute_hdr.total_load_in_batch());
    const uint32_t hot_load = (total_load + parts.ndpus() - 1) / parts.ndpus();
    const uint32_t cold_load_goal = total_load * std::max(3u, opt.balancing + 1) / 3 / parts.ndpus();

    for (unsigned idx_dpu = 0; idx_dpu < parts.ndpus(); ++idx_dpu) {
        build_overloaded_base_state(parts, data, qrys, rerouted, reroute_hdr, idx_dpu, states[idx_dpu]);
        auto& state = states[idx_dpu];
        uint32_t cold_load = state.cold_load;
        uint32_t cold_npairs = state.cold_npairs;
        const uint32_t hot_npairs = state.base_npairs == 0 ? 0 : (state.base_npairs + opt.balancing - 1) / opt.balancing;

        if (state.parts.empty() || hot_npairs == 0 || cold_load <= cold_load_goal) {
            cold_loads[idx_dpu] = {idx_dpu, cold_load};
            continue;
        }

        find_absolutely_hot_ranges(state.parts.begin(), state.parts.end(), hot_npairs, hot_load,
            [&](auto part_it, DataChunkIterator begin, DataChunkIterator end, uint32_t load) {
                const bool end_at_range_end = (end == part_it->end());
                // part_it (list 要素) の delim 側右境界キー。end_at_range_end の
                // 時はここから hot.key_range.end を拾う — 直前の chunk 末データ
                // キーではなく cold partition 本来の終端を使うことで、データが
                // 存在しない右側キー空間も hot に取り込む。
                const Key current_part_end = state.part_end_keys.at(&*part_it);

                SrcNewHotRange src_hot;
                src_hot.hot.pairs_range = PairsRange{begin->begin(), end->begin()};
                src_hot.hot.key_range = KeyRange{
                    src_hot.hot.pairs_range.begin()->key,
                    end_at_range_end ? current_part_end
                                     : prev_key(src_hot.hot.pairs_range.end()->key)};
                src_hot.hot.load = load;
                src_hot.src_dpu = idx_dpu;
                new_hots.push_back(src_hot);

                cold_npairs -= static_cast<uint32_t>(src_hot.npairs());
                cold_load -= load;

                if (begin != part_it->begin()) {
                    // 左 cold 断片は (part の元々の左境界, hot 開始の直前) を
                    // 担当する。delim 側の右端は hot の begin key の直前。
                    const auto new_it = state.parts.insert(part_it, ChunkedPairsRange{part_it->begin(), begin});
                    state.part_end_keys.emplace(&*new_it, prev_key(begin->begin()->key));
                }
                // 右 cold 残余 (*part_it) は左が詰められるだけなので、その
                // delim 側右端 (= current_part_end) は変わらない。map 上の
                // 既存エントリも &*part_it が stable なのでそのまま有効。
                *part_it = ChunkedPairsRange{end, part_it->end()};
                return cold_load > cold_load_goal;
            });
        for (auto it = state.parts.begin(); it != state.parts.end();) {
            if (it->npairs() == 0) {
                state.part_end_keys.erase(&*it);
                it = state.parts.erase(it);
            } else {
                ++it;
            }
        }

        if (cold_load > cold_load_goal && !state.parts.empty()) {
            const uint32_t nr_relative_hots = hot_load == 0 ? 0 : cold_load / hot_load;
            if (nr_relative_hots > 0) {
                const auto carved = find_relatively_hot_ranges(state.parts.begin(), state.parts.end(), hot_npairs, nr_relative_hots,
                    [&](auto range_it, PairsRange hot_range, uint32_t load) {
                        const bool end_at_range_end = (hot_range.end() == range_it->PairsRange::end());
                        const Key current_part_end = state.part_end_keys.at(&*range_it);

                        SrcNewHotRange src_hot;
                        src_hot.hot.pairs_range = hot_range;
                        src_hot.hot.key_range = KeyRange{
                            hot_range.begin()->key,
                            end_at_range_end ? current_part_end
                                             : prev_key(hot_range.end()->key)};
                        src_hot.hot.load = load;
                        src_hot.src_dpu = idx_dpu;
                        new_hots.push_back(src_hot);
                        cold_npairs -= static_cast<uint32_t>(src_hot.npairs());
                        cold_load -= load;
                        return cold_load > cold_load_goal;
                    });

                auto left_range = carved[0].first;
                auto right_range = carved[1].first;
                const DataChunkIterator left = carved[0].second;
                const DataChunkIterator right = carved[1].second;

                if (left_range == right_range) {
                    if (right != left_range->end()) {
                        if (left != left_range->begin()) {
                            // 新 left cold 断片: 左 = 旧 left_range と同じ、
                            // 右 = hot の開始 (left.begin()->key) の直前。
                            const auto new_it = state.parts.insert(left_range, ChunkedPairsRange{left_range->begin(), left});
                            state.part_end_keys.emplace(&*new_it, prev_key(left.begin()->key));
                        }
                        // *left_range は右 cold 残余になる; delim 側右端は変わらない。
                        *left_range = ChunkedPairsRange{right, left_range->end()};
                    } else {
                        if (left != left_range->begin()) {
                            // 残余は左 [begin, left)。右端は hot の直前 (= left の開始 key の直前)。
                            *left_range = ChunkedPairsRange{left_range->begin(), left};
                            state.part_end_keys.at(&*left_range) = prev_key(left.begin()->key);
                        } else {
                            state.part_end_keys.erase(&*left_range);
                            state.parts.erase(left_range);
                        }
                    }
                } else {
                    // left_range..right_range の間の中間 range は全て carve-out 済。
                    for (auto range = std::next(left_range); range != right_range;) {
                        state.part_end_keys.erase(&*range);
                        range = state.parts.erase(range);
                    }
                    if (right != right_range->end()) {
                        // right_range は右 cold 残余に縮退。delim 側右端は不変。
                        *right_range = ChunkedPairsRange{right, right_range->end()};
                    } else {
                        state.part_end_keys.erase(&*right_range);
                        state.parts.erase(right_range);
                    }
                    if (left != left_range->begin()) {
                        // left_range は左 cold 残余に縮退。delim 側右端は hot 開始の直前へ更新。
                        *left_range = ChunkedPairsRange{left_range->begin(), left};
                        state.part_end_keys.at(&*left_range) = prev_key(left.begin()->key);
                    } else {
                        state.part_end_keys.erase(&*left_range);
                        state.parts.erase(left_range);
                    }
                }
            }
        }

        cold_loads[idx_dpu] = {idx_dpu, cold_load};
    }

    if (new_hots.empty()) {
        return Dur{0};
    }

    std::partial_sort(cold_loads.begin(), cold_loads.begin() + static_cast<std::ptrdiff_t>(new_hots.size()), cold_loads.end(),
        [](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });
    std::sort(new_hots.begin(), new_hots.end(), [](const auto& lhs, const auto& rhs) { return lhs.hot.load > rhs.hot.load; });

    for (size_t idx_hot = 0; idx_hot < new_hots.size(); ++idx_hot) {
        const unsigned dst_dpu = cold_loads[idx_hot].first;
        const auto& src_hot = new_hots[idx_hot];
        const Key begin_key = src_hot.hot.key_range.begin;
        const Key end_key = src_hot.hot.key_range.end;
        parts.delims.emplace(Delim{begin_key, DelimType::Hot}, PartInfo{dst_dpu});
        Key cold_resume_key;
        if (add_one_safe(end_key, cold_resume_key)) {
            parts.delims.emplace(Delim{cold_resume_key, DelimType::Cold}, PartInfo{src_hot.src_dpu});
        }
        move_hot_range(data, src_hot.src_dpu, dst_dpu, begin_key, end_key);
    }

    return std::chrono::duration_cast<Dur>(std::chrono::duration<uint64_t, std::nano>{all.size() * kFullRebalancePerPairNs});
}
}  // namespace


template <>
struct QueryHandler<operation> {
    bool commutative = false;
    std::vector<std::array<uint64_t, remove_t + 1>> counts;
    uint64_t total_load = 0;

    bool is_commutative_range_query(const operation& op) const
    {
        return commutative && op.type == scan_t;
    }

    void on_batch_begin()
    {
        counts.clear();
        total_load = 0;
    }

    template <typename Func>
    void route_one(const operation& op, const Delims& delims, Func&& func)
    {
        const auto ensure_counts = [&](const unsigned ndpus) {
            if (counts.size() < ndpus) {
                counts.resize(ndpus);
            }
        };
        const auto apply_point = [&](const Delims::const_iterator it, const operation& routed_op) {
            if (it == delims.end()) {
                return;
            }
            ensure_counts(it->second.dpu + 1);
            counts[it->second.dpu][routed_op.type]++;
            total_load += 1;
            func(it, routed_op);
        };

        if (op.type == empty_t || remove_t < op.type) {
            return;
        }

        if (is_point_like(op.type)) {
            const Key key = point_key(op);
            Delims::const_iterator it;
            if (op.type != predecessor_t) {
                it = delims.upper_bound(Delim{key, DelimType::Hot});
            } else {
                it = delims.lower_bound(Delim{key, DelimType::Base});
            }
            if (it == delims.begin()) {
                return;
            }
            --it;
            apply_point(it, op);
            return;
        }

        if (op.type != scan_t) {
            return;
        }

        const Key lkey = key_int64_to_uint64(op.tsk.s.lkey);
        const Key rkey = key_int64_to_uint64(op.tsk.s.rkey);
        if (rkey < lkey) {
            return;
        }

        auto it = delims.upper_bound(Delim{lkey, DelimType::Hot});
        if (it == delims.begin()) {
            if (delims.empty() || delims.begin()->first.key > rkey) {
                return;
            }
        } else {
            --it;
        }

        std::unordered_set<unsigned> seen_dpus;
        while (it != delims.end()) {
            const Key part_begin = it->first.key;
            const auto next = std::next(it);
            const Key part_end = next == delims.end() ? kKeyMax : prev_key(next->first.key);
            if (rkey < part_begin) {
                break;
            }
            if (ranges_overlap(lkey, rkey, part_begin, part_end)) {
                operation sub = op;
                sub.tsk.s.lkey = key_uint64_to_int64(std::max(lkey, part_begin));
                sub.tsk.s.rkey = key_uint64_to_int64(std::min(rkey, part_end));
                ensure_counts(it->second.dpu + 1);
                if (!is_commutative_range_query(op) || seen_dpus.insert(it->second.dpu).second) {
                    counts[it->second.dpu][scan_t]++;
                    func(it, sub);
                }
            }
            if (next == delims.end()) {
                break;
            }
            it = next;
        }
        total_load += 2;
    }

    void on_batch_end() {}

    Dur estimate_batch_time(const RoutedQueries<operation>&) const
    {
        uint64_t max_cost = 0;
        for (const auto& per_dpu : counts) {
            uint64_t cost = 0;
            for (int op = 0; op <= remove_t; ++op) {
                cost += per_dpu[static_cast<size_t>(op)] * op_weight(static_cast<operation_t>(op));
            }
            max_cost = std::max(max_cost, cost);
        }
        return std::chrono::duration_cast<Dur>(std::chrono::duration<uint64_t, std::nano>{max_cost + cBatchNs});
    }

    uint64_t total_load_in_batch() const
    {
        return total_load;
    }

    template <typename DelimIter, typename Func>
    void locate_loaded(const operation& op, DelimIter begin, DelimIter end, Func&& func) const
    {
        const auto locate_upper = [&](const Key key) -> DelimIter {
            auto it = std::upper_bound(begin, end, Delim{key, DelimType::Hot},
                [](const Delim& lhs, const auto& rhs) { return lhs < rhs.first; });
            if (it == begin) {
                return end;
            }
            --it;
            return it;
        };
        const auto locate_lower = [&](const Key key) -> DelimIter {
            auto it = std::lower_bound(begin, end, Delim{key, DelimType::Base},
                [](const auto& lhs, const Delim& rhs) { return lhs.first < rhs; });
            if (it == begin) {
                return end;
            }
            --it;
            return it;
        };

        if (is_point_like(op.type)) {
            const Key key = point_key(op);
            const auto it = op.type == predecessor_t ? locate_lower(key) : locate_upper(key);
            if (it != end) {
                func(it, 1);
            }
            return;
        }
        if (op.type != scan_t) {
            return;
        }
        const auto left = locate_upper(key_int64_to_uint64(op.tsk.s.lkey));
        if (left != end) {
            func(left, 1);
        }
        const auto right = locate_upper(key_int64_to_uint64(op.tsk.s.rkey));
        if (right != end) {
            func(right, 1);
        }
    }
};


inline std::vector<KVPair> load_kv_data(const std::string& path)
{
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) {
        throw std::runtime_error("failed to open data file: " + path);
    }

    const uintmax_t size = FileSystem::file_size(path);
    if (size % sizeof(operation) != 0) {
        throw std::runtime_error("invalid operations binary size: " + path);
    }

    std::vector<operation> ops(size / sizeof(operation));
    if (!ops.empty()) {
        ifs.read(reinterpret_cast<char*>(ops.data()), static_cast<std::streamsize>(size));
        if (!ifs) {
            throw std::runtime_error("failed to read data file: " + path);
        }
    }

    std::vector<KVPair> data;
    data.reserve(ops.size());
    for (const auto& op : ops) {
        if (op.type != insert_t) {
            throw std::runtime_error("data file contains non-insert operation: " + path);
        }

        const Key key = key_int64_to_uint64(op.tsk.i.key);
        const Value value = value_int64_to_uint64(op.tsk.i.value);

        data.push_back(KVPair{
            key,
            value,
        });
    }
    return data;
}

inline Partitioning equal_data_partitions(const unsigned ndpus, const KVPair sorted_data[], const size_t ndata)
{
    ASSERT(ndata > 0);
    Partitioning result;
    for (unsigned i_dpu = 0; i_dpu < ndpus; ++i_dpu) {
        const Key key = sorted_data[i_dpu * ndata / ndpus].key;
        result.delims.emplace(Delim{key, DelimType::Base}, PartInfo{i_dpu});
        result.delims.emplace(Delim{key, DelimType::Cold}, PartInfo{i_dpu});
    }
    return result;
}

inline Partitioning equal_data_partitions(const unsigned ndpus, const std::vector<MapNode>& sorted)
{
    const size_t ndata = sorted.size();
    ASSERT(ndata > 0);
    Partitioning result;
    for (unsigned i_dpu = 0; i_dpu < ndpus; ++i_dpu) {
        const Key key = sorted[i_dpu * ndata / ndpus].key();
        result.delims.emplace(Delim{key, DelimType::Base}, PartInfo{i_dpu});
        result.delims.emplace(Delim{key, DelimType::Cold}, PartInfo{i_dpu});
    }
    return result;
}

inline DistributedData apply_partitioning(const Partitioning& parts, const KVPair sorted_data[], const size_t ndata)
{
    const size_t ndpus = parts.ndpus();

    ASSERT(ndata > 0);
    const KVPair* data_cursor = &sorted_data[0];
    const KVPair* const data_end = &sorted_data[ndata];

    DistributedData result;
    result.cold.resize(ndpus);
    result.hot.resize(ndpus);

    for (auto it_delim = parts.delims.begin(); it_delim != parts.delims.end(); ++it_delim) {
        std::map<Key, Value>& target = (it_delim->first.type == DelimType::Hot ? result.hot : result.cold)[it_delim->second.dpu];
        const auto next_delim = std::next(it_delim);
        const KVPair* const part_end = next_delim == parts.delims.end() ? data_end
                                                                        : std::lower_bound(data_cursor, data_end, next_delim->first.key,
                                                                            [](const KVPair& pair, const Key key) { return pair.key < key; });

        for (; data_cursor < part_end; ++data_cursor) {
            target.emplace_hint(target.end(), data_cursor->key, data_cursor->value);
        }
    }

    return result;
}

inline DistributedData apply_partitioning_nodes(const Partitioning& parts, std::vector<MapNode>&& sorted)
{
    const size_t ndpus = parts.ndpus();

    ASSERT(!sorted.empty());
    auto data_cursor = sorted.begin();
    const auto data_end = sorted.end();

    DistributedData result;
    result.cold.resize(ndpus);
    result.hot.resize(ndpus);

    for (auto it_delim = parts.delims.begin(); it_delim != parts.delims.end(); ++it_delim) {
        DataMap& target = (it_delim->first.type == DelimType::Hot ? result.hot : result.cold)[it_delim->second.dpu];
        const auto next_delim = std::next(it_delim);
        const auto part_end = next_delim == parts.delims.end()
                                  ? data_end
                                  : std::lower_bound(data_cursor, data_end, next_delim->first.key,
                                      [](const MapNode& node, const Key key) { return node.key() < key; });

        // 各 partition 内では昇順で流し込むので target.end() は常に最適な hint
        // となり、insert は O(1) amortized。ここで node 再確保を避けるのが本経路
        // の狙い。
        for (; data_cursor < part_end; ++data_cursor) {
            target.insert(target.end(), std::move(*data_cursor));
        }
    }

    return result;
}

template <typename Query>
inline RoutedQueries<Query> route_queries(const Partitioning& parts, QueryHandler<Query>& hdr, const Query qrys[], const size_t nqrys)
{
    const size_t ndpus = parts.ndpus();

    RoutedQueries<Query> result;
    result.cold.resize(ndpus);
    result.hot.resize(ndpus);

    for (size_t i_qry = 0; i_qry < nqrys; ++i_qry) {
        const Query& qry = qrys[i_qry];
        hdr.route_one(qry, parts.delims, [&](const Delims::const_iterator it_delim, const Query& routed_qry) {
            auto& target = (it_delim->first.type == DelimType::Hot ? result.hot : result.cold)[it_delim->second.dpu];
            target.sub_qrys.push_back(routed_qry);
            target.orig_idxs.push_back(i_qry);
        });
    }

    return result;
}

template <typename Query>
Dur rebalancing(Partitioning& parts, DistributedData& data, const Query qrys[], const size_t nqrys, const RoutedQueries<Query>& routed, QueryHandler<Query>& hdr)
{
    if (opt.balancing == 0) {
        return Dur{0};
    }

    const uint32_t cold_nqrys_goal = static_cast<uint32_t>(nqrys * std::max(3u, opt.balancing + 1) / 3 / parts.ndpus());
    const uint32_t cold_nqrys_threshold = std::visit(
        overload(
            [&](const double& hwm) { return static_cast<uint32_t>(cold_nqrys_goal * hwm); },
            [&](const NoiseParams& np) { return np.stored_threshold(nqrys); }),
        opt.threshold_spec);

    uint64_t moved_pairs = 0;
    std::vector<BaseColdState> states(parts.ndpus());
    std::vector<unsigned> overloaded;
    overloaded.reserve(parts.ndpus());
    for (unsigned i_dpu = 0; i_dpu < parts.ndpus(); ++i_dpu) {
        if (routed.cold[i_dpu].sub_qrys.size() > cold_nqrys_threshold) {
            overloaded.push_back(i_dpu);
            moved_pairs += data.cold[i_dpu].size();
        }
    }
    if (overloaded.empty()) {
        return Dur{0};
    }

    if (!opt.enable_incremental) {
        return full_repartition(parts, data, qrys, nqrys, hdr);
    }

    const uint32_t total_load = static_cast<uint32_t>(hdr.total_load_in_batch());
    const uint32_t hot_load = (total_load + parts.ndpus() - 1) / parts.ndpus();
    const uint32_t cold_load_goal = total_load * std::max(3u, opt.balancing + 1) / 3 / parts.ndpus();
    const size_t nr_existing_hots = static_cast<size_t>(std::count_if(parts.delims.begin(), parts.delims.end(),
        [](const auto& delim) { return delim.first.type == DelimType::Hot; }));

    std::vector<SrcNewHotRange> new_hots;
    new_hots.reserve(parts.ndpus());
    std::vector<std::pair<unsigned, uint32_t>> cold_loads(parts.ndpus());
    for (unsigned idx_dpu = 0; idx_dpu < parts.ndpus(); ++idx_dpu) {
        uint32_t load = 0;
        for (const auto& sub_qry : routed.cold[idx_dpu].sub_qrys) {
            load += (sub_qry.type == scan_t) ? 2 : 1;
        }
        cold_loads[idx_dpu] = {idx_dpu, load};
    }

    for (const unsigned idx_dpu : overloaded) {
        build_overloaded_base_state(parts, data, qrys, routed, hdr, idx_dpu, states[idx_dpu]);
        auto& state = states[idx_dpu];
        uint32_t cold_load = state.cold_load;
        uint32_t cold_npairs = state.cold_npairs;
        const uint32_t hot_npairs = state.base_npairs == 0 ? 0 : (state.base_npairs + opt.balancing - 1) / opt.balancing;

        if (state.parts.empty() || hot_npairs == 0 || cold_load <= cold_load_goal) {
            cold_loads[idx_dpu] = {idx_dpu, cold_load};
            continue;
        }

        find_absolutely_hot_ranges(state.parts.begin(), state.parts.end(), hot_npairs, hot_load,
            [&](auto part_it, DataChunkIterator begin, DataChunkIterator end, uint32_t load) {
                const bool end_at_range_end = (end == part_it->end());
                // full_repartition 側と同じ理由で cold partition の delim 側
                // 右境界を参照し、データ末尾と partition 終端の間にあるキー
                // 非存在領域を hot 範囲に取り込む。
                const Key current_part_end = state.part_end_keys.at(&*part_it);

                SrcNewHotRange src_hot;
                src_hot.hot.pairs_range = PairsRange{begin->begin(), end->begin()};
                src_hot.hot.key_range = KeyRange{
                    src_hot.hot.pairs_range.begin()->key,
                    end_at_range_end ? current_part_end
                                     : prev_key(src_hot.hot.pairs_range.end()->key)};
                src_hot.hot.load = load;
                src_hot.src_dpu = idx_dpu;
                new_hots.push_back(src_hot);

                cold_npairs -= static_cast<uint32_t>(src_hot.npairs());
                cold_load -= load;

                if (begin != part_it->begin()) {
                    const auto new_it = state.parts.insert(part_it, ChunkedPairsRange{part_it->begin(), begin});
                    state.part_end_keys.emplace(&*new_it, prev_key(begin->begin()->key));
                }
                *part_it = ChunkedPairsRange{end, part_it->end()};
                return cold_load > cold_load_goal;
            });
        for (auto it = state.parts.begin(); it != state.parts.end();) {
            if (it->npairs() == 0) {
                state.part_end_keys.erase(&*it);
                it = state.parts.erase(it);
            } else {
                ++it;
            }
        }

        if (cold_load > cold_load_goal && !state.parts.empty() && hot_load > 0) {
            const uint32_t nr_relative_hots = cold_load / hot_load;
            if (nr_relative_hots > 0) {
                const auto carved = find_relatively_hot_ranges(state.parts.begin(), state.parts.end(), hot_npairs, nr_relative_hots,
                    [&](auto range_it, PairsRange hot_range, uint32_t load) {
                        const bool end_at_range_end = (hot_range.end() == range_it->PairsRange::end());
                        const Key current_part_end = state.part_end_keys.at(&*range_it);

                        SrcNewHotRange src_hot;
                        src_hot.hot.pairs_range = hot_range;
                        src_hot.hot.key_range = KeyRange{
                            hot_range.begin()->key,
                            end_at_range_end ? current_part_end
                                             : prev_key(hot_range.end()->key)};
                        src_hot.hot.load = load;
                        src_hot.src_dpu = idx_dpu;
                        new_hots.push_back(src_hot);
                        cold_npairs -= static_cast<uint32_t>(src_hot.npairs());
                        cold_load -= src_hot.hot.load;
                        return cold_load > cold_load_goal;
                    });

                auto left_range = carved[0].first;
                auto right_range = carved[1].first;
                const DataChunkIterator left = carved[0].second;
                const DataChunkIterator right = carved[1].second;

                if (left_range == right_range) {
                    if (right != left_range->end()) {
                        if (left != left_range->begin()) {
                            const auto new_it = state.parts.insert(left_range, ChunkedPairsRange{left_range->begin(), left});
                            state.part_end_keys.emplace(&*new_it, prev_key(left.begin()->key));
                        }
                        *left_range = ChunkedPairsRange{right, left_range->end()};
                    } else {
                        if (left != left_range->begin()) {
                            *left_range = ChunkedPairsRange{left_range->begin(), left};
                            state.part_end_keys.at(&*left_range) = prev_key(left.begin()->key);
                        } else {
                            state.part_end_keys.erase(&*left_range);
                            state.parts.erase(left_range);
                        }
                    }
                } else {
                    for (auto range = std::next(left_range); range != right_range;) {
                        state.part_end_keys.erase(&*range);
                        range = state.parts.erase(range);
                    }
                    if (right != right_range->end()) {
                        *right_range = ChunkedPairsRange{right, right_range->end()};
                    } else {
                        state.part_end_keys.erase(&*right_range);
                        state.parts.erase(right_range);
                    }
                    if (left != left_range->begin()) {
                        *left_range = ChunkedPairsRange{left_range->begin(), left};
                        state.part_end_keys.at(&*left_range) = prev_key(left.begin()->key);
                    } else {
                        state.part_end_keys.erase(&*left_range);
                        state.parts.erase(left_range);
                    }
                }
            }
        }

        cold_loads[idx_dpu] = {idx_dpu, cold_load};
        if (nr_existing_hots + new_hots.size() > parts.ndpus()) {
            return full_repartition(parts, data, qrys, nqrys, hdr)
                   + std::chrono::duration_cast<Dur>(std::chrono::duration<uint64_t, std::nano>{moved_pairs * kIncRebalancePerPairNs + cIncRebalance} * 6/10);
        }
    }

    if (new_hots.empty()) {
        return Dur{0};
    }

    for (unsigned idx_dpu = 0; idx_dpu < parts.ndpus(); ++idx_dpu) {
        const bool already_hot = std::any_of(parts.delims.begin(), parts.delims.end(),
            [&](const auto& delim) { return delim.first.type == DelimType::Hot && delim.second.dpu == idx_dpu; });
        if (already_hot) {
            cold_loads[idx_dpu].second = std::numeric_limits<uint32_t>::max();
        }
    }
    std::partial_sort(cold_loads.begin(), cold_loads.begin() + static_cast<std::ptrdiff_t>(new_hots.size()), cold_loads.end(),
        [](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });
    std::sort(new_hots.begin(), new_hots.end(), [](const auto& lhs, const auto& rhs) { return lhs.hot.load > rhs.hot.load; });

    for (size_t idx_hot = 0; idx_hot < new_hots.size(); ++idx_hot) {
        const unsigned idx_dpu = cold_loads[idx_hot].first;
        const auto& src_hot = new_hots[idx_hot];
        const Key begin_key = src_hot.hot.key_range.begin;
        const Key end_key = src_hot.hot.key_range.end;

        parts.delims.emplace(Delim{begin_key, DelimType::Hot}, PartInfo{idx_dpu});
        Key cold_resume_key;
        if (add_one_safe(end_key, cold_resume_key)) {
            parts.delims.emplace(Delim{cold_resume_key, DelimType::Cold}, PartInfo{src_hot.src_dpu});
        }

        move_hot_range(data, src_hot.src_dpu, idx_dpu, begin_key, end_key);
    }

    return std::chrono::duration_cast<Dur>(std::chrono::duration<uint64_t, std::nano>{moved_pairs * kIncRebalancePerPairNs + cIncRebalance});
}

int main(int argc, char* argv[])
{
    const CMDOpt parsed_opt{argc, argv};
    opt = parsed_opt;

    std::visit(
        overload(
            [](const double& hwm) { std::cerr << "legacy threshold: hwm-ratio=" << hwm << std::endl; },
            [](const NoiseParams& np) {
                std::cerr << "noise-aware threshold:"
                          << " p=" << np.p
                          << " L=" << np.L
                          << " K1=" << np.K1
                          << " K2=" << np.K2
                          << std::endl;
            }),
        opt.threshold_spec);

    std::vector<KVPair> data_pairs = load_kv_data(opt.data_file);
    if (data_pairs.empty()) {
        std::cerr << "data file is empty" << std::endl;
        return 1;
    }

    Partitioning parts = equal_data_partitions(opt.ndpus, data_pairs.data(), data_pairs.size());
    DistributedData data = apply_partitioning(parts, data_pairs.data(), data_pairs.size());
    std::vector<KVPair>{}.swap(data_pairs);
    const pimtree_queries queries = make_pimtree_queries(opt.workload_file);
    QueryHandler<operation> hdr{opt.commutative, {}, 0};

    std::mt19937_64 gen;
    Dur virtual_clock{500'000};
    Dur virtual_clock_at_last_sample{0};

    auto next_batch_size = [&](size_t remaining) -> size_t {
        return std::visit(overload(
                              [&](size_t& fixed) -> size_t {
                                  return std::min(fixed, remaining);
                              },
                              [&](double& rate) -> size_t {
                                  const Dur elapsed = virtual_clock - virtual_clock_at_last_sample;
                                  const double elapsed_sec = std::chrono::duration_cast<std::chrono::duration<double>>(elapsed).count();
                                  const double mean = rate * elapsed_sec;
                                  const size_t sampled = std::poisson_distribution<size_t>{mean}(gen);
                                  virtual_clock_at_last_sample = virtual_clock;
                                  return std::min(sampled, remaining);
                              }),
            opt.batch_spec);
    };

    std::cout << "inject_time,batch_idx,batch_size,imbalance,est_batch,est_rebalance,total_load" << std::endl;

    for (size_t batch_idx = 0, offset = 0; offset < queries.length; ++batch_idx) {
        const Dur inject_time = virtual_clock;
        const size_t current_batch_size = next_batch_size(queries.length - offset);
        const operation* batch_queries = &queries.ops[offset];

        hdr.on_batch_begin();
        const RoutedQueries<operation> routed = route_queries(parts, hdr, batch_queries, current_batch_size);
        hdr.on_batch_end();

        const Dur rebalance_time = rebalancing(parts, data, batch_queries, current_batch_size, routed, hdr);

        hdr.on_batch_begin();
        const RoutedQueries<operation> rerouted = route_queries(parts, hdr, batch_queries, current_batch_size);
        hdr.on_batch_end();
        const Dur batch_time = hdr.estimate_batch_time(rerouted);

        virtual_clock += batch_time + rebalance_time;
        const uint64_t total_load = hdr.total_load_in_batch();

        std::vector<size_t> cold_load(parts.ndpus()), hot_load(parts.ndpus()), total(parts.ndpus());
        for (unsigned dpu = 0; dpu < parts.ndpus(); ++dpu) {
            cold_load[dpu] = rerouted.cold[dpu].sub_qrys.size();
            hot_load[dpu] = rerouted.hot[dpu].sub_qrys.size();
            total[dpu] = cold_load[dpu] + hot_load[dpu];
        }

        const size_t max_load = *std::max_element(total.begin(), total.end());
        const size_t sum_load = std::accumulate(total.begin(), total.end(), size_t{0});
        const double avg_load = static_cast<double>(sum_load) / static_cast<double>(parts.ndpus());
        const double imbalance = avg_load == 0.0 ? 0.0 : static_cast<double>(max_load) / avg_load;

        std::cout << inject_time.count() << ","
                  << batch_idx << ","
                  << current_batch_size << ","
                  << imbalance << ","
                  << batch_time.count() << ","
                  << rebalance_time.count() << ","
                  << total_load
                  << std::endl;

        offset += current_batch_size;
    }

    return 0;
}
