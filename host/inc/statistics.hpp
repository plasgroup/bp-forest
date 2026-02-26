#ifndef __STATISTICS_HPP__
#define __STATISTICS_HPP__

#include <chrono>
#include <map>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

class XferStatistics
{
    struct XferEntry {
        XferEntry() : total_bytes(0),
                      effective_bytes(0),
                      count(0) {}
        uint64_t total_bytes;
        uint64_t effective_bytes;
        uint64_t count;
    };
    std::map<std::string, std::vector<XferEntry>> stat;
    unsigned epoch = 0;

public:
    void new_batch()
    {
        epoch++;
    }

    void add(const char* symbol,
        uint64_t xfer_bytes, uint64_t effective_bytes)
    {
        std::string key = std::string(symbol);
        if (stat.find(key) == stat.end()) {
            std::vector<XferEntry> v;
            stat.insert(std::make_pair(key, std::vector<XferEntry>()));
        }
        std::vector<XferEntry>& v = stat[key];
        while (v.size() <= epoch)
            v.emplace_back();
        XferEntry& e = v[epoch];
        e.total_bytes += xfer_bytes;
        e.effective_bytes += effective_bytes;
        e.count++;
    }

    void print()
    {
        printf("==== XFER STATISTICS (MB) ====\n");
        printf("symbol                    rd count xfer-bytes    average  effective effeciency(%%) \n");
        for (auto x : stat) {
            const char* symbol = x.first.c_str();
            std::vector<XferEntry>& v = x.second;
            uint64_t sum_total_bytes = 0;
            uint64_t sum_effective_bytes = 0;
            uint64_t sum_count = 0;
            for (unsigned i = 0; i < v.size(); i++) {
                XferEntry& e = v[i];
                sum_total_bytes += e.total_bytes;
                sum_effective_bytes += e.effective_bytes;
                sum_count += e.count;
                print_line(symbol, static_cast<int>(i), e.count, e.total_bytes, e.effective_bytes);
            }
            print_line(symbol, -1, sum_count, sum_total_bytes, sum_effective_bytes);
        }
    }

private:
    void print_line(const char* symbol, int rd,
        uint64_t count, uint64_t total, uint64_t effective)
    {
#define MB(x) (((float)(x)) / 1000 / 1000)
        printf("%-25s %2d %5lu %10.3f %10.3f %10.3f %5.3f\n",
            symbol, rd, count,
            MB(total),
            count > 0 ? MB(total / count) : 0.0,
            MB(effective),
            total > 0 ? ((float)effective) / static_cast<float>(total) * 100 : 0.0);
#undef MB
    }
};

#ifdef MEASURE_XFER_BYTES
extern XferStatistics xfer_statistics;
#endif /* MEASURE_XFER_BYTES */


struct ElapsedTime {
    using Duration = std::chrono::nanoseconds;
    using Instances = std::map<int, ElapsedTime*>;

    Duration time;
    const std::string label;
    const Instances::iterator iter;

    static Instances& instances()
    {
        static Instances impl;
        return impl;
    }

    explicit ElapsedTime(std::string_view label, int order = 0)
        : time{}, label{label}, iter{instances().insert({order, this}).first}
    {
    }
    ~ElapsedTime()
    {
        instances().erase(iter);
    }
    auto count() const { return time.count(); }

    static void reset()
    {
        for (auto& pair : instances()) {
            auto& time = pair.second->time;
            time = Duration::zero();
        }
    }
    static std::ostream& print(std::ostream& ostr)
    {
        auto& map = instances();
        auto iter = map.cbegin();
        if (iter != map.cend()) {
            ostr << iter->second->time.count();
            iter++;
        }
        for (; iter != map.cend(); iter++) {
            ostr << ',' << iter->second->time.count();
        }
        return ostr;
    }
    static std::ostream& print_labels(std::ostream& ostr)
    {
        auto& map = instances();
        auto iter = map.cbegin();
        if (iter != map.cend()) {
            ostr << iter->second->label;
            iter++;
        }
        for (; iter != map.cend(); iter++) {
            ostr << ',' << iter->second->label;
        }
        return ostr;
    }
};

struct ScopedTimer final {
    ElapsedTime* const result;
    const ElapsedTime::Duration prev;
    const std::chrono::high_resolution_clock::time_point begin;

    ScopedTimer(ElapsedTime& dur) : result{&dur}, prev{dur.time}, begin{std::chrono::high_resolution_clock::now()} {}
    ~ScopedTimer()
    {
        result->time = prev + std::chrono::duration_cast<ElapsedTime::Duration>(std::chrono::high_resolution_clock::now() - begin);
    }
};


inline ElapsedTime
    DatabaseInitTime{"init_db[ns]", 0},

    RebalancingTime{"rebalance[ns]", 1000},  // 1000--1999

    DataRetrieveTime{"retrieve_data[ns]", 1100},  // 1100--1199
#ifdef SYNCHRONOUS_DPU_EXEC
    CommandingSerializationTime{"cmd_serialization[ns]", 1110},
#endif
    SerializeTime{"serialize[ns]", 1120},
#ifdef SYNCHRONOUS_DPU_EXEC
    NrPairsRecvTime{"recv_nr_pairs[ns]", 1130},
#endif
    PairsBufAllocTime{"alloc_pairs_buf[ns]", 1140},
    PairsRecvTime{"recv_pairs[ns]", 1150},

    RefWorkloadPrepareTime{"prepare_ref_workload[ns]", 1200},
    PrepareForPartitioningTime{"prepare_for_part[ns]", 1300},
    PartitioningTime{"part[ns]", 1400},

    PartitionApplyTime{"apply_part[ns]", 1500},  // 1500--1599
#ifdef SYNCHRONOUS_DPU_EXEC
    ColdPairsSendTime{"send_cold_pairs[ns]", 1510},
    ColdTreesConstructTime{"const_cold_trees[ns]", 1520},
    HotPairsSendTime{"send_hot_pairs[ns]", 1530},
    HotTreesConstructTime{"const_hot_trees[ns]", 1540},
#else
    TreeConstructTime{"const_trees[ns]", 1510},
#endif

    RoutingTableMakeTime{"make_routing_table[ns]", 1600},

    BatchTotalTime{"batch[ns]", 2000},  // 2000--2999
    QueryRoutingTime{"route_qry[ns]", 2100},
#ifdef SYNCHRONOUS_DPU_EXEC
    QuerySendTime{"send_qry[ns]", 2210},
    QueryExecTime{"exec_qry[ns]", 2220},
    QueryRecvTime{"recv_qry[ns]", 2230},
#else /* SYNCHRONOUS_DPU_EXEC */
    QuerySendExecRecvTime{"send_exec_recv_qry[ns]", 2200},
#endif
    PostprocessTime{"postprocess[ns]", 2300};

#endif /* __STATISTICS_HPP__ */
