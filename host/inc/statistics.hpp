#ifndef __STATISTICS_HPP__
#define __STATISTICS_HPP__

#include <map>
#include <string>
#include <vector>

class XferStatistics {
    struct XferEntry {
        XferEntry() :
            total_bytes(0),
            effective_bytes(0),
            count(0) {}
        uint64_t total_bytes;
        uint64_t effective_bytes;
        uint64_t count;
    };
    std::map<std::string, std::vector<XferEntry> > stat;
    int epoch = 0;

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

    void print(FILE* fp)
    {
        printf("==== XFER STATISTICS (MB) ====\n");
        printf("symbol                    rd count xfer-bytes    average  effective effeciency(%%) \n");
        for (auto x: stat) {
            const char* symbol = x.first.c_str();
            std::vector<XferEntry>& v = x.second;
            uint64_t sum_total_bytes = 0;
            uint64_t sum_effective_bytes = 0;
            uint64_t sum_count = 0;
            for (int i = 0; i < v.size(); i++) {
                XferEntry& e = v[i];
                sum_total_bytes += e.total_bytes;
                sum_effective_bytes += e.effective_bytes;
                sum_count += e.count;
                print_line(symbol, i, e.count, e.total_bytes, e.effective_bytes);
            }
            print_line(symbol, -1, sum_count, sum_total_bytes, sum_effective_bytes);
        }
    }

private:
    void print_line(const char* symbol, int rd,
                    uint64_t count, uint64_t total, uint64_t effective)
    {
#define MB(x) (((float) (x)) / 1000 / 1000)
        printf("%-25s %2d %5lu %10.3f %10.3f %10.3f %5.3f\n",
            symbol, rd, count,
            MB(total),
            count > 0 ? MB(total / count) : 0.0,
            MB(effective),
            total > 0 ? ((float) effective) / total * 100: 0.0
            );
#undef MB
    }
};

#ifdef MEASURE_XFER_BYTES
extern XferStatistics xfer_statistics;
#endif /* MEASURE_XFER_BYTES */   

#endif /* __STATISTICS_HPP__ */