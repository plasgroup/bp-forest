#include <vector>
#include "common.h"
#include "pimtree_query.hpp"

template <typename K>
class PointGenerator {
public:
    PointGenerator() {}
    virtual ~PointGenerator() {}
    virtual std::vector<K> generate(size_t n) = 0;
    virtual std::vector<K> generate_sorted(size_t n)
    {
        std::vector<K> keys = generate(n);
        std::sort(keys.begin(), keys.end());
        return keys;
    };
};

template <typename K>
class EvenGenerator : public PointGenerator<K> {
    K min;
    K max;
public:
    EvenGenerator(K min, K max)
        : min(min), max(max)
    {}

    std::vector<K> generate(size_t n)
    {
        K key_interval = (max - min) / (n - 1);
        std::vector<K> keys;
        keys.reserve(n);
        for (size_t i = 0; i < n; i++) {
            keys.push_back(min + i * key_interval);
        }
        return keys;
    }

    std::vector<K> generate_sorted(size_t n)
    {
        return generate(n);
    }
};

template <typename K>
class OverKeyGenerator : public PointGenerator<K> {
protected:
    std::vector<K>& keys;

public:
    OverKeyGenerator(std::vector<K>& keys)
        : keys(keys)
    {}

    virtual ~OverKeyGenerator() {}

    virtual std::vector<size_t> generate_indeces(size_t n) = 0;
    
    virtual std::vector<K> generate(size_t n)
    {
        std::vector<size_t> ids = generate_indeces(n);
        std::vector<K> workload(n);
        for (size_t i = 0; i < n; i++)
            workload[i] = keys[ids[i]];
        return workload;
    };
};

template <typename K>
class SlicedZipfOverKeyGenerator : public OverKeyGenerator<K> {
    double alpha;
    size_t nslices;
    bool scramble;
    int seed;

    // from PIM-Tree source code
    std::vector<double> init_zipf_pos(size_t P, double alpha) {
        double sum = 0;
        std::vector<double> zipf_pos(P, 1.0);
        for (size_t i = 0; i < P; i++) {
            zipf_pos[i] /= pow((double) (i + 1), alpha);
            sum += zipf_pos[i];
        }
        for (size_t i = 0; i < P; i++) {
            zipf_pos[i] /= sum;
            if (i > 0) {
                zipf_pos[i] += zipf_pos[i - 1];
            }
        }
        return zipf_pos;
    }

    // from PIM-Tree source code
    //   P: number of slices
    //   ds_size: number of items
    //   n: number of queries
    //   return: indeces of items
    std::vector<size_t> zipf_over_items(size_t P, size_t ds_size, size_t n) {
        std::mt19937_64 mt(seed);

        std::vector<double> zipf_pos = init_zipf_pos(P, alpha);

        std::vector<size_t> order(P); // rank -> slice-id
        for (size_t i = 0; i < P; i++)
            order[i] = i;
        if (scramble) {
            for (size_t i = 0; i < P; i++) {
                size_t r = std::uniform_int_distribution<uint64_t>(0, P)(mt) % (P - i);
                std::swap(order[i], order[i + r]);
            }
        }

        std::vector<size_t> slice_id(n);  // query-index -> slice-id
        for (size_t i = 0; i < n; i++) {
            double rd = std::uniform_real_distribution<double>(0.0, 1.0)(mt);
            int l = -1, r = ((int) P) - 1;  // (]
            while (r - l > 1) {
                int mid = (l + r) >> 1;
                if (zipf_pos[mid] < rd) {
                    l = mid;
                } else {
                    r = mid;
                }
            }
            slice_id[i] = order[r];        
        }

        std::vector<size_t> ids(n);  // query-index -> item-id
        for (size_t i = 0; i < n; i++) {
            uint64_t rd = std::uniform_int_distribution<uint64_t>(0, ds_size)(mt);
            size_t left = (ds_size / P) * slice_id[i];
            size_t item_id = left + rd / P + 1;  // why +1?
            if (item_id >= ds_size)
                item_id = ds_size - 1;
            ids[i] = item_id;
        }

        return ids;
    }

public:
    SlicedZipfOverKeyGenerator(std::vector<K>& keys, double alpha, size_t nslices, bool scramble, unsigned int seed)
        : OverKeyGenerator<K>(keys), alpha(alpha), nslices(nslices), scramble(scramble), seed(seed)
    {}
    
    std::vector<size_t> generate_indeces(size_t n)
    {
        return zipf_over_items(nslices, this->keys.size(), n);
    }
};

template <typename K>
class RangeGenerator {
protected:
    OverKeyGenerator<K>* point_generator;
public:
    RangeGenerator(OverKeyGenerator<K>* pgen)
        : point_generator(pgen)
    {}
    virtual ~RangeGenerator() {}
    virtual std::vector<std::pair<K, K>> generate(size_t n) = 0;
};

template <typename K>
class ConstLengthRangeGenerator : public RangeGenerator<K> {
    const std::vector<K>& keys;
    size_t items_in_range;
public:
    ConstLengthRangeGenerator(OverKeyGenerator<K>* pgen, const std::vector<K>& keys, size_t items_in_range)
        : RangeGenerator<K>(pgen), keys(keys), items_in_range(items_in_range)
    {}

    std::vector<std::pair<K, K>> generate(size_t n)
    {
        std::vector<size_t> points = this->point_generator->generate_indeces(n);
        std::vector<std::pair<K, K>> ranges(n);
        for (size_t i = 0; i < n; i++) {
            size_t left = points[i];
            size_t right = std::min(left + items_in_range, keys.size() - 1);
            ranges[i] = {keys[left], keys[right]};
        }
        return ranges;
    }
};

template <typename K, typename V>
std::vector<std::pair<K, V>> load_init_data(const std::string &file_name)
{
    std::vector<std::pair<K, V>> kvs;
    pimtree_queries init_data = make_pimtree_queries(file_name);
    for (size_t i = 0; i < init_data.length; i++) {
        if (init_data.ops[i].type != insert_t) {
            fprintf(stderr, "invalid operation type in init file\n");
            exit(1);
        }
        auto& item = init_data.ops[i].tsk.i;
        kvs.push_back({(K) item.key, (V) item.value});
    }
    return kvs;
}

template <typename K>
std::vector<K> load_point_workload(const std::string &file_name)
{
    std::vector<K> workload;
    pimtree_queries queries = make_pimtree_queries(file_name);
    for (size_t i = 0; i < queries.length; i++) {
        if (queries.ops[i].type != get_t) {
            fprintf(stderr, "invalid operation type in workload file\n");
            exit(1);
        }
        workload.push_back(queries.ops[i].tsk.g.key);
    }
    return workload;
}

template <typename K>
std::vector<std::pair<K, K>> load_range_workload(const std::string &file_name)
{
    std::vector<std::pair<K, K>> workload;
    pimtree_queries queries = make_pimtree_queries(file_name);
    for (size_t i = 0; i < queries.length; i++) {
        if (queries.ops[i].type != scan_t) {
            fprintf(stderr, "invalid operation type in workload file\n");
            exit(1);
        }
        workload.push_back({queries.ops[i].tsk.s.lkey, queries.ops[i].tsk.s.rkey});
    }
    return workload;
}

template <typename K, typename V>
void save_init_data(const std::string &file_name, const std::vector<std::pair<K, V>> &kvs)
{
    struct operation op;
    memset(&op, 0, sizeof(op));
    FILE* fp = fopen(file_name.c_str(), "wb");
    if (!fp) {
        perror("fopen");
        exit(1);
    }
    for (auto [key, value]: kvs) {
        op.type = insert_t;
        op.tsk.i.key = (int64_t) key;
        op.tsk.i.value = (int64_t) value;
        fwrite(&op, sizeof(op), 1, fp);
    }
    fclose(fp);
}

template <typename K>
void save_point_workload(const std::string &file_name, const std::vector<K> &keys)
{
    struct operation op;
    memset(&op, 0, sizeof(op));
    FILE* fp = fopen(file_name.c_str(), "wb");
    if (!fp) {
        perror("fopen");
        exit(1);
    }
    for (auto key: keys) {
        op.type = get_t;
        op.tsk.g.key = (int64_t) key;
        fwrite(&op, sizeof(op), 1, fp);
    }
    fclose(fp);
}

// both inclusive
template <typename K>
void save_range_workload(const std::string & file_name, const std::vector<std::pair<K, K>> &ranges)
{
    struct operation op;
    memset(&op, 0, sizeof(op));
    FILE* fp = fopen(file_name.c_str(), "wb");
    if (!fp) {
        perror("fopen");
        exit(1);
    }
    for (auto [lkey, rkey]: ranges) {
        op.type = scan_t;
        op.tsk.s.lkey = (int64_t) lkey;
        op.tsk.s.rkey = (int64_t) rkey;
        fwrite(&op, sizeof(op), 1, fp);
    }
    fclose(fp);
}