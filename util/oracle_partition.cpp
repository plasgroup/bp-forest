#include <cmdline.h>
#include <random>
#include "host/inc/pimtree_query.hpp"
#include "host/inc/pimtree_query.ipp"
#include "host/inc/statistics.hpp"
#include "partitioner.hpp"
#include <queue>

struct Option {
    cmdline::parser a;
    void parse(int argc, char* argv[])
    {
        a.add<std::string>("pimtree-workload-file", 'w', "file path to PIM-Tree workload file", false);
        a.add<std::string>("pimtree-init-file", 'i', "file path to PIM-Tree init file", false);
       
        a.add<double>("alpha", 'a', "zipf alpha", false, 0.99);
        a.add<double>("items", 'n', "number of items in millions", false, 500.0);
        a.add<int>("queries", 'q', "number of queries", false, 1024 * 1024);
        a.add<int>("slices", 's', "number of slices", false, 1024 * 10);

        a.add<int>("max-items-per-dpu", 'm', "maximum number of items per DPU", false, 40 * 1000);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, pred, rmq", false, "get");
        a.add<int>("nr-dpus", 'p', "number of DPUs", false, 2500);
        a.parse_check(argc, argv);
    }

    const std::string& workload_file() {
        return a.get<std::string>("pimtree-workload-file");
    }

    const std::string& init_file() {
        return a.get<std::string>("pimtree-init-file");
    }

    int max_items_per_dpu() {
        return a.get<int>("max-items-per-dpu");
    }
    
    int nr_dpus() {
        return a.get<int>("nr-dpus");
    }

    double alpha() {
        return a.get<double>("alpha");
    }

    int num_slices() {
        return a.get<int>("slices");
    }

    size_t items() {
        return (size_t)(a.get<double>("items") * 1000 * 1000);
    }

    int queries() {
        return a.get<int>("queries");
    }

    operation_t op_type() {
        if (a.get<std::string>("ops") == "get")
            return get_t;
        else if (a.get<std::string>("ops") == "rmq")
            return scan_t;
        else {
            fprintf(stderr, "invalid operation type: %s\n", a.get<std::string>("ops").c_str());
            exit(1);
            return empty_t;
        }
    }
} opt;

#ifdef PIM_TREE
std::vector<int64_t> load_keys(const std::string &init_file)
{
    std::vector<int64_t> keys;
    pimtree_queries init_data = make_pimtree_queries(opt.init_file());
    for (size_t i = 0; i < init_data.length; i++) {
        if (init_data.ops[i].type != insert_t) {
            fprintf(stderr, "invalid operation type in init file\n");
            exit(1);
        }
        keys.push_back(init_data.ops[i].tsk.i.key);
    }
    return keys;
}

std::vector<int64_t> load_queries(const std::string &workload_file)
{
    std::vector<int64_t> workload;
    pimtree_queries queries = make_pimtree_queries(opt.workload_file());
    for (size_t i = 0; i < queries.length; i++) {
        if (queries.ops[i].type != get_t) {
            fprintf(stderr, "invalid operation type in workload file\n");
            exit(1);
        }
        workload.push_back(queries.ops[i].tsk.g.key);
    }
    std::sort(workload.begin(), workload.end());
    return workload;
}
#else // PIM_TREE

#define KEY_MIN INT64_MIN
#define KEY_MAX INT64_MAX

std::vector<int64_t> keys_to_generate_queries;
std::vector<int64_t> load_keys(const std::string &init_file)
{
    size_t key_interval = KEY_MAX / (opt.items() - 1);
    std::vector<int64_t> keys;
    keys.reserve(opt.items());
    for (size_t i = 0; i < opt.items(); i++) {
        keys.push_back(KEY_MIN + i * key_interval);
    }
    keys_to_generate_queries = keys;
    return keys;
}

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

std::vector<size_t> zipf_over_items(size_t P, size_t ds_size, double alpha, size_t n) {
    std::random_device rnd;
    std::mt19937_64 mt(rnd());
    std::uniform_real_distribution<double> dist(0.0, 1.0);

    std::vector<double> zipf_pos = init_zipf_pos(P, alpha);

    std::vector<size_t> order(P); // rank -> slice-id
    for (size_t i = 0; i < P; i++)
        order[i] = i;
#if 0
    for (size_t i = 0; i < P; i++) {
        size_t r = std::uniform_int_distribution<uint64_t>(0, P)(mt) % (P - i);
        std::swap(order[i], order[i + r]);
    }
#endif // 0
//    auto order = tabulate(P, [&](size_t i) { return i; });
//    for (int i = 0; i < P; i++) {
//        int r = abs(rn_gen::parallel_rand()) % (P - i);
//        swap(order[i], order[i + r]);
//    }

    std::vector<size_t> slice_id(n);  // query-index -> slice-id
    for (size_t i = 0; i < n; i++) {
        double rd = dist(mt);
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

std::vector<int64_t> load_point_queries(const std::string &workload_file)
{
    std::vector<size_t> ids = zipf_over_items(opt.num_slices(), opt.items(), opt.alpha(), opt.queries());
    std::vector<int64_t> queries;
    for (size_t i: ids)
        queries.push_back(keys_to_generate_queries[i]);
    std::sort(queries.begin(), queries.end());
    return queries;
}

std::vector<std::pair<int64_t, int64_t>> load_range_queries(const std::string &workload_file)
{
    std::vector<size_t> ids = zipf_over_items(opt.num_slices(), opt.items(), opt.alpha(), opt.queries());
    std::vector<std::pair<int64_t, int64_t>> queries;
    for (size_t i: ids) {
        size_t left = keys_to_generate_queries[i];
        size_t right = left + (KEY_MAX / (opt.items() - 1) * 100);
        queries.push_back(std::make_pair(left, right));
    }
    return queries;
}
#endif // PIM_TRE

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    std::vector<int64_t> keys = load_keys(opt.init_file());
    std::vector<int64_t> workload = load_point_queries(opt.workload_file());
/*
    std::vector<ChunkedPartitioner::chunk> chunks;
    for (int64_t key: keys) {
        ChunkedPartitioner::chunk c;
        c.left_key = key;
        c.count = 1;
        chunks.push_back(c);
    }
*/

    ChunkedBPForestPartitioner partitioner(new BPForestChunkBuilder(15, 20), 4);
    auto part = partitioner.partition_point(keys, workload, opt.nr_dpus());
    for (size_t i = 0; i < part.size(); i++) {
        if (part[i] != Partitioner::INVALID_PARTITION)
            printf("part[%ld] = (%ld, %ld) %ld\n", i, part[i].first, part[i].second, part[i].second - part[i].first);
    }
/*
    //OraclePartitioner partitioner(opt.max_items_per_dpu());
    OraclePartitioner op(opt.max_items_per_dpu());
    auto par1 = op.partition_point(keys, workload, opt.nr_dpus());

//    SingletonChunkBuilder chunk_builder; //
    BPForestChunkBuilder chunk_builder(15, 20);
    ChunkedOraclePartitioner cop(&chunk_builder, opt.max_items_per_dpu());
    auto par2 = cop.partition_point(keys, workload, opt.nr_dpus());
    std::vector<ChunkBuilder::chunk> chunks;
    for_each_bpforest_baserange(keys, opt.nr_dpus(), [&](size_t begin_idx, size_t end_idx) {
        chunk_builder.build_chunks(chunks, keys, {begin_idx, end_idx});
    });

    auto chunks_it = chunks.begin();
    auto prev = chunks.begin();
    size_t nkeys = 0;
    for (size_t i = 0; i < par2.size(); i++) {
        auto& p = par2[i];
        while (chunks_it->left_key < keys[p.first]) {
            chunks_it++;
            nkeys += chunks_it->count;
        }
        if (chunks_it->left_key != keys[p.first]) {
            printf("chunk_it->left_key = %ld, keys[%d] = %ld chunk_ID = %ld\n", chunks_it->left_key, p.first, keys[p.first], chunks_it - chunks.begin());
            exit(1);
        } else {
            printf("par2[%ld] = (%ld, %ld) %ld [%ld, delta=%ld %ld]\n", i, p.first, p.second, p.second - p.first, chunks_it - chunks.begin(), chunks_it - prev, nkeys);
        }
        prev = chunks_it;
        nkeys = 0;
    }
*/
/*
    if (par1.size() != par2.size()) {
        printf("par1.size() = %ld, par2.size() = %ld\n", par1.size(), par2.size());
        exit(1);
    }
    printf("par1.size() = %ld, par2.size() = %ld\n", par1.size(), par2.size());
    for (size_t i = 0; i < par1.size(); i++) {
        if (par1[i] != par2[i]) {
            printf("par1[%ld] = (%ld, %ld) %ld, par2[%ld] = (%ld, %ld) %ld\n", i, par1[i].first, par1[i].second, par1[i].second - par1[i].first, i, par2[i].first, par2[i].second, par2[i].second - par2[i].first);
        }
    }
*/
    printf("OK\n");
#if 0
    BPForestPartitioner partitioner(1);
    std::vector<Partitioner::partition_t> hot = partitioner.partition_point(keys, workload, opt.nr_dpus());

    for (size_t i = 0; i < hot.size(); i++) {
        if (hot[i] != Partitioner::INVALID_PARTITION)
            printf("hot[%ld] = (%d, %d) %d\n", i, hot[i].first, hot[i].second, hot[i].second - hot[i].first);
    }
#endif // 0

    return 0;
}


#if 0
int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    std::vector<int64_t> keys = load_keys(opt.init_file());

    if (opt.op_type() == get_t) {
        std::vector<int64_t> workload = load_point_queries(opt.workload_file());

//        printf("workload: ");
//        for (int i = 0; i < workload.size(); i++)
//            printf("%ld ", workload[i]);
//        printf("\n");

        std::cout << "computing frequency" << std::endl;
        std::vector<size_t> freq(keys.size(), 0);
        for (int i = 0; i < workload.size(); i++) {
            auto it = std::lower_bound(keys.begin(), keys.end(), workload[i]);
//            auto it = std::find(keys.begin(), keys.end(), workload[i]);
            if (it != keys.end())
                freq[it - keys.begin()]++;
        }
        std::cout << "sorting frequency" << std::endl;
        std::sort(freq.begin(), freq.end(), std::greater<size_t>());
        printf("freq: %d %d...  2500th:%d \n", freq[0], freq[1], freq[2499]);

        int l = 1, r = workload.size();
        while (l < r) {
            int m = (l + r) / 2;
            printf("start_computation with m = %d\n", m);
            int p = compute_need_dpus_point(keys, workload, opt.max_items_per_dpu(), m);
            printf("need %d dpus\n", p);
            if (p > opt.nr_dpus())
                l = m + 1;
            else
                r = m;
        }
        printf("query limit = %d\n", l);
    } else if (opt.op_type() == scan_t) {
        std::vector<std::pair<int64_t, int64_t>> workload = load_range_queries(opt.workload_file());

        std::vector<int64_t> ls;
        std::vector<int64_t> rs;
        for (int i = 0; i < workload.size(); i++) {
            ls.push_back(workload[i].first);
            rs.push_back(workload[i].second);
        }
        std::sort(ls.begin(), ls.end());
        std::sort(rs.begin(), rs.end());

/*
        printf("ls: ");
        for (int i = 0; i < ls.size(); i++)
            printf(" %d", ls[i]);
        printf("\n");
        printf("rs: ");
        for (int i = 0; i < rs.size(); i++)
            printf(" %d", rs[i]);
        printf("\n");
*/
        int l = 1, r = workload.size();
        while (l < r) {
            int m = (l + r) / 2;
            printf("start_computation with m = %d\n", m);
            int p = compute_need_dpus_range(keys, ls, rs, opt.max_items_per_dpu(), m);
            printf("need %d dpus\n", p);
            if (p > opt.nr_dpus())
                l = m + 1;
            else
                r = m;
        }
        printf("query limit = %d\n", l);
    }
}
#endif // 0