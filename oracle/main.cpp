#include <iostream>
#include <cstdio>
#include <string>
#include <map>
#include "pimtree_query.hpp"

void process_scan(
    const std::string& init_file,
    const std::string& query_file,
    const int num_of_dpu,
    const int max_items_per_dpu
);
void process_get(
    const std::string& init_file,
    const std::string& query_file,
    const int num_of_dpu,
    const int max_items_per_dpu
);

int main(int argc, char** argv) {
    /*
    argv[1]: init file(must be sorted)
    argv[2]: query file
    argv[3]: mode string("scan" or "get")
    argv[4]: num of dpu
    argv[5]: max item per dpu
    */
    if (argc != 6) {
        std::cerr << "Usage: " << argv[0] << " <init file> <query file> <mode> <num of dpu> <max item per dpu>" << std::endl;
        return 1;
    }
    std::string init_file;
    std::string query_file;
    std::string mode;
    int num_of_dpu;
    int max_items_per_dpu;
    try {
        init_file = argv[1];
        query_file = argv[2];
        mode = argv[3];
        num_of_dpu = std::stoi(argv[4]);
        max_items_per_dpu = std::stoi(argv[5]);

        // modeの妥当性を確認
        if (mode != "scan" && mode != "get") {
            std::cerr << "Error: mode must be \"scan\" or \"get\".\n";
            return 1;
        }

        std::cout << "Init File: " << init_file << "\n";
        std::cout << "Query File: " << query_file << "\n";
        std::cout << "Mode: " << mode << "\n";
        std::cout << "Number of DPUs: " << num_of_dpu << "\n";
        std::cout << "Max Items per DPU: " << max_items_per_dpu << "\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: Invalid input. " << e.what() << "\n";
        return 1;
    }

    if (mode == "scan") {
        process_scan(init_file, query_file, num_of_dpu, max_items_per_dpu);
    } else if (mode == "get") {
        process_get(init_file, query_file, num_of_dpu, max_items_per_dpu);
    }

    return 0;
}

void process_scan(
    const std::string& init_file,
    const std::string& query_file,
    const int num_of_dpu,
    const int max_items_per_dpu
) {
    pimtree_queries queries = make_pimtree_queries(query_file);
}

void process_get(
    const std::string& init_file,
    const std::string& query_file,
    const int num_of_dpu,
    const int max_items_per_dpu
) {
    int64_t* each_dpu_elements = new int64_t[num_of_dpu];
    int64_t* each_dpu_queries = new int64_t[num_of_dpu];
    pimtree_queries test_queries = make_pimtree_queries(query_file);
    std::map<int64_t, int64_t> query_map;
    int search_max = test_queries.length;
    int search_min = (test_queries.length - 1) / num_of_dpu + 1;
    printf("search_min: %d, search_max: %d\n", search_min, search_max);

    for (size_t i = 0; i < test_queries.length; i++) {
        query_map[test_queries.ops[i].tsk.g.key] += 1;
    }
    free_pimtree_queries(test_queries);

    pimtree_queries init_queries = make_pimtree_queries(init_file);
    if (max_items_per_dpu * num_of_dpu < init_queries.length) {
        std::cerr << "Error: Too many items for the given number of DPUs.\n";
        free_pimtree_queries(init_queries);
        delete[] each_dpu_elements;
        delete[] each_dpu_queries;
        return;
    }
    while (search_min < search_max) {
        int current_search = search_min + (search_max - search_min) / 2;
        printf("current_search: %d\n", current_search);
        bool can_divide = true;

        int64_t current_dpu_element = 0;
        int64_t current_dpu_query = 0;
        int current_dpu_index = 0;
        for (size_t i = 0;i < init_queries.length;i++) {
            int64_t key = init_queries.ops[i].tsk.i.key;
            std::map<int64_t, int64_t>::iterator it = query_map.find(key);
            bool has_query = it != query_map.end();
            if(
                max_items_per_dpu < (current_dpu_element + 1) ||
                (has_query && current_search < (current_dpu_query + it->second))
            ) {
                each_dpu_elements[current_dpu_index] = current_dpu_element;
                each_dpu_queries[current_dpu_index] = current_dpu_query;
                current_dpu_element = 1;
                current_dpu_query = has_query ? it->second : 0;
                if (current_search < current_dpu_query) {
                    can_divide = false;
                    break;
                }
                current_dpu_index += 1;
            } else {
                current_dpu_element += 1;
                current_dpu_query += has_query ? it->second : 0;
            }

            if (num_of_dpu == current_dpu_index) {
                can_divide = false;
                break;
            }
        }

        if (can_divide) {
            search_max = current_search;
        } else {
            search_min = current_search + 1;
        }
    }

    printf("alpha is %d\n", search_min);
    for (int i = 0; i < num_of_dpu; i++) {
        printf("DPU %d: element is %ld, query is %ld\n", i, each_dpu_elements[i], each_dpu_queries[i]);
    }

    free_pimtree_queries(init_queries);
    delete[] each_dpu_elements;
    delete[] each_dpu_queries;
}

