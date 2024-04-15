#include "piecewise_constant_workload.hpp"
#include "workload_types.h"

#include <cereal/archives/binary.hpp>

#include <cmdline.h>

#include <generator.h>
#include <scrambled_zipfian_generator.h>
#include <utils.h>
#include <zipfian_generator.h>

#include <cmath>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <vector>

int main(int argc, char* argv[])
{
    cmdline::parser a;
    a.add<std::string>("filename", 'f', "workload file name", true);
    a.add<size_t>("keynum", 'n', "num of generated_keys to generate", false, 100000000);
    a.add<double>("zipfianconst", 'a', "zipfianconst", false, 0.99);
    a.add<uint64_t>("elementnum", 'e', "num of elements of zipfian dist.", false, 2500);
    a.add("scramble", 's', "whether scramble or not");
    a.add("nowrite", 'q', "don't generate file if true");
    a.add("showinfo", 'v', "show debug info if true");
    a.parse_check(argc, argv);
    // a.add<int>("elementnum", 'e', "num of elements of zipfian dist.", true, (1ULL << 31) - 1;); // 32bit zipfian dist.

    const double zipfian_const = a.get<double>("zipfianconst");
    const size_t key_num = a.get<size_t>("keynum");
    const uint64_t min = KEY_MIN;
    const uint64_t num_partitions = a.get<uint64_t>("elementnum");
    const uint64_t range = KEY_MAX / num_partitions;  // key range per 1 division

    PiecewiseConstantWorkload generated;
    generated.metadata.intervals.reserve(num_partitions + 1u);
    for (uint64_t idx_part = 0; idx_part < num_partitions + 1u; idx_part++) {
        generated.metadata.intervals.push_back(idx_part * range);
    }
    generated.metadata.densities.reserve(num_partitions);

    std::unique_ptr<ycsbc::Generator<uint64_t>> generator;
    if (a.exist("scramble")) {
        generator.reset(new ycsbc::ScrambledZipfianGenerator(min, num_partitions, zipfian_const));
        generated.metadata.densities.resize(num_partitions);
        for (uint64_t idx_part = 0; idx_part < num_partitions; idx_part++) {
            generated.metadata.densities.at(utils::FNVHash64(idx_part) % num_partitions) = std::pow(idx_part + 1, -zipfian_const);
        }
    } else {
        generator.reset(new ycsbc::ZipfianGenerator(min, num_partitions, zipfian_const));
        for (uint64_t idx_part = 0; idx_part < num_partitions; idx_part++) {
            generated.metadata.densities.push_back(std::pow(idx_part + 1, -zipfian_const));
        }
    }

    generated.data.reserve(key_num);
    std::random_device rnd;
    std::mt19937_64 mt(rnd());
    if (a.exist("showinfo")) {
        std::cout << "range: " << range << std::endl;
    }
    std::uniform_int_distribution<uint64_t> rand_in_range(0, range - 1u);
    std::vector<uint64_t> distribution(num_partitions);
    clock_t start_time;
    double time_elapsed;

    start_time = clock();

    for (size_t i = 0; i < key_num; i++) {
        time_elapsed = (double)(clock() - start_time) / CLOCKS_PER_SEC;
        if (time_elapsed >= 3.0) {
            printf("[zipfianconst = %.2f] %.2f%% completed\n", zipfian_const, ((double)i / (double)key_num) * 100);
            start_time = clock();
        }
        uint64_t which_range = generator->Next();
        uint64_t in_range = rand_in_range(mt);
        generated.data.push_back(which_range * range + in_range);
        distribution[which_range]++;
    }
    for (unsigned i = 0; i < 10; i++) {
        if (a.exist("showinfo")) {
            std::cout << "num keys in " << i << "th range: " << distribution[i] << ", " << 100 * (double)distribution[i] / (double)key_num << "%" << std::endl;
        }
    }

    if (!a.exist("nowrite")) {
        const std::string& filename = a.get<std::string>("filename");
        std::ofstream writing_file{filename, std::ios::binary};
        if (!writing_file) {
            std::cerr << "cannot open file " << filename << std::endl;
            return 1;
        }
        {
            cereal::BinaryOutputArchive oarchive(writing_file);
            oarchive(generated);
        }

        std::cout << filename << " has been written, num of reqs = " << key_num << ", size = " << writing_file.tellp() << " B" << std::endl;
    }
    return 0;
}
