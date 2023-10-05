#include "cmdline.h"
#include "common.h"
#include "generator.h"
#include "scrambled_zipfian_generator.h"
#include "zipfian_generator.h"
#include <cstdint>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>

int main(int argc, char* argv[])
{
    cmdline::parser a;
    a.add<std::string>("filename", 'f', "workload file name", false, "test");
    a.add<int>("keynum", 'n', "num of generated_keys to generate", false, 100000000);
    a.add<double>("zipfianconst", 'a', "zipfianconst", false, 0.99);
    a.add<uint64_t>("elementnum", 'e', "num of elements of zipfian dist.", false, 2500);
    a.add("scramble", 's', "whether scramble or not");
    a.add("nowrite", 'q', "don't generate file if true");
    a.add("showinfo", 'v', "show debug info if true");
    a.parse_check(argc, argv);
    // a.add<int>("elementnum", 'e', "num of elements of zipfian dist.", true, (1ULL << 31) - 1;); // 32bit zipfian dist.

    double zipfian_const = a.get<double>("zipfianconst");
    int key_num = a.get<int>("keynum");
    uint64_t min = 0;
    uint64_t num_devide = a.get<uint64_t>("elementnum");

    ycsbc::Generator<uint64_t>* generator;
    if (a.exist("scramble")) {
        generator = new ycsbc::ScrambledZipfianGenerator(min, num_devide, zipfian_const);
    } else
        generator = new ycsbc::ZipfianGenerator(min, num_devide, zipfian_const);

    key_int64_t* generated_keys = (key_int64_t*)malloc(sizeof(key_int64_t) * key_num);
    if (generated_keys == NULL) {
        std::cerr << "Memory cannot be allocated." << std::endl;
        return 1;
    }
    std::random_device rnd;
    std::mt19937_64 mt(rnd());
    uint64_t range = std::numeric_limits<uint64_t>::max() / num_devide;  // key range per 1 division
    if (a.exist("showinfo")) {
        std::cout << "range: " << range << std::endl;
    }
    std::uniform_int_distribution<uint64_t> rand_in_range(0, range);
    uint64_t distribution[num_devide];
    for (int i = 0; i < key_num; i++) {
        uint64_t which_range = generator->Next();
        uint64_t in_range = rand_in_range(mt);
        generated_keys[i] = which_range * range + in_range;
        distribution[which_range]++;
        if (a.exist("showinfo")) {
            // std::cout << which_range << "th range, offset: " << in_range << std::endl;
            // std::cout << "generated key = " << generated_keys[i] << std::endl;
        }
    }
    for (int i = 0; i < 10; i++) {
        if (a.exist("showinfo")) {
            std::cout << "num keys in " << i << "th range: " << distribution[i] << ", " << 100 * (double)distribution[i] / key_num << "%" << std::endl;
        }
    }

    if (!a.exist("nowrite")) {
        std::ofstream writing_file;
        std::stringstream ss;
        ss << "./workload/zipf_const_" << zipfian_const << ".bin";
        std::string filename = ss.str();
        writing_file.open(filename, std::ios::binary);
        if (!writing_file) {
            std::cerr << "cannot open file " << filename << std::endl;
            return 1;
        }
        writing_file.write((const char*)generated_keys, sizeof(key_int64_t) * key_num);
        free(generated_keys);
        std::cout << filename << " written, num of reqs = " << key_num << std::endl;
    }
    return 0;
}