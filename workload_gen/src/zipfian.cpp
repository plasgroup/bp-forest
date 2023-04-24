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
    a.add<uint64_t>("elementnum", 'e', "num of elements of zipfian dist.", false, 2048);
    a.add("scramble", 's', "whether scramble or not");
    a.parse_check(argc, argv);
    //a.add<int>("elementnum", 'e', "num of elements of zipfian dist.", true, (1ULL << 31) - 1;); // 32bit zipfian dist.

    double zipfian_const = a.get<double>("zipfianconst");
    int key_num = a.get<int>("keynum");
    uint64_t min = 0;
    uint64_t num_devide = a.get<uint64_t>("elementnum");

    ycsbc::Generator<uint64_t>* generator;
    if (a.exist("scramble")) {
        generator = new ycsbc::ScrambledZipfianGenerator(min, num_devide, zipfian_const);
    } else
        generator = new ycsbc::ZipfianGenerator(min, num_devide, zipfian_const);

    intkey_t* generated_keys = (intkey_t*)malloc(sizeof(intkey_t) * key_num);
    if (generated_keys == NULL) {
        std::cerr << "Memory cannot be allocated." << std::endl;
        return 1;
    }
    std::random_device rnd;
    std::mt19937_64 mt(rnd());
    uint64_t range = std::numeric_limits<uint64_t>::max() / num_devide;  // key range per 1 division
    std::uniform_int_distribution<uint64_t> rand_range(0, range);
    for (int i = 0; i < key_num; i++) {
        generated_keys[i] = generator->Next() * range + rand_range(mt);
    }
    std::cout << "range: " << range << std::endl;

    std::ofstream writing_file;
    std::stringstream ss;
    ss << "./workload/zipf_const_" << zipfian_const << ".bin";
    std::string filename = ss.str();

    writing_file.open(filename, std::ios::binary);
    if (!writing_file) {
        std::cerr << "cannot open file " << filename << std::endl;
        return 1;
    }
    writing_file.write((const char*)generated_keys, sizeof(intkey_t) * key_num);
    free(generated_keys);
    return 0;
}