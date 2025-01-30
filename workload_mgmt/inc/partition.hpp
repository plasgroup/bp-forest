#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <ios>
#include <iostream>
#include <string>
#include <vector>


struct Partition {
    int64_t left_key;
    size_t length;
};

inline void store_partition(const std::string& file_name, const std::vector<Partition>& partition)
{
    std::ofstream file{file_name, std::ios::binary};
    if (!file) {
        std::cerr << "cannot open file: " << file_name << std::endl;
        exit(1);
    }
    file.write(reinterpret_cast<const char*>(&partition[0]), static_cast<std::streamsize>(sizeof(Partition) * partition.size()));
}
