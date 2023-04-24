#pragma once

#include <iostream>
#include <random>
#include <vector>

class rand_generator
{
public:
    static void init()
    {
        mt64.seed(rnd());
        mt32.seed(rnd());
    }
    static uint64_t rand64() { return mt64(); }
    static uint32_t rand32() { return mt32(); }

private:
    static inline std::mt19937_64 mt64;
    static inline std::mt19937 mt32;
    static inline std::random_device rnd;
};