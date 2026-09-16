#pragma once

#include <array>
#include <cstdint>
#include <limits>


// from https://prng.di.unimi.it/xoshiro256plusplus.c
struct xoshiro256pp {
    using result_type = uint64_t;

    explicit xoshiro256pp(result_type seed);
    constexpr result_type operator()();
    constexpr static result_type min() { return 0; }
    constexpr static result_type max() { return std::numeric_limits<result_type>::max(); }

    // equivalent to 2^128 calls to operator()
    constexpr void jump();

private:
    static constexpr uint64_t uint64_rotl(uint64_t x, int k);

    std::array<uint64_t, 4> states;
};
inline xoshiro256pp::xoshiro256pp(result_type seed)
{
    for (auto& state : states) {
        // SplitMix64 from https://prng.di.unimi.it/splitmix64.c
        uint64_t z = (seed += 0x9e3779b97f4a7c15);
        z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
        z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
        state = z ^ (z >> 31);
    }
}
inline constexpr auto xoshiro256pp::operator()() -> result_type
{
    const uint64_t result = uint64_rotl(states[0] + states[3], 23) + states[0];

    const uint64_t t = states[1] << 17;

    states[2] ^= states[0];
    states[3] ^= states[1];
    states[1] ^= states[2];
    states[0] ^= states[3];

    states[2] ^= t;

    states[3] = uint64_rotl(states[3], 45);

    return result;
}
inline constexpr void xoshiro256pp::jump()
{
    constexpr uint64_t JUMP[] = {0x180ec6d33cfd0aba, 0xd5a61266f0c9392c, 0xa9582618e03fc9aa, 0x39abdc4529b1661c};

    uint64_t s0 = 0;
    uint64_t s1 = 0;
    uint64_t s2 = 0;
    uint64_t s3 = 0;
    for (unsigned i = 0; i < sizeof(JUMP) / sizeof(JUMP[0]); i++)
        for (int b = 0; b < 64; b++) {
            if (JUMP[i] & UINT64_C(1) << b) {
                s0 ^= states[0];
                s1 ^= states[1];
                s2 ^= states[2];
                s3 ^= states[3];
            }
            (*this)();
        }

    states[0] = s0;
    states[1] = s1;
    states[2] = s2;
    states[3] = s3;
}
inline constexpr uint64_t xoshiro256pp::uint64_rotl(uint64_t x, int k)
{
    return (x << k) | (x >> (64 - k));
}
