#pragma once

#include <cstddef>
#include <vector>
#include <random>

namespace common {
namespace util {

const size_t SMALL_NOISE_LIST_LENGTH = 1000;

const size_t MAX_NOISE_LIST_LENGTH = 1000000;

class Noises {
private:
    std::vector<int> noises;

    int cur_idx;

public:
    Noises(std::vector<int> noises);
    Noises();
    ~Noises() = default;

    int get_cur_idx();
    int next();
};

Noises generate_noises(double stddev, int max_abs_noise, int length, std::mt19937& random_engine);

Noises empty_noises();

}
}