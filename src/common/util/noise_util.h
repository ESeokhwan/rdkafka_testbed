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
    std::vector<double> noises;

    size_t cur_idx;

public:
    Noises(std::vector<double> noises);
    Noises();
    ~Noises() = default;

    size_t get_cur_idx();
    double next();
};

Noises generate_noises(double stddev, double max_abs_noise, size_t length, std::mt19937& random_engine);

Noises empty_noises();

}
}