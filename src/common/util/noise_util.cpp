#include "util/noise_util.h"

namespace common {
namespace util {

Noises::Noises(std::vector<double> noises) : noises(std::move(noises)), cur_idx(0) {}

Noises::Noises() : noises(std::vector<double>()), cur_idx(0) {}

size_t Noises::get_cur_idx() {
    return this->cur_idx;
}

double Noises::next() {
    double prev_noise = 0;
    if (!noises.empty()) {
        prev_noise = this->noises[this->cur_idx];
        this->cur_idx = (this->cur_idx + 1) % noises.size();
    }
    return prev_noise;
}

Noises generate_noises(double stddev, double max_abs_noise, size_t length, std::mt19937& random_engine) {
    std::vector<double> noises;
    if (max_abs_noise < 0) max_abs_noise = -1 * max_abs_noise;
    std::normal_distribution<double> dist(0.0, stddev);
    for (size_t i = 0; i < length; i++) {
        double noise = dist(random_engine);
        if (noise > max_abs_noise) noise = max_abs_noise;
        if (noise < -1 * max_abs_noise) noise = -1 * max_abs_noise;
        noises.push_back(noise);
    }
    return Noises(noises);
}

Noises empty_noises() {
    return Noises();
}

}
}