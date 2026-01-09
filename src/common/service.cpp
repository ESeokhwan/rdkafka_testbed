#include "service.h"
#include <atomic>

namespace common {

AbstractService::AbstractService(int round_cnt, double interval, util::Noises noises) :
    round_cnt(round_cnt), interval(interval), noises(std::move(noises)), cur_idx_reserved(0) {}

double AbstractService::cur_interval() {
    int cur_noise = noises.next();
    return interval + cur_noise;
}

bool AbstractService::has_more() {
    return cur_idx_reserved < round_cnt;
}

bool AbstractService::reserve() {
    int after_reserve = cur_idx_reserved.fetch_add(1) + 1;
    return after_reserve < round_cnt;
}

bool AbstractService::close_scheduled() {
    return close_scheduled_flag.exchange(true, std::memory_order_relaxed);
}


}

