#pragma once

#include "util/noise_util.h"
#include <atomic>

namespace common {

class IService {
public:
    virtual ~IService() = default;

    virtual int cur_interval() = 0;
    virtual bool has_more() = 0;
    virtual bool is_done() = 0;
    virtual bool reserve() = 0;
    virtual void work() = 0;
    virtual bool close_scheduled() = 0;
    virtual void close() = 0;
};

class AbstractService: public IService {
public:
    AbstractService(int round_cnt, int interval, util::Noises noises);
    ~AbstractService() override = default;

    int cur_interval() override;
    bool has_more() override;
    bool is_done() override = 0;
    bool reserve() override;
    void work() override = 0;
    bool close_scheduled() override;
    void close() override = 0;

protected:
    int round_cnt;
    int interval;
    util::Noises noises;

    std::atomic<int> cur_idx_reserved;
    std::atomic<bool> close_scheduled_flag;
};

}