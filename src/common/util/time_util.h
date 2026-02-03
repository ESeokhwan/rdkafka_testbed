#pragma once

#include <string>

namespace common {
namespace util {

int64_t get_current_timestamp();

int64_t get_current_nano_tick();

std::string current_time_str();

}
}