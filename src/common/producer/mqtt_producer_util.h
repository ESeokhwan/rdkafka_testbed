#pragma once

#include <mosquitto.h>
#include <string>

namespace common {
namespace producer {

mosquitto *create_mosq_client(std::string broker, std::string client_id);

}
}