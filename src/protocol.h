#pragma once

#include "net.h"
#include "task_graph.h"

#include <cstdint>

constexpr std::uint32_t max_message = 1024 * 1024;
void send_message(const Socket& socket, const json& message);
bool receive_message(const Socket& socket, json& message);
