#pragma once
#include "rest_rpc.hpp"
#include "coordinator.h"
#include "rest_rpc/rpc_client.hpp"
#include "spdlog/spdlog.h"

using rest_rpc::rpc_client;

class Worker {
public:
    Worker() =default;
    MapTask get_task() {
        rpc_client client("127.0.0.1", 9000);
        client.connect();
        
        MapTask result = client.call<MapTask>("get_task");
        // client.run();
        spdlog::info("get task: {}", result.name);
        return result;
    }
};