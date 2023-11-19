#include <future>
#include <vector>
#pragma once
#include <thread>
#include "rest_rpc.hpp"

struct MapTask {
	int id;
	std::string name;
	MSGPACK_DEFINE(id, name);
};

MapTask get_task(rest_rpc::rpc_service::rpc_conn conn) {
	return { 1, "tom"};
}


class CoorDinator {
public:
    CoorDinator(std::string& file_pattern, int n_reduce) : task_files_(file_pattern), n_reduce_(n_reduce){
        
    }

    bool done() {
        return false;
    }

    int serve() {
        // auto fut = std::async(std::launch::async, [&server]() {
        //     server.run();
        // });
        std::thread t([]() {
            rest_rpc::rpc_service::rpc_server server(9000, std::thread::hardware_concurrency());
            server.register_handler("get_task", get_task);
            server.run();
        });
        t.detach();
        return 0;
    }

    std::string task_files_;
    int n_reduce_;
};