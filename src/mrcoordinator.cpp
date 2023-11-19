#include "mr/coordinator.h"
#include <chrono>
#include <thread>
#include <iostream>
#include "libs/flags.h"
#include "spdlog/spdlog.h"


int main() {
    std::cout << FLAGS_input_file << std::endl;
    CoorDinator cor {FLAGS_input_file, 10};
    cor.serve();
    spdlog::info("CoorDinator start serving...");
    while (!cor.done()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
}