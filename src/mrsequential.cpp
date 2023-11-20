#include <iostream>
#include <filesystem>
#include <sstream>
#include <map>
#include <vector>
#include <string>
#include "dynamic.hpp"
#include "flags.h"
#include <fstream>


namespace fs = std::filesystem;

std::vector<fs::path> read_txt_files(const fs::path& directory) {
    std::vector<fs::path> txt_files;

    if (fs::exists(directory) && fs::is_directory(directory)) {
        // 遍历目录
        for (const auto& entry : fs::directory_iterator(directory)) {
            if (entry.is_regular_file() &&
                entry.path().extension() == ".txt" && 
                entry.path().filename().string().substr(0, 2) == "pg") {
                txt_files.push_back(entry.path());
            }
        }
    } else {
        std::cerr << "Provided path is not a directory or does not exist.\n";
    }

    return txt_files;
}


int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    auto directory = fs::current_path();
    auto mr_data_file = fs::path(FLAGS_mrdata_path);
    if (!fs::exists(directory / mr_data_file)) {
        spdlog::error("Cannot find data file: {}", mr_data_file.string());
        return -1;
    }
    auto txt_files = read_txt_files(directory / mr_data_file);
    auto app = AppCreator<MapReduceApp>::instance()->create(FLAGS_mrapp);
    if (app == nullptr) {
        spdlog::error("Cannot find app: {}", FLAGS_mrapp);
        return -1;
    }
    std::vector<KeyValue> intermediate;
    for (const auto& file : txt_files) {
        spdlog::info("Processing file: {}", file.string());
        std::ifstream file_stream(file.string());
        std::stringstream ss;
        if (file_stream) {
            ss << file_stream.rdbuf();
            file_stream.close();
            auto res = app->map(file.string(), ss.str());
            intermediate.insert(intermediate.end(), res.begin(), res.end());
        } else {
            spdlog::error("Cannot open file: {}", file.string());
        }
    }

    std::map<
        decltype(KeyValue::first), 
        std::vector<decltype(KeyValue::second)>
    > intermediate_map;
    std::sort(intermediate.begin(), intermediate.end(), [](const auto& p1, const auto& p2) {
        return p1.first < p2.first;
    });
    for (const auto& p : intermediate) {
        intermediate_map[p.first].push_back(p.second);
    }

    std::string out_file_name {"mr-out-0"};
    std::ofstream out_fss(out_file_name);
    for (const auto& [key, values] : intermediate_map) {
        out_fss << key << " " << app->reduce(key, values) << "\n";
    }
    out_fss.close();
    return 0;
}