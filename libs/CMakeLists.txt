cmake_minimum_required(VERSION 3.10)
project(map_reduce)

# 设置C++标准为C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_library(libs SHARED dynamic.hpp flags.cpp)
# target_include_directories(libs PUBLIC .)
set_target_properties(libs PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(libs PUBLIC gflags)

