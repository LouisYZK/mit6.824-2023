cmake_minimum_required(VERSION 3.10)
project(map_reduce)

# 设置C++标准为C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
file(GLOB SOURCES "*.cpp")
add_library(mrapps SHARED ${SOURCES})
target_include_directories(mrapps PUBLIC ../libs) 