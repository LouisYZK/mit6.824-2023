cmake_minimum_required(VERSION 3.10)
project(map_reduce)

# 设置C++标准为C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
# 开启compile_commands.json 生成，给clangd用
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
find_package(Boost REQUIRED)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
endif()

include_directories(${CMAKE_SOURCE_DIR}/third_party/nameof/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/spdlog/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/rest_rpc/thirdparty/msgpack-c/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/rest_rpc/thirdparty/asio)
include_directories(${CMAKE_SOURCE_DIR}/third_party/rest_rpc/include)

add_subdirectory(mrapps)
add_subdirectory(libs)
add_subdirectory(third_party/gflags)


# 添加执行文件
add_executable(mrseq src/mrsequential.cpp )
target_include_directories(mrseq PUBLIC libs)
target_link_libraries(mrseq PUBLIC mrapps)
target_link_libraries(mrseq PUBLIC libs)


add_executable(mrcooridnator src/mrcoordinator.cpp)
target_link_libraries(mrcooridnator PUBLIC libs)
target_include_directories(mrcooridnator PUBLIC ${CMAKE_SOURCE_DIR})


add_executable(mrworker src/mrworker.cpp)
target_link_libraries(mrworker PUBLIC libs)
target_include_directories(mrworker PUBLIC ${CMAKE_SOURCE_DIR})