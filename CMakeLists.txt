cmake_minimum_required(VERSION 3.17)
project(uringtest)

set(CMAKE_CXX_STANDARD 17)

add_executable(uringtest main.cpp)

target_link_libraries(uringtest uring)