cmake_minimum_required(VERSION 3.22)

# set the project name
project(Simulator VERSION 0.1)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS "-Wall -march=native -mcmodel=medium -fPIC -ffast-math -funroll-loops ${CMAKE_CXX_FLAGS}")

include_directories(./)

link_libraries(config++)

add_library(admission admission/admission.cpp)
link_libraries(admission)

add_library(parsers parsers/parser.cpp)
link_libraries(parsers)

add_library(stats stats/stats.cpp)
link_libraries(stats)

file(GLOB caches_src "caches/*.cpp")
add_library(caches ${caches_src})
link_libraries(caches)

file(GLOB src "*.cpp")
add_executable(cache ${src})
