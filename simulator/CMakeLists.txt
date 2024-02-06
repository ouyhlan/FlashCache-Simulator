cmake_minimum_required(VERSION 3.22)

# set the project name
project(Simulator VERSION 0.1)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS "-Wall -march=native -mcmodel=medium -fPIC -ffast-math -funroll-loops ${CMAKE_CXX_FLAGS}")
# set(CMAKE_CXX_FLAGS "-Wall -mcmodel=medium -fPIC -ffast-math -funroll-loops ${CMAKE_CXX_FLAGS}")

# Add googletest module
# add_subdirectory(third_party/googletest)
# include(GoogleTest)

# enable_testing()

# function (add_test_executable name)
#     add_executable(${name} ${ARGN})
#     target_link_libraries(${name} GTest::gtest_main)
#     gtest_discover_tests(${name})
# endfunction ()

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

file(GLOB utils_src "utils/*.cpp")
add_library(utils ${utils_src})
link_libraries(utils)

file(GLOB src "*.cpp")
add_executable(cache ${src})

# test executable
# file(GLOB test_files "test/*.cpp")
# foreach(test_file ${test_files})
#     get_filename_component(testname ${test_file} NAME_WLE)
#     add_test_executable(${testname} ${test_file})
# endforeach()