cmake_minimum_required(VERSION 3.20)

project(
  netlib
  VERSION 0.1.0
  DESCRIPTION "A high-performance Unix network library"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

add_library(
  netlib
  STATIC
    src/channel.cc
    src/event_loop.cc
    src/main_reactor.cc
    src/sub_reactor.cc
    src/thread_pool.cc
    src/unique_fd.cc)
add_library(netlib::netlib ALIAS netlib)

target_compile_features(netlib PUBLIC cxx_std_20)
target_compile_options(netlib PRIVATE -Wall -Wextra -Wpedantic)
target_include_directories(
  netlib
  SYSTEM
  PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/moodycamel")
target_include_directories(
  netlib
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_link_libraries(netlib PUBLIC Threads::Threads)

add_library(netlib_log STATIC src/log/logging.cc)
add_library(netlib::log ALIAS netlib_log)

target_compile_features(netlib_log PUBLIC cxx_std_20)
target_compile_options(netlib_log PRIVATE -Wall -Wextra -Wpedantic)
target_include_directories(
  netlib_log
  SYSTEM
  PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/moodycamel")
target_include_directories(
  netlib_log
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_link_libraries(netlib_log PUBLIC Threads::Threads)

add_executable(netlib_log_example examples/log_example.cc)
target_link_libraries(netlib_log_example PRIVATE netlib::log)

include(CTest)

if(BUILD_TESTING)
  add_executable(netlib_reactor_test tests/reactor_test.cc)
  target_link_libraries(netlib_reactor_test PRIVATE netlib::netlib)
  add_test(NAME netlib_reactor_test COMMAND netlib_reactor_test)

  add_executable(netlib_log_test tests/log_test.cc)
  target_link_libraries(netlib_log_test PRIVATE netlib::log)
  add_test(NAME netlib_log_test COMMAND netlib_log_test)
endif()
