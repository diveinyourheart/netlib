cmake_minimum_required(VERSION 3.20)

project(
  netlib
  VERSION 0.1.0
  DESCRIPTION "A cross-platform Reactor network library"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

if(NOT WIN32 AND NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "netlib currently supports Windows and Linux; add a platform backend for this OS")
endif()

function(netlib_warnings target)
  if(MSVC)
    target_compile_options(${target} PRIVATE /W4 /permissive- /utf-8)
  else()
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
  endif()
endfunction()

add_library(netlib_platform STATIC src/platform.cc)
add_library(netlib::platform ALIAS netlib_platform)
target_compile_features(netlib_platform PUBLIC cxx_std_20)
target_include_directories(netlib_platform PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
netlib_warnings(netlib_platform)
if(WIN32)
  target_compile_definitions(netlib_platform PRIVATE WIN32_LEAN_AND_MEAN NOMINMAX _WIN32_WINNT=0x0602)
  target_link_libraries(netlib_platform PUBLIC ws2_32)
endif()

add_library(
  netlib
  STATIC
    src/channel.cc
    src/connection_handle.cc
    src/event_loop.cc
    src/http1_codec.cc
    src/length_prefixed_codec.cc
    src/main_reactor.cc
    src/rpc_codec.cc
    src/sub_reactor.cc
    src/thread_pool.cc
    src/unique_fd.cc)
add_library(netlib::netlib ALIAS netlib)

target_compile_features(netlib PUBLIC cxx_std_20)
netlib_warnings(netlib)
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
target_link_libraries(netlib PUBLIC Threads::Threads netlib::platform netlib::timer)

add_library(netlib_timer STATIC src/timer.cc)
add_library(netlib::timer ALIAS netlib_timer)
target_compile_features(netlib_timer PUBLIC cxx_std_20)
target_include_directories(netlib_timer PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_link_libraries(netlib_timer PUBLIC Threads::Threads)
netlib_warnings(netlib_timer)

add_library(netlib_log STATIC src/log/logging.cc)
add_library(netlib::log ALIAS netlib_log)

target_compile_features(netlib_log PUBLIC cxx_std_20)
netlib_warnings(netlib_log)
if(WIN32)
  target_compile_definitions(netlib_log PRIVATE WIN32_LEAN_AND_MEAN NOMINMAX)
endif()
target_include_directories(
  netlib_log
  SYSTEM
  PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/moodycamel")
target_include_directories(
  netlib_log
  SYSTEM
  PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/fmt/include")
target_include_directories(
  netlib_log
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_compile_definitions(netlib_log PUBLIC FMT_HEADER_ONLY=1)
if(MSVC)
  # Public logging macros use C++20 __VA_OPT__, including in consumer code.
  target_compile_options(netlib_log PUBLIC /Zc:preprocessor /utf-8)
endif()
target_link_libraries(netlib_log PUBLIC Threads::Threads netlib::timer)

target_link_libraries(netlib PUBLIC netlib_log)

add_executable(netlib_log_example examples/log_example.cc)
target_link_libraries(netlib_log_example PRIVATE netlib::log)

include(CTest)

if(BUILD_TESTING)
  add_executable(netlib_thread_pool_test tests/thread_pool_test.cc)
  target_link_libraries(netlib_thread_pool_test PRIVATE netlib::netlib)
  add_test(NAME netlib_thread_pool_test COMMAND netlib_thread_pool_test)
  set_tests_properties(netlib_thread_pool_test PROPERTIES TIMEOUT 30)
  add_executable(netlib_timer_test tests/timer_test.cc)
  target_link_libraries(netlib_timer_test PRIVATE netlib::timer)
  add_test(NAME netlib_timer_test COMMAND netlib_timer_test)
  set_tests_properties(netlib_timer_test PROPERTIES TIMEOUT 30)
  add_executable(netlib_platform_test tests/platform_test.cc)
  target_link_libraries(netlib_platform_test PRIVATE netlib::netlib)
  add_test(NAME netlib_platform_test COMMAND netlib_platform_test)
  add_executable(netlib_reactor_test tests/reactor_test.cc)
  target_link_libraries(netlib_reactor_test PRIVATE netlib::netlib)
  add_test(NAME netlib_reactor_test COMMAND netlib_reactor_test)

  add_executable(netlib_log_test tests/log_test.cc)
  target_link_libraries(netlib_log_test PRIVATE netlib::log)
  add_test(NAME netlib_log_test_stdout_colorless
    COMMAND netlib_log_test stdout-colorless)
  add_test(NAME netlib_log_test_stdout_colored
    COMMAND netlib_log_test stdout-colored)
  add_test(NAME netlib_log_test_concurrent COMMAND netlib_log_test concurrent)
  add_test(NAME netlib_log_test_invalid COMMAND netlib_log_test invalid)
  add_test(NAME netlib_log_test_singleton COMMAND netlib_log_test singleton)
  add_executable(netlib_log_barrier_test tests/log_barrier_test.cc)
  target_link_libraries(netlib_log_barrier_test PRIVATE netlib::log)
  add_test(NAME netlib_log_barrier_test_barrier
    COMMAND netlib_log_barrier_test barrier)
  add_test(NAME netlib_log_barrier_test_periodic
    COMMAND netlib_log_barrier_test periodic)
  add_test(NAME netlib_log_barrier_test_drops
    COMMAND netlib_log_barrier_test drops)
  add_test(NAME netlib_log_barrier_test_failure
    COMMAND netlib_log_barrier_test failure)
  add_test(NAME netlib_log_barrier_test_submission_during_flush
    COMMAND netlib_log_barrier_test submission-during-flush)
  add_test(NAME netlib_log_barrier_test_flush_while_producing
    COMMAND netlib_log_barrier_test flush-while-producing)
  set_tests_properties(
    netlib_log_barrier_test_barrier
    netlib_log_barrier_test_periodic
    netlib_log_barrier_test_drops
    netlib_log_barrier_test_failure
    netlib_log_barrier_test_submission_during_flush
    netlib_log_barrier_test_flush_while_producing
    PROPERTIES TIMEOUT 30)

  add_executable(netlib_codec_test tests/codec_test.cc)
  target_link_libraries(netlib_codec_test PRIVATE netlib::netlib)
  add_test(NAME netlib_codec_test COMMAND netlib_codec_test)

  add_executable(netlib_http_codec_test tests/http_codec_test.cc)
  target_link_libraries(netlib_http_codec_test PRIVATE netlib::netlib)
  add_test(NAME netlib_http_codec_test COMMAND netlib_http_codec_test)
  add_executable(netlib_rpc_codec_test tests/rpc_codec_test.cc)
  target_link_libraries(netlib_rpc_codec_test PRIVATE netlib::netlib)
  netlib_warnings(netlib_rpc_codec_test)
  add_test(NAME netlib_rpc_codec_test COMMAND netlib_rpc_codec_test)
  set_tests_properties(netlib_rpc_codec_test PROPERTIES TIMEOUT 30)
  set_tests_properties(netlib_platform_test netlib_reactor_test
    netlib_log_test_stdout_colorless netlib_log_test_stdout_colored
    netlib_log_test_concurrent netlib_log_test_invalid
    netlib_log_test_singleton netlib_codec_test netlib_http_codec_test
    PROPERTIES TIMEOUT 120)
endif()
