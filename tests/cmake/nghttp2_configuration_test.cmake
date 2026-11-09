cmake_minimum_required(VERSION 3.20)

if(NOT NETLIB_SOURCE_DIR OR NOT NETLIB_TEST_BINARY_DIR)
  message(FATAL_ERROR "NETLIB_SOURCE_DIR and NETLIB_TEST_BINARY_DIR are required")
endif()
file(MAKE_DIRECTORY "${NETLIB_TEST_BINARY_DIR}")
set(invalid_archive "${NETLIB_TEST_BINARY_DIR}/wrong-version.tar.xz")
file(WRITE "${invalid_archive}" "not the pinned nghttp2 release archive\n")
set(missing_archive "${NETLIB_TEST_BINARY_DIR}/missing.tar.xz")
set(configure_args -G "${NETLIB_TEST_GENERATOR}"
  "-DCMAKE_CXX_COMPILER=${NETLIB_TEST_CXX_COMPILER}"
  "-DCMAKE_MAKE_PROGRAM=${NETLIB_TEST_MAKE_PROGRAM}"
  -DBUILD_TESTING=OFF)

function(check_configure name enabled archive expected_error)
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -S "${NETLIB_SOURCE_DIR}"
      -B "${NETLIB_TEST_BINARY_DIR}/${name}" ${configure_args}
      "-DNETLIB_ENABLE_HTTP2=${enabled}"
      "-DNETLIB_NGHTTP2_ARCHIVE=${archive}"
      ${ARGN}
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error
    TIMEOUT 45)
  if(expected_error)
    if(result EQUAL 0 OR NOT "${output}\n${error}" MATCHES "${expected_error}")
      message(FATAL_ERROR "${name} did not fail with the expected diagnostic:\n${output}\n${error}")
    endif()
  elseif(NOT result EQUAL 0)
    message(FATAL_ERROR "${name} failed:\n${output}\n${error}")
  endif()
endfunction()

# OFF must not inspect archives, download sources, or enable a C compiler.
check_configure(disabled OFF "${missing_archive}" ""
  -DCMAKE_C_COMPILER=netlib-c-compiler-must-not-be-enabled)
if(EXISTS "${NETLIB_TEST_BINARY_DIR}/disabled/_deps")
  message(FATAL_ERROR "HTTP/2 OFF populated a dependency directory")
endif()
file(GLOB c_compiler_files
  "${NETLIB_TEST_BINARY_DIR}/disabled/CMakeFiles/*/CMakeCCompiler.cmake")
if(c_compiler_files)
  message(FATAL_ERROR "HTTP/2 OFF unexpectedly enabled the C language")
endif()
check_configure(missing ON "${missing_archive}" "NETLIB_NGHTTP2_ARCHIVE does not name an existing file")
check_configure(mismatched ON "${invalid_archive}" "NETLIB_NGHTTP2_ARCHIVE SHA-256 mismatch")
set(unsupported_source "${NETLIB_TEST_BINARY_DIR}/unsupported-source")
file(MAKE_DIRECTORY "${unsupported_source}")
file(WRITE "${unsupported_source}/CMakeLists.txt"
  "project(nghttp2 VERSION 0.0.0 LANGUAGES C)\n")
check_configure(unsupported ON "" "Unsupported nghttp2 source version"
  "-DFETCHCONTENT_SOURCE_DIR_NGHTTP2=${unsupported_source}")
message(STATUS "nghttp2 OFF isolation and missing/mismatched/incompatible dependency diagnostics passed")
