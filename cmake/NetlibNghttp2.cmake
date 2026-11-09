# Keep dependency options local to this function: BUILD_TESTING and C++ flags
# in the parent project must not be overwritten by the upstream configuration.
function(netlib_setup_nghttp2)
  set(version "1.71.0")
  set(sha256 "c0f55121d88650e4b9227aec28cebaf664d2417ca60fe70be0f8d38829f52116")
  set(archive_name "nghttp2-${version}.tar.xz")
  set(archive_url "https://github.com/nghttp2/nghttp2/releases/download/v${version}/${archive_name}")
  set(NETLIB_NGHTTP2_ARCHIVE "" CACHE FILEPATH
      "Optional local official ${archive_name} archive (SHA-256 verified)")

  if(NETLIB_NGHTTP2_ARCHIVE)
    get_filename_component(archive "${NETLIB_NGHTTP2_ARCHIVE}" ABSOLUTE
      BASE_DIR "${PROJECT_SOURCE_DIR}")
    if(NOT EXISTS "${archive}" OR IS_DIRECTORY "${archive}")
      message(FATAL_ERROR
        "NETLIB_NGHTTP2_ARCHIVE does not name an existing file: ${archive}\n"
        "Provide the official ${archive_name}, or leave this option empty to download it.")
    endif()
    file(SHA256 "${archive}" actual_sha256)
    if(NOT actual_sha256 STREQUAL sha256)
      message(FATAL_ERROR
        "NETLIB_NGHTTP2_ARCHIVE SHA-256 mismatch. netlib requires the official ${archive_name}.\n"
        "Expected: ${sha256}\nActual:   ${actual_sha256}\n"
        "Other versions, archive formats and modified/corrupt files are not accepted.")
    endif()
  else()
    set(archive "${archive_url}")
    message(STATUS
      "netlib: fetching pinned nghttp2 ${version}; for offline builds set NETLIB_NGHTTP2_ARCHIVE")
  endif()

  # New archive timestamps avoid stale build artifacts when the pin changes.
  if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
  endif()
  include(FetchContent)
  set(FETCHCONTENT_TRY_FIND_PACKAGE_MODE NEVER)
  FetchContent_Declare(nghttp2
    URL "${archive}"
    URL_HASH "SHA256=${sha256}"
    TLS_VERIFY TRUE
    TIMEOUT 120
    INACTIVITY_TIMEOUT 30
    # Populate first, then validate and configure in the scope below.
    SOURCE_SUBDIR netlib_no_automatic_subdirectory)
  FetchContent_MakeAvailable(nghttp2)

  if(NOT EXISTS "${nghttp2_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR
      "nghttp2 ${version} source is unavailable. For offline builds provide "
      "NETLIB_NGHTTP2_ARCHIVE=${archive_name}; do not use an unpopulated "
      "FETCHCONTENT_FULLY_DISCONNECTED directory.")
  endif()
  # Also check source overrides supplied by a parent FetchContent declaration.
  file(STRINGS "${nghttp2_SOURCE_DIR}/CMakeLists.txt" project_declaration
    REGEX "^project\\(nghttp2 VERSION [0-9.]+ LANGUAGES C\\)")
  if(NOT project_declaration STREQUAL "project(nghttp2 VERSION ${version} LANGUAGES C)")
    message(FATAL_ERROR
      "Unsupported nghttp2 source version; netlib pins ${version}. "
      "Use the official ${archive_name} and remove conflicting FetchContent source overrides.")
  endif()

  # Upstream's cmake_dependent_option(BUILD_TESTING) force-writes the cache,
  # even when a normal scoped variable is supplied. Preserve that cache entry.
  get_property(had_testing_cache CACHE BUILD_TESTING PROPERTY TYPE SET)
  if(had_testing_cache)
    get_property(testing_cache_value CACHE BUILD_TESTING PROPERTY VALUE)
    get_property(testing_cache_type CACHE BUILD_TESTING PROPERTY TYPE)
    get_property(testing_cache_help CACHE BUILD_TESTING PROPERTY HELPSTRING)
  endif()
  set(ENABLE_LIB_ONLY ON)
  set(BUILD_STATIC_LIBS ON)
  set(BUILD_SHARED_LIBS OFF)
  set(BUILD_TESTING OFF)
  set(ENABLE_DOC OFF)
  set(ENABLE_FAILMALLOC OFF)
  set(ENABLE_HTTP3 OFF)
  set(WITH_LIBXML2 OFF)
  set(WITH_JEMALLOC OFF)
  set(WITH_MRUBY OFF)
  set(WITH_NEVERBLEED OFF)
  set(WITH_LIBBPF OFF)
  set(WITH_WOLFSSL OFF)
  add_subdirectory("${nghttp2_SOURCE_DIR}" "${nghttp2_BINARY_DIR}" EXCLUDE_FROM_ALL)
  if(had_testing_cache)
    set(BUILD_TESTING "${testing_cache_value}" CACHE "${testing_cache_type}"
      "${testing_cache_help}" FORCE)
  else()
    unset(BUILD_TESTING CACHE)
  endif()
  if(NOT TARGET nghttp2_static)
    message(FATAL_ERROR "nghttp2 ${version} did not provide its required static library target")
  endif()

  # Private implementation dependency: netlib consumers receive static linkage
  # requirements, but not the headers or NGHTTP2_STATICLIB compile definition.
  add_library(netlib_nghttp2 INTERFACE)
  target_link_libraries(netlib_nghttp2 INTERFACE nghttp2_static)
  # Use the portable nghttp2_ssize/*2 API. MSVC has no POSIX ssize_t, which is
  # referenced by deprecated declarations unless this upstream switch is set.
  target_compile_definitions(netlib_nghttp2 INTERFACE NGHTTP2_NO_SSIZE_T)
  set_property(TARGET netlib_nghttp2 PROPERTY NETLIB_NGHTTP2_VERSION "${version}")
  target_link_libraries(netlib PRIVATE netlib_nghttp2)
  message(STATUS "netlib: nghttp2 ${version} static protocol library enabled (HTTP/2 Session is not implemented yet)")
endfunction()
