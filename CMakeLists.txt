# 3.25: SYSTEM subprojects (third-party warnings stay out of -Werror / /WX builds on every platform)
cmake_minimum_required(VERSION 3.25)

project(GrpcAsyncStream
    VERSION 2.0.0
    DESCRIPTION "Header-only C++ wrapper over the gRPC asynchronous bidirectional client stream"
    HOMEPAGE_URL "https://github.com/egorodet/GrpcAsyncStream"
    LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------

if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    set(GRPC_ASYNC_STREAM_IS_TOP_LEVEL ON)
else()
    set(GRPC_ASYNC_STREAM_IS_TOP_LEVEL OFF)
endif()

option(GRPC_ASYNC_STREAM_BUILD_TESTS      "Build GrpcAsyncStream unit and integration tests" ${GRPC_ASYNC_STREAM_IS_TOP_LEVEL})
option(GRPC_ASYNC_STREAM_BUILD_BENCHMARKS "Build GrpcAsyncStream heavy-load benchmarks"      ${GRPC_ASYNC_STREAM_IS_TOP_LEVEL})
option(GRPC_ASYNC_STREAM_ENABLE_WARNINGS  "Compile tests with extra warnings as errors"       ${GRPC_ASYNC_STREAM_IS_TOP_LEVEL})
option(GRPC_ASYNC_STREAM_ENABLE_SANITIZERS "Build tests with the sanitizers supported by the toolchain" OFF)
option(GRPC_ASYNC_STREAM_HEAP_TRACKING    "Track C++ heap usage in the benchmarks (off automatically with sanitizers)" ON)
option(GRPC_ASYNC_STREAM_INSTALL          "Generate install and package-config rules"        ${GRPC_ASYNC_STREAM_IS_TOP_LEVEL})
option(GRPC_ASYNC_STREAM_BUILD_TEST_UTILS "Provide the GrpcAsyncStream::TestUtils target (grpc_test_utils headers) for tests of other projects"
       ${GRPC_ASYNC_STREAM_IS_TOP_LEVEL})

# This project's own tests and benchmarks are built on the test utilities
if((GRPC_ASYNC_STREAM_BUILD_TESTS OR GRPC_ASYNC_STREAM_BUILD_BENCHMARKS) AND NOT GRPC_ASYNC_STREAM_BUILD_TEST_UTILS)
    message(STATUS "GrpcAsyncStream: enabling GRPC_ASYNC_STREAM_BUILD_TEST_UTILS, required by the tests and benchmarks")
    set(GRPC_ASYNC_STREAM_BUILD_TEST_UTILS ON)
endif()

set(GRPC_ASYNC_STREAM_GRPC_PROVIDER "auto" CACHE STRING "Where to take gRPC from: auto, package or fetch")
set_property(CACHE GRPC_ASYNC_STREAM_GRPC_PROVIDER PROPERTY STRINGS auto package fetch)
set(GRPC_ASYNC_STREAM_GRPC_VERSION   "1.71.0" CACHE STRING "gRPC version built from source when it is fetched")
set(GRPC_ASYNC_STREAM_CATCH2_VERSION "3.8.1"  CACHE STRING "Catch2 version used for tests and benchmarks")
set(GRPC_ASYNC_STREAM_MAGIC_ENUM_VERSION "0.9.8" CACHE STRING "magic_enum version used by the test utilities")

if(GRPC_ASYNC_STREAM_IS_TOP_LEVEL)
    # The whole build, gRPC and Abseil included, uses one C++ standard: Abseil's
    # ABI (e.g. absl ordering types) depends on it. C++20 is the minimum.
    if(NOT DEFINED CMAKE_CXX_STANDARD OR CMAKE_CXX_STANDARD LESS 20)
        set(CMAKE_CXX_STANDARD 20)
    endif()
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
    if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
        set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
    endif()
endif()

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

include(GNUInstallDirs)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

include(Dependencies)

# ---------------------------------------------------------------------------
# Header-only library: <grpc_utils/...>, depends on gRPC only
# ---------------------------------------------------------------------------

add_library(GrpcAsyncStream INTERFACE)
add_library(GrpcAsyncStream::GrpcAsyncStream ALIAS GrpcAsyncStream)

target_include_directories(GrpcAsyncStream INTERFACE
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(GrpcAsyncStream INTERFACE gRPC::grpc++)
target_compile_features(GrpcAsyncStream INTERFACE cxx_std_20)

# Shows the header in IDE project trees
target_sources(GrpcAsyncStream INTERFACE
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/grpc_utils/AsyncReadWriteStream.h>)

# ---------------------------------------------------------------------------
# Header-only test utilities: <grpc_test_utils/...>, for tests of code built on
# the library (this project's and other projects' tests alike)
#
#   StreamTestUtils.h     polling helpers and message-sequence checks   (gRPC)
#   FakeReaderWriter.h    deterministic fake of the async bidi stream   (gRPC)
#   InProcessServer.h     gRPC server reachable through in-process channels (gRPC)
#   EnumReflection.h      names / indexes of the library enums          (magic_enum)
#   Catch2StringMakers.h  readable Catch2 output for the enums and grpc::StatusCode
#                         (Catch2: link your test target to Catch2 to use this header)
# ---------------------------------------------------------------------------

set(GRPC_ASYNC_STREAM_TEST_UTILS_HEADERS
    include/grpc_test_utils/Catch2StringMakers.h
    include/grpc_test_utils/EnumReflection.h
    include/grpc_test_utils/FakeReaderWriter.h
    include/grpc_test_utils/InProcessServer.h
    include/grpc_test_utils/StreamTestUtils.h)

if(GRPC_ASYNC_STREAM_BUILD_TEST_UTILS)
    grpc_async_stream_add_magic_enum()

    add_library(GrpcAsyncStreamTestUtils INTERFACE)
    add_library(GrpcAsyncStream::TestUtils ALIAS GrpcAsyncStreamTestUtils)
    set_target_properties(GrpcAsyncStreamTestUtils PROPERTIES EXPORT_NAME TestUtils)

    target_include_directories(GrpcAsyncStreamTestUtils INTERFACE
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    target_link_libraries(GrpcAsyncStreamTestUtils INTERFACE
        GrpcAsyncStream::GrpcAsyncStream
        magic_enum::magic_enum)
    target_compile_features(GrpcAsyncStreamTestUtils INTERFACE cxx_std_20)

    # Shows the headers in IDE project trees (one generator expression per header)
    foreach(header IN LISTS GRPC_ASYNC_STREAM_TEST_UTILS_HEADERS)
        target_sources(GrpcAsyncStreamTestUtils INTERFACE $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/${header}>)
    endforeach()
endif()

# ---------------------------------------------------------------------------
# Tests and benchmarks
# ---------------------------------------------------------------------------

if(GRPC_ASYNC_STREAM_BUILD_TESTS OR GRPC_ASYNC_STREAM_BUILD_BENCHMARKS)
    include(GrpcProtoGen)
    include(CompilerOptions)
    grpc_async_stream_add_catch2()
    include(CTest)
    include(Catch)

    # Test protocol shared by tests and benchmarks
    grpc_async_stream_add_proto_library(GrpcAsyncStreamTestProto
        PROTOS ${PROJECT_SOURCE_DIR}/tests/proto/test_stream.proto)

    # This repository's own test service (tests/proto) on top of the test utilities
    add_library(GrpcAsyncStreamTestSupport STATIC
        tests/support/TestServer.cpp
        tests/support/TestServer.h)
    target_include_directories(GrpcAsyncStreamTestSupport PUBLIC ${PROJECT_SOURCE_DIR}/tests)
    target_link_libraries(GrpcAsyncStreamTestSupport PUBLIC
        GrpcAsyncStream::TestUtils
        GrpcAsyncStreamTestProto)

    grpc_async_stream_configure_test_target(GrpcAsyncStreamTestSupport)
endif()

if(GRPC_ASYNC_STREAM_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(GRPC_ASYNC_STREAM_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

# ---------------------------------------------------------------------------
# Installation (only meaningful when gRPC comes from an installed package)
# ---------------------------------------------------------------------------

if(GRPC_ASYNC_STREAM_INSTALL AND GRPC_ASYNC_STREAM_GRPC_FOUND_AS_PACKAGE)
    include(CMakePackageConfigHelpers)

    install(TARGETS GrpcAsyncStream EXPORT GrpcAsyncStreamTargets)
    install(DIRECTORY include/grpc_utils DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    if(GRPC_ASYNC_STREAM_BUILD_TEST_UTILS)
        install(TARGETS GrpcAsyncStreamTestUtils EXPORT GrpcAsyncStreamTargets)
        install(DIRECTORY include/grpc_test_utils DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    endif()
    install(EXPORT GrpcAsyncStreamTargets
        NAMESPACE GrpcAsyncStream::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/GrpcAsyncStream)

    configure_package_config_file(
        cmake/GrpcAsyncStreamConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/GrpcAsyncStreamConfig.cmake
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/GrpcAsyncStream)
    write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/GrpcAsyncStreamConfigVersion.cmake
        COMPATIBILITY SameMajorVersion
        ARCH_INDEPENDENT)
    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/GrpcAsyncStreamConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/GrpcAsyncStreamConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/GrpcAsyncStream)
elseif(GRPC_ASYNC_STREAM_INSTALL AND GRPC_ASYNC_STREAM_IS_TOP_LEVEL)
    message(STATUS "GrpcAsyncStream: install rules skipped because gRPC is built from source")
endif()
