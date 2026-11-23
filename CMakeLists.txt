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

set(GRPC_ASYNC_STREAM_GRPC_PROVIDER "auto" CACHE STRING "Where to take gRPC from: auto, package or fetch")
set_property(CACHE GRPC_ASYNC_STREAM_GRPC_PROVIDER PROPERTY STRINGS auto package fetch)
set(GRPC_ASYNC_STREAM_GRPC_VERSION   "1.71.0" CACHE STRING "gRPC version built from source when it is fetched")
set(GRPC_ASYNC_STREAM_CATCH2_VERSION "3.8.1"  CACHE STRING "Catch2 version used for tests and benchmarks")
set(GRPC_ASYNC_STREAM_MAGIC_ENUM_VERSION "0.9.8" CACHE STRING "magic_enum version fetched when no installed package is found")

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
# Header-only library
# ---------------------------------------------------------------------------

add_library(GrpcAsyncStream INTERFACE)
add_library(GrpcAsyncStream::GrpcAsyncStream ALIAS GrpcAsyncStream)

target_include_directories(GrpcAsyncStream INTERFACE
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(GrpcAsyncStream INTERFACE gRPC::grpc++ magic_enum::magic_enum)
target_compile_features(GrpcAsyncStream INTERFACE cxx_std_20)

# Shows the header in IDE project trees
target_sources(GrpcAsyncStream INTERFACE
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/AsyncReadWriteStream.h>)

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

    # Common test support: in-process test server and fake stream
    add_library(GrpcAsyncStreamTestSupport STATIC
        tests/support/TestServer.cpp
        tests/support/TestServer.h
        tests/support/FakeReaderWriter.h
        tests/support/StreamTestUtils.h)
    target_include_directories(GrpcAsyncStreamTestSupport PUBLIC ${PROJECT_SOURCE_DIR}/tests)
    target_link_libraries(GrpcAsyncStreamTestSupport PUBLIC
        GrpcAsyncStream::GrpcAsyncStream
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
    install(FILES include/AsyncReadWriteStream.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
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
