# Dependency resolution for GrpcAsyncStream.
#
# Every dependency is resolved in the same order, so that libraries provided by a
# package manager (Conan's CMakeDeps, vcpkg, system packages) or by a parent
# project are used instead of building another copy:
#
#   1. targets already imported into this build, e.g. by Conan's CMakeDeps
#      (find_package() in a parent project or a toolchain) or by the consumer;
#   2. an installed package found with find_package(<Name> CONFIG), which also
#      finds the <name>-config.cmake files that `conan install` generates when the
#      build uses Conan's toolchain (conan_toolchain.cmake / CMake presets);
#   3. otherwise the pinned version is fetched and built by CPM.
#
# gRPC (required by the header itself):
#   GRPC_ASYNC_STREAM_GRPC_PROVIDER = auto    - steps 1, 2, 3 as above (default)
#                                     package - steps 1, 2; fail if gRPC is not provided
#                                     fetch   - build gRPC from source via CPM, unless
#                                               its targets are already imported (step 1)
#
# magic_enum (test utilities) and Catch2 (this project's tests and benchmarks)
# follow steps 1-3; the library header depends on neither.
#
# After this file the following normalized targets exist:
#   gRPC::grpc++            - gRPC C++ library
#   protobuf::libprotobuf   - protobuf runtime
#   GrpcAsyncStream_protoc  - path of the protoc executable        (variable)
#   GrpcAsyncStream_grpc_cpp_plugin - path of grpc_cpp_plugin      (variable, generator expression)
#   GRPC_ASYNC_STREAM_GRPC_SOURCE    - where gRPC came from: imported, package or fetched (variable)
#
# When gRPC is built from source with GRPC_ASYNC_STREAM_GRPC_UNSECURE=ON (default for
# the top-level project), gRPC::grpc++ refers to grpc++_unsecure: gRPC without TLS
# credentials, xDS and RE2-based policies. That builds about 40% less of gRPC core,
# no SSL library at all (BoringSSL is neither cloned nor built, a system OpenSSL is
# not used) and no RE2. Insecure and in-process channels work as usual.

include_guard(GLOBAL)

include(${CMAKE_CURRENT_LIST_DIR}/CPM.cmake)

# ---------------------------------------------------------------------------
# gRPC
# ---------------------------------------------------------------------------

# GRPC_ASYNC_STREAM_GRPC_FOUND_AS_PACKAGE: gRPC is provided from outside this
# build (imported targets or an installed package), so install rules can refer to it
set(GRPC_ASYNC_STREAM_GRPC_FOUND_AS_PACKAGE OFF)
set(GRPC_ASYNC_STREAM_GRPC_SOURCE "")

if(NOT GRPC_ASYNC_STREAM_GRPC_PROVIDER MATCHES "^(auto|package|fetch)$")
    message(FATAL_ERROR "GrpcAsyncStream: GRPC_ASYNC_STREAM_GRPC_PROVIDER must be auto, package or fetch, "
                        "not '${GRPC_ASYNC_STREAM_GRPC_PROVIDER}'")
endif()

# 1. Targets already imported (e.g. Conan's CMakeDeps via a parent project's find_package)
if(TARGET gRPC::grpc++)
    set(GRPC_ASYNC_STREAM_GRPC_SOURCE "imported")
    if(GRPC_ASYNC_STREAM_GRPC_PROVIDER STREQUAL "fetch")
        message(STATUS "GrpcAsyncStream: gRPC targets are already imported; using them instead of fetching gRPC")
    endif()
# 2. Installed package (Conan-generated or installed gRPCConfig.cmake)
elseif(NOT GRPC_ASYNC_STREAM_GRPC_PROVIDER STREQUAL "fetch")
    find_package(gRPC CONFIG QUIET)
    if(gRPC_FOUND AND TARGET gRPC::grpc++)
        set(GRPC_ASYNC_STREAM_GRPC_SOURCE "package")
    elseif(GRPC_ASYNC_STREAM_GRPC_PROVIDER STREQUAL "package")
        message(FATAL_ERROR
            "GrpcAsyncStream: gRPC package not found. With Conan, run `conan install` and configure with "
            "its toolchain (conan_toolchain.cmake or the generated CMake preset); otherwise set "
            "CMAKE_PREFIX_PATH to the gRPC install prefix, or use -DGRPC_ASYNC_STREAM_GRPC_PROVIDER=fetch")
    endif()
endif()

if(GRPC_ASYNC_STREAM_GRPC_SOURCE)
    # The protobuf runtime normally comes with gRPC's package; find it explicitly otherwise
    if(NOT TARGET protobuf::libprotobuf)
        find_package(Protobuf CONFIG QUIET)
        if(NOT TARGET protobuf::libprotobuf)
            find_package(Protobuf REQUIRED)
        endif()
    endif()
    set(GRPC_ASYNC_STREAM_GRPC_FOUND_AS_PACKAGE ON)
    if(gRPC_VERSION)
        set(_grpc_version_text " ${gRPC_VERSION}")
    endif()
    if(gRPC_DIR)
        set(_grpc_location_text " from ${gRPC_DIR}")
    endif()
    message(STATUS "GrpcAsyncStream: using ${GRPC_ASYNC_STREAM_GRPC_SOURCE} gRPC${_grpc_version_text}${_grpc_location_text}")
endif()

# 3. Build gRPC from source
if(NOT GRPC_ASYNC_STREAM_GRPC_FOUND_AS_PACKAGE)
    set(GRPC_ASYNC_STREAM_GRPC_SOURCE "fetched")
    message(STATUS "GrpcAsyncStream: building gRPC v${GRPC_ASYNC_STREAM_GRPC_VERSION} from source "
                   "(this takes a while on the first build; set CPM_SOURCE_CACHE to reuse the sources)")

    set(_grpc_submodules
        "third_party/abseil-cpp"
        "third_party/protobuf"
        "third_party/re2"
        "third_party/cares/cares"
        "third_party/zlib")
    set(_grpc_extra_options)

    # Prefer system OpenSSL / zlib when available: it saves building BoringSSL and zlib
    find_package(ZLIB QUIET)
    if(GRPC_ASYNC_STREAM_GRPC_UNSECURE)
        # grpc++_unsecure links no SSL library. gRPC still configures one, so point
        # its BoringSSL module at an empty directory: nothing is cloned or built,
        # and no system OpenSSL (whatever its version) is touched.
        set(_grpc_no_ssl_dir "${CMAKE_CURRENT_BINARY_DIR}/_deps/grpc-no-boringssl")
        file(MAKE_DIRECTORY "${_grpc_no_ssl_dir}")
        file(CONFIGURE OUTPUT "${_grpc_no_ssl_dir}/CMakeLists.txt"
             CONTENT "# Intentionally empty: GrpcAsyncStream builds grpc++_unsecure, which needs no SSL library\n")
        set(_grpc_ssl_provider module)
        list(APPEND _grpc_extra_options "BORINGSSL_ROOT_DIR ${_grpc_no_ssl_dir}")
        message(STATUS "GrpcAsyncStream: building gRPC without TLS (grpc++_unsecure); "
                       "set GRPC_ASYNC_STREAM_GRPC_UNSECURE=OFF for the full library")
    else()
        find_package(OpenSSL QUIET)
        if(OpenSSL_FOUND)
            set(_grpc_ssl_provider package)
        else()
            set(_grpc_ssl_provider module)
            list(APPEND _grpc_submodules "third_party/boringssl-with-bazel")
        endif()
    endif()
    if(ZLIB_FOUND)
        set(_grpc_zlib_provider package)
    else()
        set(_grpc_zlib_provider module)
    endif()

    # Bundled BoringSSL needs NASM for its assembly on Windows; without NASM use
    # the portable C implementation instead of failing the build
    set(_grpc_openssl_no_asm OFF)
    if(WIN32 AND _grpc_ssl_provider STREQUAL "module" AND NOT GRPC_ASYNC_STREAM_GRPC_UNSECURE)
        find_program(GRPC_ASYNC_STREAM_NASM NAMES nasm)
        if(NOT GRPC_ASYNC_STREAM_NASM)
            set(_grpc_openssl_no_asm ON)
            message(STATUS "GrpcAsyncStream: NASM not found, building BoringSSL without assembly (OPENSSL_NO_ASM)")
        endif()
    endif()

    # CMake 4 removed compatibility with projects requiring CMake < 3.5, and
    # gRPC's bundled c-ares declares cmake_minimum_required(VERSION 3.1.0), which
    # CMake 4 rejects ("Compatibility with CMake < 3.5 has been removed"). Raise
    # the policy floor for the gRPC subtree only; CMake 3.x ignores the variable.
    block(SCOPE_FOR VARIABLES PROPAGATE gRPC_ADDED gRPC_SOURCE_DIR gRPC_BINARY_DIR)
        set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
        CPMAddPackage(
            NAME gRPC
            GITHUB_REPOSITORY grpc/grpc
            GIT_TAG v${GRPC_ASYNC_STREAM_GRPC_VERSION}
            GIT_SHALLOW TRUE
            # Only the submodules needed for building the C++ library and the code generator
            GIT_SUBMODULES ${_grpc_submodules}
            SYSTEM TRUE
            EXCLUDE_FROM_ALL TRUE
            OPTIONS
                "gRPC_BUILD_TESTS OFF"
                "gRPC_BUILD_CODEGEN ON"
                "gRPC_BUILD_GRPC_CPP_PLUGIN ON"
                "gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF"
                "gRPC_BUILD_GRPC_NODE_PLUGIN OFF"
                "gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF"
                "gRPC_BUILD_GRPC_PHP_PLUGIN OFF"
                "gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF"
                "gRPC_BUILD_GRPC_RUBY_PLUGIN OFF"
                "gRPC_BUILD_CSHARP_EXT OFF"
                "gRPC_DOWNLOAD_ARCHIVES OFF"
                "gRPC_INSTALL OFF"
                "gRPC_SSL_PROVIDER ${_grpc_ssl_provider}"
                "gRPC_ZLIB_PROVIDER ${_grpc_zlib_provider}"
                "protobuf_BUILD_TESTS OFF"
                "protobuf_INSTALL OFF"
                "protobuf_BUILD_CONFORMANCE OFF"
                "protobuf_BUILD_EXAMPLES OFF"
                "utf8_range_ENABLE_TESTS OFF"
                "utf8_range_ENABLE_INSTALL OFF"
                "ABSL_PROPAGATE_CXX_STD ON"
                "ABSL_ENABLE_INSTALL OFF"
                "ABSL_BUILD_TESTING OFF"
                "RE2_BUILD_TESTING OFF"
                "CARES_BUILD_TOOLS OFF"
                "CARES_INSTALL OFF"
                "OPENSSL_NO_ASM ${_grpc_openssl_no_asm}"
                # MSVC: every library links the same (dynamic) C runtime as this project
                "gRPC_MSVC_STATIC_RUNTIME OFF"
                "protobuf_MSVC_STATIC_RUNTIME OFF"
                "ABSL_MSVC_STATIC_RUNTIME OFF"
                ${_grpc_extra_options}
        )
    endblock()

    # Normalize target names to the ones exported by an installed gRPC package
    if(NOT TARGET gRPC::grpc++)
        if(GRPC_ASYNC_STREAM_GRPC_UNSECURE)
            add_library(gRPC::grpc++ ALIAS grpc++_unsecure)
        else()
            add_library(gRPC::grpc++ ALIAS grpc++)
        endif()
    endif()
    if(NOT TARGET protobuf::libprotobuf)
        add_library(protobuf::libprotobuf ALIAS libprotobuf)
    endif()
endif()

# Locations of the code generators used for the test protocol: imported or built
# targets first, then programs on PATH (e.g. Conan tool_requires in cross builds,
# which the build environment (conanbuild) puts on PATH)
if(TARGET protobuf::protoc)
    set(GrpcAsyncStream_protoc $<TARGET_FILE:protobuf::protoc>)
    set(GrpcAsyncStream_protoc_target protobuf::protoc)
elseif(TARGET protoc)
    set(GrpcAsyncStream_protoc $<TARGET_FILE:protoc>)
    set(GrpcAsyncStream_protoc_target protoc)
else()
    find_program(GRPC_ASYNC_STREAM_PROTOC_PROGRAM NAMES protoc)
    if(GRPC_ASYNC_STREAM_PROTOC_PROGRAM)
        set(GrpcAsyncStream_protoc "${GRPC_ASYNC_STREAM_PROTOC_PROGRAM}")
    endif()
endif()

if(TARGET gRPC::grpc_cpp_plugin)
    set(GrpcAsyncStream_grpc_cpp_plugin $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
    set(GrpcAsyncStream_grpc_cpp_plugin_target gRPC::grpc_cpp_plugin)
elseif(TARGET grpc_cpp_plugin)
    set(GrpcAsyncStream_grpc_cpp_plugin $<TARGET_FILE:grpc_cpp_plugin>)
    set(GrpcAsyncStream_grpc_cpp_plugin_target grpc_cpp_plugin)
else()
    find_program(GRPC_ASYNC_STREAM_GRPC_CPP_PLUGIN_PROGRAM NAMES grpc_cpp_plugin)
    if(GRPC_ASYNC_STREAM_GRPC_CPP_PLUGIN_PROGRAM)
        set(GrpcAsyncStream_grpc_cpp_plugin "${GRPC_ASYNC_STREAM_GRPC_CPP_PLUGIN_PROGRAM}")
    endif()
endif()

# Catch2 (v3): this project's tests and benchmarks. Afterwards Catch2::Catch2WithMain
# exists and, when a Catch.cmake module was found, catch_discover_tests() is available
# (GRPC_ASYNC_STREAM_CATCH2_DISCOVERY is ON).
function(grpc_async_stream_add_catch2)
    set(_catch2_source "")
    # 1. Already imported (e.g. Conan's CMakeDeps)
    if(TARGET Catch2::Catch2WithMain)
        set(_catch2_source "imported")
    else()
        # 2. Installed package (Conan-generated or installed Catch2Config.cmake)
        find_package(Catch2 3 CONFIG QUIET)
        if(TARGET Catch2::Catch2WithMain)
            set(_catch2_source "package")
        else()
            # 3. Fetch with CPM
            CPMAddPackage(
                NAME Catch2
                GITHUB_REPOSITORY catchorg/Catch2
                VERSION ${GRPC_ASYNC_STREAM_CATCH2_VERSION}
                GIT_SHALLOW TRUE
                SYSTEM TRUE
                EXCLUDE_FROM_ALL TRUE
                OPTIONS
                    "CATCH_INSTALL_DOCS OFF"
                    "CATCH_INSTALL_EXTRAS OFF"
            )
            set(_catch2_source "fetched")
        endif()
    endif()

    # Catch.cmake (catch_discover_tests) ships next to Catch2's package config or in
    # its source tree; Conan's toolchain puts the recipe's build dirs on CMAKE_MODULE_PATH.
    find_file(GRPC_ASYNC_STREAM_CATCH_MODULE Catch.cmake
        PATHS ${CMAKE_MODULE_PATH} "${Catch2_DIR}" "${Catch2_SOURCE_DIR}/extras"
        NO_DEFAULT_PATH NO_CACHE)
    if(GRPC_ASYNC_STREAM_CATCH_MODULE)
        get_filename_component(_catch_module_dir "${GRPC_ASYNC_STREAM_CATCH_MODULE}" DIRECTORY)
        list(APPEND CMAKE_MODULE_PATH "${_catch_module_dir}")
        set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH}" PARENT_SCOPE)
        set(GRPC_ASYNC_STREAM_CATCH2_DISCOVERY ON PARENT_SCOPE)
    else()
        set(GRPC_ASYNC_STREAM_CATCH2_DISCOVERY OFF PARENT_SCOPE)
        message(STATUS "GrpcAsyncStream: Catch.cmake not found; tests are registered with CTest as one test")
    endif()
    if(Catch2_VERSION)
        set(_catch2_version_text " ${Catch2_VERSION}")
    endif()
    message(STATUS "GrpcAsyncStream: using ${_catch2_source} Catch2${_catch2_version_text}")
endfunction()

# magic_enum: enum names, parsing, value counts and lists in the test utilities
function(grpc_async_stream_add_magic_enum)
    # 1. Already imported (e.g. Conan's CMakeDeps)
    if(TARGET magic_enum::magic_enum)
        message(STATUS "GrpcAsyncStream: using imported magic_enum")
        return()
    endif()
    # 2. Installed package (Conan-generated or installed magic_enumConfig.cmake).
    #    Any version: the test utilities only use long-standing magic_enum API.
    find_package(magic_enum CONFIG QUIET)
    if(TARGET magic_enum::magic_enum)
        message(STATUS "GrpcAsyncStream: using package magic_enum ${magic_enum_VERSION}")
        return()
    endif()
    # 3. Fetch with CPM
    # Installed with this project when the test utilities are installed, so that
    # the exported GrpcAsyncStream::TestUtils target can find it
    if(GRPC_ASYNC_STREAM_INSTALL AND GRPC_ASYNC_STREAM_BUILD_TEST_UTILS AND GRPC_ASYNC_STREAM_GRPC_FOUND_AS_PACKAGE)
        set(_install_magic_enum ON)
        set(_exclude_magic_enum FALSE)
    else()
        set(_install_magic_enum OFF)
        set(_exclude_magic_enum TRUE)
    endif()
    CPMAddPackage(
        NAME magic_enum
        GITHUB_REPOSITORY Neargye/magic_enum
        VERSION ${GRPC_ASYNC_STREAM_MAGIC_ENUM_VERSION}
        GIT_SHALLOW TRUE
        SYSTEM TRUE
        EXCLUDE_FROM_ALL ${_exclude_magic_enum}
        OPTIONS
            "MAGIC_ENUM_OPT_BUILD_EXAMPLES OFF"
            "MAGIC_ENUM_OPT_BUILD_TESTS OFF"
            "MAGIC_ENUM_OPT_INSTALL ${_install_magic_enum}"
    )
    if(NOT TARGET magic_enum::magic_enum)
        message(FATAL_ERROR "GrpcAsyncStream: magic_enum::magic_enum target is not available")
    endif()
    message(STATUS "GrpcAsyncStream: using fetched magic_enum ${GRPC_ASYNC_STREAM_MAGIC_ENUM_VERSION}")
endfunction()
