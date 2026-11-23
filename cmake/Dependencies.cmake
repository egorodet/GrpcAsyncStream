# Dependency resolution for GrpcAsyncStream.
#
# gRPC (required by the header itself):
#   GRPC_ASYNC_STREAM_GRPC_PROVIDER = auto    - use an installed gRPC (find_package CONFIG),
#                                               fall back to building it from source via CPM
#                                     package - require an installed gRPC
#                                     fetch   - always build gRPC from source via CPM
#
# Catch2 and magic_enum are used by the tests and benchmarks only; the library
# header does not depend on them. Catch2 is fetched by CPM (which honours an
# available package when CPM_USE_LOCAL_PACKAGES is ON); magic_enum is taken from
# an installed package when one is found, otherwise fetched by CPM.
#
# After this file the following normalized targets exist:
#   gRPC::grpc++            - gRPC C++ library
#   protobuf::libprotobuf   - protobuf runtime
#   GrpcAsyncStream_protoc  - path of the protoc executable        (variable)
#   GrpcAsyncStream_grpc_cpp_plugin - path of grpc_cpp_plugin      (variable, generator expression)

include_guard(GLOBAL)

include(${CMAKE_CURRENT_LIST_DIR}/CPM.cmake)

# ---------------------------------------------------------------------------
# gRPC
# ---------------------------------------------------------------------------

set(GRPC_ASYNC_STREAM_GRPC_FOUND_AS_PACKAGE OFF)

if(NOT GRPC_ASYNC_STREAM_GRPC_PROVIDER STREQUAL "fetch")
    find_package(gRPC CONFIG QUIET)
    if(gRPC_FOUND)
        # gRPCConfig pulls in protobuf, but make sure protoc is reachable as a target
        if(NOT TARGET protobuf::protoc)
            find_package(Protobuf CONFIG QUIET)
            if(NOT Protobuf_FOUND)
                find_package(Protobuf REQUIRED)
            endif()
        endif()
        set(GRPC_ASYNC_STREAM_GRPC_FOUND_AS_PACKAGE ON)
        message(STATUS "GrpcAsyncStream: using installed gRPC ${gRPC_VERSION}")
    elseif(GRPC_ASYNC_STREAM_GRPC_PROVIDER STREQUAL "package")
        message(FATAL_ERROR
            "GrpcAsyncStream: gRPC package not found. Set CMAKE_PREFIX_PATH to the gRPC install "
            "prefix or configure with -DGRPC_ASYNC_STREAM_GRPC_PROVIDER=fetch")
    endif()
endif()

if(NOT GRPC_ASYNC_STREAM_GRPC_FOUND_AS_PACKAGE)
    message(STATUS "GrpcAsyncStream: building gRPC v${GRPC_ASYNC_STREAM_GRPC_VERSION} from source "
                   "(this takes a while on the first build; set CPM_SOURCE_CACHE to reuse the sources)")

    # Prefer system OpenSSL / zlib when available: it saves building BoringSSL and zlib
    find_package(OpenSSL QUIET)
    find_package(ZLIB QUIET)
    if(OpenSSL_FOUND)
        set(_grpc_ssl_provider package)
    else()
        set(_grpc_ssl_provider module)
    endif()
    if(ZLIB_FOUND)
        set(_grpc_zlib_provider package)
    else()
        set(_grpc_zlib_provider module)
    endif()

    # Bundled BoringSSL needs NASM for its assembly on Windows; without NASM use
    # the portable C implementation instead of failing the build
    set(_grpc_openssl_no_asm OFF)
    if(WIN32 AND _grpc_ssl_provider STREQUAL "module")
        find_program(GRPC_ASYNC_STREAM_NASM NAMES nasm)
        if(NOT GRPC_ASYNC_STREAM_NASM)
            set(_grpc_openssl_no_asm ON)
            message(STATUS "GrpcAsyncStream: NASM not found, building BoringSSL without assembly (OPENSSL_NO_ASM)")
        endif()
    endif()

    CPMAddPackage(
        NAME gRPC
        GITHUB_REPOSITORY grpc/grpc
        GIT_TAG v${GRPC_ASYNC_STREAM_GRPC_VERSION}
        GIT_SHALLOW TRUE
        # Only the submodules needed for building the C++ library and the code generator
        GIT_SUBMODULES
            "third_party/abseil-cpp"
            "third_party/protobuf"
            "third_party/re2"
            "third_party/cares/cares"
            "third_party/zlib"
            "third_party/boringssl-with-bazel"
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
    )

    # Normalize target names to the ones exported by an installed gRPC package
    if(NOT TARGET gRPC::grpc++)
        add_library(gRPC::grpc++ ALIAS grpc++)
    endif()
    if(NOT TARGET protobuf::libprotobuf)
        add_library(protobuf::libprotobuf ALIAS libprotobuf)
    endif()
endif()

# Locations of the code generators used for the test protocol
if(TARGET protobuf::protoc)
    set(GrpcAsyncStream_protoc $<TARGET_FILE:protobuf::protoc>)
    set(GrpcAsyncStream_protoc_target protobuf::protoc)
elseif(TARGET protoc)
    set(GrpcAsyncStream_protoc $<TARGET_FILE:protoc>)
    set(GrpcAsyncStream_protoc_target protoc)
endif()

if(TARGET gRPC::grpc_cpp_plugin)
    set(GrpcAsyncStream_grpc_cpp_plugin $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
    set(GrpcAsyncStream_grpc_cpp_plugin_target gRPC::grpc_cpp_plugin)
elseif(TARGET grpc_cpp_plugin)
    set(GrpcAsyncStream_grpc_cpp_plugin $<TARGET_FILE:grpc_cpp_plugin>)
    set(GrpcAsyncStream_grpc_cpp_plugin_target grpc_cpp_plugin)
endif()

function(grpc_async_stream_add_catch2)
    if(TARGET Catch2::Catch2WithMain)
        return()
    endif()
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
    # Make catch_discover_tests() available
    list(APPEND CMAKE_MODULE_PATH "${Catch2_SOURCE_DIR}/extras")
    set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH}" PARENT_SCOPE)
endfunction()

# magic_enum: enum names, parsing, value counts and lists in tests and benchmarks
function(grpc_async_stream_add_magic_enum)
    if(TARGET magic_enum::magic_enum)
        return()
    endif()
    CPMFindPackage(
        NAME magic_enum
        GITHUB_REPOSITORY Neargye/magic_enum
        VERSION ${GRPC_ASYNC_STREAM_MAGIC_ENUM_VERSION}
        GIT_SHALLOW TRUE
        SYSTEM TRUE
        EXCLUDE_FROM_ALL TRUE
        OPTIONS
            "MAGIC_ENUM_OPT_BUILD_EXAMPLES OFF"
            "MAGIC_ENUM_OPT_BUILD_TESTS OFF"
            "MAGIC_ENUM_OPT_INSTALL OFF"
    )
    if(NOT TARGET magic_enum::magic_enum)
        message(FATAL_ERROR "GrpcAsyncStream: magic_enum::magic_enum target is not available")
    endif()
endfunction()
