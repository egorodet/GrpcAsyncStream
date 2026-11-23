# Compiler and platform configuration for the GrpcAsyncStream tests and benchmarks.
#
# Supported toolchains: GCC and Clang on Linux, Apple Clang on macOS, MSVC (and
# clang-cl) on Windows. Memory checks are enabled only where the platform
# supports them:
#
#   Check                     Linux GCC/Clang   macOS Apple Clang     Windows MSVC        MinGW
#   AddressSanitizer          yes               yes                   yes (/fsanitize)    no
#   UndefinedBehaviorSanitizer yes              yes                   no                  no
#   LeakSanitizer             yes               no (not supported)    no                  no
#   Container-overflow checks yes               no (false positives)  no (link mismatch)  no
#   Heap tracking (benchmark) yes, unless a sanitizer is enabled (sanitizers replace the allocator)
#
# Only this project's code is instrumented; gRPC and its dependencies are not.
# Checks that require every linked object to be instrumented are therefore off:
#   - MSVC: the STL's vector/string/optional ASan annotations are disabled, otherwise the
#     linker rejects mixing instrumented and plain objects (LNK2038 annotate_*).
#   - macOS: libc++ container-overflow detection reports false positives when
#     plain (gRPC) code modifies containers created by instrumented code.
#   - GCC/Clang: GRPC_ASAN_SUPPRESSED is defined. gRPC's public headers change
#     struct layouts when they detect ASan: gpr_mu (pthread builds) gets an extra
#     `int* leak_checker` member. On macOS grpc::internal::Mutex wraps gpr_mu, so
#     grpc::CompletionQueue, grpc::Server etc. would be 8 bytes larger in our code
#     than in the plain gRPC library (ODR violation -> heap-buffer-overflow in
#     CompletionQueue's inline constructor, SEGV in ~ServerContextBase).
#     Set GRPC_ASYNC_STREAM_GRPC_ASAN_INSTRUMENTED=ON if your gRPC itself is
#     built with -fsanitize=address, so the layouts match without the define.
#
# Results exported to the parent scope:
#   GRPC_ASYNC_STREAM_SANITIZERS_ACTIVE     - sanitizers are really enabled for this toolchain
#   GRPC_ASYNC_STREAM_HEAP_TRACKING_ACTIVE  - the benchmark replaces operator new/delete to track the heap
#   GRPC_ASYNC_STREAM_TEST_ENVIRONMENT      - environment for CTest (sanitizer runtime options)

include_guard(GLOBAL)

set(GRPC_ASYNC_STREAM_SANITIZE_COMPILE_OPTIONS)
set(GRPC_ASYNC_STREAM_SANITIZE_LINK_OPTIONS)
set(GRPC_ASYNC_STREAM_SANITIZE_DEFINITIONS)
set(GRPC_ASYNC_STREAM_SANITIZERS_ACTIVE OFF)
set(GRPC_ASYNC_STREAM_TEST_ENVIRONMENT)

option(GRPC_ASYNC_STREAM_GRPC_ASAN_INSTRUMENTED
       "The linked gRPC is itself built with AddressSanitizer (don't define GRPC_ASAN_SUPPRESSED)" OFF)

if(GRPC_ASYNC_STREAM_ENABLE_SANITIZERS)
    if(MSVC)
        # MSVC supports AddressSanitizer only. It is incompatible with the runtime
        # checks (/RTC*) and incremental linking that CMake enables for Debug.
        # ASan without debug info raises warning C5072 (an error with /WX) and gives
        # unreadable reports, so configurations without debug info get /Z7 (embedded
        # in the objects: no PDB contention in parallel builds) and /DEBUG.
        set(GRPC_ASYNC_STREAM_SANITIZE_COMPILE_OPTIONS /fsanitize=address $<$<CONFIG:Release,MinSizeRel>:/Z7>)
        set(GRPC_ASYNC_STREAM_SANITIZE_LINK_OPTIONS /INCREMENTAL:NO /DEBUG)
        set(GRPC_ASYNC_STREAM_SANITIZE_DEFINITIONS
            _DISABLE_VECTOR_ANNOTATION _DISABLE_STRING_ANNOTATION _DISABLE_OPTIONAL_ANNOTATION)
        foreach(flags_var CMAKE_CXX_FLAGS_DEBUG CMAKE_CXX_FLAGS_RELWITHDEBINFO CMAKE_C_FLAGS_DEBUG CMAKE_C_FLAGS_RELWITHDEBINFO)
            # Included at directory scope: this changes the flags of the targets defined after it
            string(REGEX REPLACE "/RTC[1csu]+" "" ${flags_var} "${${flags_var}}")
        endforeach()
        set(GRPC_ASYNC_STREAM_SANITIZERS_ACTIVE ON)
        message(STATUS "GrpcAsyncStream: AddressSanitizer enabled (UBSan and LeakSanitizer are not available with MSVC)")
    elseif(MINGW)
        message(WARNING "GrpcAsyncStream: sanitizers are not supported with MinGW, GRPC_ASYNC_STREAM_ENABLE_SANITIZERS ignored")
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        set(GRPC_ASYNC_STREAM_SANITIZE_COMPILE_OPTIONS
            -fsanitize=address,undefined -fno-sanitize-recover=undefined -fno-omit-frame-pointer)
        set(GRPC_ASYNC_STREAM_SANITIZE_LINK_OPTIONS -fsanitize=address,undefined)
        if(NOT GRPC_ASYNC_STREAM_GRPC_ASAN_INSTRUMENTED)
            # Keep gRPC header struct layouts identical to the non-instrumented gRPC library
            set(GRPC_ASYNC_STREAM_SANITIZE_DEFINITIONS GRPC_ASAN_SUPPRESSED)
        endif()
        set(GRPC_ASYNC_STREAM_SANITIZERS_ACTIVE ON)
        if(APPLE)
            # LeakSanitizer is not supported by the macOS (Apple Clang) runtime
            list(APPEND GRPC_ASYNC_STREAM_TEST_ENVIRONMENT "ASAN_OPTIONS=detect_leaks=0:detect_container_overflow=0")
            message(STATUS "GrpcAsyncStream: AddressSanitizer + UBSan enabled (LeakSanitizer is not supported on macOS)")
        else()
            list(APPEND GRPC_ASYNC_STREAM_TEST_ENVIRONMENT "ASAN_OPTIONS=detect_leaks=1")
            message(STATUS "GrpcAsyncStream: AddressSanitizer + UBSan + LeakSanitizer enabled")
        endif()
        list(APPEND GRPC_ASYNC_STREAM_TEST_ENVIRONMENT "UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1")
    else()
        message(WARNING "GrpcAsyncStream: sanitizers are not supported for ${CMAKE_CXX_COMPILER_ID}, option ignored")
    endif()
endif()

# The benchmark's heap tracking replaces the global operator new/delete and asks
# the C runtime for the usable size of each block. Sanitizers install their own
# allocator (and quarantine freed memory), so tracking is disabled with them.
if(GRPC_ASYNC_STREAM_HEAP_TRACKING AND NOT GRPC_ASYNC_STREAM_SANITIZERS_ACTIVE)
    set(GRPC_ASYNC_STREAM_HEAP_TRACKING_ACTIVE ON)
else()
    set(GRPC_ASYNC_STREAM_HEAP_TRACKING_ACTIVE OFF)
    if(GRPC_ASYNC_STREAM_HEAP_TRACKING AND GRPC_ASYNC_STREAM_BUILD_BENCHMARKS)
        message(STATUS "GrpcAsyncStream: benchmark heap tracking disabled because sanitizers are enabled")
    endif()
endif()

function(grpc_async_stream_configure_test_target target)
    target_compile_features(${target} PUBLIC cxx_std_20)

    if(MSVC)
        target_compile_options(${target} PRIVATE
            /permissive-          # standard conformance
            /Zc:__cplusplus       # report the real __cplusplus value
            /Zc:preprocessor      # conforming preprocessor (needed by some C++20 code)
            /utf-8                # sources and literals are UTF-8
            /bigobj)              # generated protobuf / Catch2 objects exceed the default section limit
        target_compile_definitions(${target} PRIVATE
            NOMINMAX
            WIN32_LEAN_AND_MEAN
            _WIN32_WINNT=0x0A00   # Windows 10, as required by gRPC
            _CRT_SECURE_NO_WARNINGS)
    elseif(MINGW)
        target_compile_definitions(${target} PRIVATE _WIN32_WINNT=0x0A00)
    endif()

    if(GRPC_ASYNC_STREAM_ENABLE_WARNINGS)
        if(MSVC)
            # Third-party headers (gRPC, protobuf, Catch2) are included as external/system headers
            target_compile_options(${target} PRIVATE /W4 /WX /external:anglebrackets /external:W0)
        else()
            target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Werror)
            if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
                # Clang's -Wconversion includes sign conversions, which protobuf's int-typed
                # accessors trigger in perfectly valid test code
                target_compile_options(${target} PRIVATE -Wno-sign-conversion)
            endif()
        endif()
    endif()

    if(GRPC_ASYNC_STREAM_SANITIZERS_ACTIVE)
        target_compile_options(${target} PRIVATE ${GRPC_ASYNC_STREAM_SANITIZE_COMPILE_OPTIONS})
        target_link_options(${target} PRIVATE ${GRPC_ASYNC_STREAM_SANITIZE_LINK_OPTIONS})
        if(GRPC_ASYNC_STREAM_SANITIZE_DEFINITIONS)
            target_compile_definitions(${target} PRIVATE ${GRPC_ASYNC_STREAM_SANITIZE_DEFINITIONS})
        endif()
    endif()
endfunction()
