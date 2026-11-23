add_executable(GrpcAsyncStreamBenchmarks
    AsyncStreamBenchmarks.cpp
    MemoryTracker.cpp
    MemoryTracker.h)

target_link_libraries(GrpcAsyncStreamBenchmarks PRIVATE
    GrpcAsyncStreamTestSupport
    Catch2::Catch2WithMain)

if(WIN32)
    target_link_libraries(GrpcAsyncStreamBenchmarks PRIVATE psapi)
endif()

# 1: MemoryTracker.cpp replaces the global operator new/delete to measure the C++ heap.
# 0: heap figures are reported as unavailable and only the process RSS is measured
#    (sanitizer builds, or GRPC_ASYNC_STREAM_HEAP_TRACKING=OFF).
if(GRPC_ASYNC_STREAM_HEAP_TRACKING_ACTIVE)
    target_compile_definitions(GrpcAsyncStreamBenchmarks PRIVATE GRPC_ASYNC_STREAM_HEAP_TRACKING=1)
else()
    target_compile_definitions(GrpcAsyncStreamBenchmarks PRIVATE GRPC_ASYNC_STREAM_HEAP_TRACKING=0)
endif()

grpc_async_stream_configure_test_target(GrpcAsyncStreamBenchmarks)

# Benchmarks are registered with CTest under the "benchmark" label; exclude them
# from regular test runs with `ctest -LE benchmark`, run only them with `ctest -L benchmark`.
if(BUILD_TESTING)
    add_test(NAME GrpcAsyncStream.Benchmarks
        COMMAND GrpcAsyncStreamBenchmarks "[benchmark]" --benchmark-samples 20)
    set_tests_properties(GrpcAsyncStream.Benchmarks PROPERTIES
        LABELS benchmark
        TIMEOUT 1800)
    if(GRPC_ASYNC_STREAM_TEST_ENVIRONMENT)
        set_tests_properties(GrpcAsyncStream.Benchmarks PROPERTIES
            ENVIRONMENT "${GRPC_ASYNC_STREAM_TEST_ENVIRONMENT}")
    endif()
endif()
