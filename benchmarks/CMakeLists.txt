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

grpc_async_stream_configure_test_target(GrpcAsyncStreamBenchmarks)

# Benchmarks are registered with CTest under the "benchmark" label; exclude them
# from regular test runs with `ctest -LE benchmark`, run only them with `ctest -L benchmark`.
if(BUILD_TESTING)
    add_test(NAME GrpcAsyncStream.Benchmarks
        COMMAND GrpcAsyncStreamBenchmarks "[benchmark]" --benchmark-samples 20)
    set_tests_properties(GrpcAsyncStream.Benchmarks PROPERTIES
        LABELS benchmark
        TIMEOUT 1800)
endif()
