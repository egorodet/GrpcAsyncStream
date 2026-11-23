add_executable(GrpcAsyncStreamTests
    StreamLifecycleTests.cpp
    WriteQueueTests.cpp
    IntegrationTests.cpp)

target_link_libraries(GrpcAsyncStreamTests PRIVATE
    GrpcAsyncStreamTestSupport
    Catch2::Catch2WithMain)

grpc_async_stream_configure_test_target(GrpcAsyncStreamTests)

catch_discover_tests(GrpcAsyncStreamTests
    TEST_PREFIX "GrpcAsyncStream."
    PROPERTIES TIMEOUT 120
    DISCOVERY_MODE PRE_TEST)
