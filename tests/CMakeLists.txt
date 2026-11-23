add_executable(GrpcAsyncStreamTests
    StreamLifecycleTests.cpp
    WriteQueueTests.cpp
    IntegrationTests.cpp)

target_link_libraries(GrpcAsyncStreamTests PRIVATE
    GrpcAsyncStreamTestSupport
    Catch2::Catch2WithMain)

grpc_async_stream_configure_test_target(GrpcAsyncStreamTests)

set(_test_properties TIMEOUT 120)
if(GRPC_ASYNC_STREAM_TEST_ENVIRONMENT)
    list(APPEND _test_properties ENVIRONMENT "${GRPC_ASYNC_STREAM_TEST_ENVIRONMENT}")
endif()

catch_discover_tests(GrpcAsyncStreamTests
    TEST_PREFIX "GrpcAsyncStream."
    PROPERTIES ${_test_properties}
    DISCOVERY_MODE PRE_TEST)
