add_executable(GrpcAsyncStreamTests
    StreamLifecycleTests.cpp
    WriteQueueTests.cpp
    IntegrationTests.cpp
    TestUtilsUsageTests.cpp)

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

# Every public header must compile on its own (self-contained includes): one
# translation unit per header, built with the project's warning flags.
set(_header_check_sources)
foreach(header IN ITEMS include/grpc_utils/AsyncReadWriteStream.h ${GRPC_ASYNC_STREAM_TEST_UTILS_HEADERS})
    string(MAKE_C_IDENTIFIER "${header}" header_id)
    file(RELATIVE_PATH include_path "${PROJECT_SOURCE_DIR}/include" "${PROJECT_SOURCE_DIR}/${header}")
    set(check_source "${CMAKE_CURRENT_BINARY_DIR}/header_checks/${header_id}.cpp")
    file(CONFIGURE OUTPUT "${check_source}" CONTENT "#include <${include_path}>\n#include <${include_path}> // include guard\n")
    list(APPEND _header_check_sources "${check_source}")
endforeach()

add_library(GrpcAsyncStreamHeaderChecks OBJECT ${_header_check_sources})
target_link_libraries(GrpcAsyncStreamHeaderChecks PRIVATE
    GrpcAsyncStream::TestUtils
    Catch2::Catch2) # for grpc_test_utils/Catch2StringMakers.h
grpc_async_stream_configure_test_target(GrpcAsyncStreamHeaderChecks)
