#pragma once

// In-process gRPC server implementing the TestStream service used by the
// integration tests and the benchmarks. Clients connect through an in-process
// channel, so no network ports are involved.

#include "test_stream.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace TestSupport
{

namespace proto = ::grpc_async_stream::test;

using Request  = proto::TestRequest;
using Response = proto::TestResponse;

class TestStreamService final : public proto::TestStream::Service
{
public:
    grpc::Status Chat(grpc::ServerContext* context, grpc::ServerReaderWriter<Response, Request>* stream) override;

    // Delay applied before reading every request: simulates a slow consumer,
    // which makes gRPC flow control push back on the client writes.
    void setReadDelay(std::chrono::microseconds delay) { m_read_delay_us = delay.count(); }

    // Number of requests received / responses sent over all calls
    [[nodiscard]] std::uint64_t receivedRequests() const { return m_received_requests; }
    [[nodiscard]] std::uint64_t sentResponses() const { return m_sent_responses; }

    // Calls that ended because the client cancelled them
    [[nodiscard]] std::uint64_t cancelledCalls() const { return m_cancelled_calls; }
    [[nodiscard]] std::uint64_t finishedCalls() const { return m_finished_calls; }
    [[nodiscard]] std::uint64_t activeCalls() const { return m_active_calls; }

private:
    std::atomic<std::int64_t>  m_read_delay_us{0};
    std::atomic<std::uint64_t> m_received_requests{0};
    std::atomic<std::uint64_t> m_sent_responses{0};
    std::atomic<std::uint64_t> m_cancelled_calls{0};
    std::atomic<std::uint64_t> m_finished_calls{0};
    std::atomic<std::uint64_t> m_active_calls{0};
};

class TestServer
{
public:
    TestServer();
    ~TestServer();

    TestServer(const TestServer&)            = delete;
    TestServer& operator=(const TestServer&) = delete;

    [[nodiscard]] TestStreamService& service() { return m_service; }

    // Channel connected to this server in-process
    [[nodiscard]] std::shared_ptr<grpc::Channel> channel() const { return m_channel; }

    // Fresh stub over the in-process channel
    [[nodiscard]] std::unique_ptr<proto::TestStream::Stub> makeStub() const;

private:
    TestStreamService              m_service;
    std::unique_ptr<grpc::Server>  m_server;
    std::shared_ptr<grpc::Channel> m_channel;
};

// Helpers creating requests
Request makeEcho(std::uint64_t id, std::string payload = {});
Request makeBurst(std::uint64_t id, std::uint32_t count, std::string payload = {});
Request makeFail(grpc::StatusCode code, std::string message);
Request makeEnd();

// Metadata key echoed back by the server in TestResponse.metadata_value
inline constexpr std::string_view metadata_key = "x-test-metadata";

// A server address that refuses connections on every platform
#if defined(_WIN32)
inline constexpr std::string_view unreachable_server_address = "ipv4:127.0.0.1:1";
#else
inline constexpr std::string_view unreachable_server_address = "unix:/nonexistent/grpc-async-stream.sock";
#endif

} // namespace TestSupport
