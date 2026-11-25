// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

#pragma once

// TestStream service (tests/proto/test_stream.proto) of this repository's tests and benchmarks,
// served by an InProcessServer. Reusable utilities live in tests/include/grpc_async_stream_test.

#include "test_stream.grpc.pb.h"

#include <grpc_async_stream_test/InProcessServer.h>

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

using Request       = proto::TestRequest;
using Response      = proto::TestResponse;
using UploadSummary = proto::UploadSummary;

class TestStreamService final : public proto::TestStream::Service
{
public:
    grpc::Status Chat(grpc::ServerContext* context, grpc::ServerReaderWriter<Response, Request>* stream) override;
    grpc::Status Upload(grpc::ServerContext* context, grpc::ServerReader<Request>* reader,
                        UploadSummary* summary) override;
    grpc::Status Subscribe(grpc::ServerContext* context, const Request* request,
                           grpc::ServerWriter<Response>* writer) override;

    // Delay before reading each request: a slow consumer, so flow control pushes back
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

// TestStreamService running in an in-process server
class TestServer
{
public:
    TestServer() = default;

    TestServer(const TestServer&)            = delete;
    TestServer& operator=(const TestServer&) = delete;

    [[nodiscard]] TestStreamService& service() { return m_service; }

    // Channel connected to this server in-process
    [[nodiscard]] std::shared_ptr<grpc::Channel> channel() const { return m_server.channel(); }

    // Fresh stub over the in-process channel
    [[nodiscard]] std::unique_ptr<proto::TestStream::Stub> makeStub() const
    {
        return m_server.makeStub<proto::TestStream>();
    }

private:
    TestStreamService                       m_service; // must outlive the server
    grpc_async_stream_test::InProcessServer m_server{m_service};
};

// Helpers creating requests
Request makeEcho(std::uint64_t id, std::string payload = {});
Request makeBurst(std::uint64_t id, std::uint32_t count, std::string payload = {});
Request makeFail(grpc::StatusCode code, std::string message);
Request makeEnd();

// Metadata key echoed back by the server in TestResponse.metadata_value
inline constexpr std::string_view metadata_key = "x-test-metadata";

} // namespace TestSupport
