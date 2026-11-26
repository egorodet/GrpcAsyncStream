// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

// Tests of the test utilities themselves: FakeCall detecting every gRPC API contract violation,
// enum names of invalid values, Catch2 string makers and InProcessServer start failures.

#include "support/TestServer.h"

#include <grpc_async_stream_test/Catch2StringMakers.h>
#include <grpc_async_stream_test/EnumReflection.h>
#include <grpc_async_stream_test/FakeReaderWriter.h>
#include <grpc_async_stream_test/InProcessServer.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace
{

using namespace TestSupport;
using namespace grpc_async_stream_test;
using grpc_async_stream::SendResult;
using grpc_async_stream::StreamState;
using grpc_async_stream::WriteOverflowPolicy;

using Call = FakeCall<Request, Response>;

Response makeResponse(std::uint64_t id)
{
    Response response;
    response.set_id(id);
    return response;
}

// A FakeCall driven directly, as a fake stream would, with its own completion queue
struct DirectCallFixture
{
    grpc::CompletionQueue queue;
    Call                  call;
    Request               request = makeEcho(1);
    Response              read_buffer;
    grpc::Status          status;

    // Distinct tags, one per operation
    int start_tag       = 0;
    int read_tag        = 0;
    int write_tag       = 0;
    int writes_done_tag = 0;
    int finish_tag      = 0;

    DirectCallFixture()                                    = default;
    DirectCallFixture(const DirectCallFixture&)            = delete;
    DirectCallFixture& operator=(const DirectCallFixture&) = delete;

    // The alarms of the call post to the queue: release them before draining it
    ~DirectCallFixture()
    {
        call.onDestroyed();
        queue.Shutdown();
        void* tag = nullptr;
        bool  ok  = false;
        while (queue.Next(&tag, &ok))
        {
        }
    }

    // Starts the call and waits until the start-call completion is delivered
    void start()
    {
        call.onStartCall(&queue, &start_tag);
        call.completeStart();
        CHECK(next() == std::pair<void*, bool>(&start_tag, true));
        CHECK_FALSE(call.startPending());
    }

    std::pair<void*, bool> next()
    {
        void* tag = nullptr;
        bool  ok  = false;
        REQUIRE(queue.AsyncNext(&tag, &ok, std::chrono::system_clock::now() + std::chrono::seconds(5)) ==
                grpc::CompletionQueue::GOT_EVENT);
        return {tag, ok};
    }

    [[nodiscard]] std::size_t violationCount(std::string_view violation) const
    {
        return static_cast<std::size_t>(std::ranges::count(call.violations(), violation));
    }
};

} // namespace

TEST_CASE("FakeCall reports completions of operations that are not pending", "[test-utils]")
{
    DirectCallFixture f;
    constexpr std::string_view not_pending = "test tried to complete an operation that is not pending";

    SECTION("Before the call is started")
    {
        f.call.completeStart();
        f.call.endReads();
        f.call.completeWrite();
        f.call.completeWritesDone();
        f.call.completeFinish(grpc::Status::OK);
        CHECK(f.violationCount(not_pending) == 5);
        CHECK(f.call.violations().size() == 5);
    }

    SECTION("Without a completion queue")
    {
        f.call.onStartCall(nullptr, &f.start_tag);
        f.call.completeStart();
        CHECK(f.violationCount(not_pending) == 1);
        CHECK(f.call.startPending()); // still pending: nothing was posted
    }

    SECTION("A message delivered without an outstanding Read")
    {
        f.start();
        f.call.deliver(makeResponse(1));
        CHECK(f.call.violations() == std::vector<std::string>{"deliver() without an outstanding Read"});
    }

    SECTION("A response for a call without a response message")
    {
        f.start();
        f.call.onFinish(&f.status, &f.finish_tag);
        f.call.completeFinish(grpc::Status(grpc::StatusCode::ABORTED, "aborted"), makeResponse(1));
        CHECK(f.call.violations() ==
              std::vector<std::string>{
                  "completeFinish() with a response for a call without a response message"});
        CHECK(f.next() == std::pair<void*, bool>(&f.finish_tag, true));
        CHECK(f.status.error_code() == grpc::StatusCode::ABORTED); // the status is still delivered
    }
}

TEST_CASE("FakeCall records every gRPC API contract violation", "[test-utils]")
{
    DirectCallFixture f;

    SECTION("Writes before the start-call completes")
    {
        f.call.onStartCall(&f.queue, &f.start_tag);
        f.call.onWrite(f.request, &f.write_tag);
        f.call.onWritesDone(&f.writes_done_tag);
        CHECK(f.call.violations() == std::vector<std::string>{
                                         "Write before the start-call completed",
                                         "WritesDone before the start-call completed",
                                         "WritesDone while another write operation is outstanding",
                                     });
    }

    SECTION("Reads and Finish with outstanding operations")
    {
        f.start();
        f.call.onWrite(f.request, &f.write_tag);
        f.call.onRead(&f.read_buffer, &f.read_tag);
        CHECK(f.call.violations().empty());

        f.call.onRead(&f.read_buffer, &f.read_tag);
        f.call.onFinish(&f.status, &f.finish_tag);
        f.call.onRead(&f.read_buffer, &f.read_tag);
        CHECK(f.call.violations() == std::vector<std::string>{
                                         "Read while another Read is outstanding",
                                         "Finish while a Read is outstanding",
                                         "Finish while a write operation is outstanding",
                                         "Read while another Read is outstanding",
                                         "Read after Finish",
                                     });
        CHECK(f.call.finishCalls() == 1);
    }

    SECTION("Writes after the half-close and after Finish")
    {
        f.start();
        f.call.onWritesDone(&f.writes_done_tag);
        CHECK(f.call.violations().empty());

        f.call.onWritesDone(&f.writes_done_tag);
        f.call.onFinish(&f.status, &f.finish_tag);
        f.call.onWrite(f.request, &f.write_tag);
        f.call.onFinish(&f.status, &f.finish_tag);
        CHECK(f.call.violations() == std::vector<std::string>{
                                         "WritesDone while another write operation is outstanding",
                                         "WritesDone called twice",
                                         "Finish while a write operation is outstanding",
                                         "Write while another write operation is outstanding",
                                         "Write after WritesDone",
                                         "Write after Finish",
                                         "Finish called more than once",
                                         "Finish while a write operation is outstanding",
                                     });
        CHECK(f.call.writes().size() == 1);
        CHECK(f.call.finishCalls() == 2);
    }

    SECTION("Completions are delivered with their tag and result")
    {
        f.start();
        f.call.onRead(&f.read_buffer, &f.read_tag);
        f.call.deliver(makeResponse(7));
        CHECK(f.next() == std::pair<void*, bool>(&f.read_tag, true));
        CHECK(f.read_buffer.id() == 7);

        f.call.onWrite(f.request, &f.write_tag);
        f.call.completeWrite(false);
        CHECK(f.next() == std::pair<void*, bool>(&f.write_tag, false));
        CHECK(f.call.violations().empty());
    }
}

TEST_CASE("Enum names and indexes handle values that are not enumerators", "[test-utils]")
{
    CHECK(enumName(SendResult::QueuedDroppedOldest) == "QueuedDroppedOldest");
    CHECK(enumName(WriteOverflowPolicy::DropOldest) == "DropOldest");
    CHECK(enumName(StreamState::HalfClosed) == "HalfClosed");
    CHECK(enumName(static_cast<SendResult>(42)) == "<invalid enumerator>");
    CHECK(enumName(static_cast<WriteOverflowPolicy>(42)) == "<invalid enumerator>");
    CHECK(enumName(static_cast<StreamState>(42)) == "<invalid enumerator>");

    CHECK(enumIndex(WriteOverflowPolicy::DropOldest) == 1);
    CHECK_THROWS_AS(enumIndex(static_cast<SendResult>(42)), std::invalid_argument);
    CHECK_THROWS_AS(enumIndex(static_cast<StreamState>(42)), std::invalid_argument);

    CHECK(statusCodeName(grpc::StatusCode::OK) == "OK (0)");
    CHECK(statusCodeName(grpc::StatusCode::DEADLINE_EXCEEDED) == "DEADLINE_EXCEEDED (4)");
    CHECK(statusCodeName(static_cast<grpc::StatusCode>(20)) == "StatusCode (20)");
}

TEST_CASE("Catch2 prints the library enums and status codes by name", "[test-utils]")
{
    CHECK(Catch::Detail::stringify(SendResult::Dropped) == "Dropped");
    CHECK(Catch::Detail::stringify(WriteOverflowPolicy::DropNewest) == "DropNewest");
    CHECK(Catch::Detail::stringify(StreamState::Finishing) == "Finishing");
    CHECK(Catch::Detail::stringify(static_cast<StreamState>(42)) == "<invalid enumerator>");
    CHECK(Catch::Detail::stringify(grpc::StatusCode::UNAVAILABLE) == "UNAVAILABLE (14)");
    CHECK(Catch::Detail::stringify(static_cast<grpc::StatusCode>(20)) == "StatusCode (20)");
}

TEST_CASE("InProcessServer throws when the server cannot start", "[test-utils]")
{
    TestStreamService service;
    auto              add_invalid_port = [](grpc::ServerBuilder& builder) {
        // An unterminated IPv6 literal: gRPC rejects it itself, without a name lookup
        builder.AddListeningPort("[::1", grpc::InsecureServerCredentials());
    };
    CHECK_THROWS_AS(InProcessServer(service, add_invalid_port), std::runtime_error);
}
