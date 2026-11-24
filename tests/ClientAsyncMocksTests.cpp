// Tests of the extensible client stream mocks (grpc_async_stream_test/ClientAsyncMocks.h):
// the mocks used as they are and through derived mocks that change the default
// behaviour (start-call completed right away or failing, scripted reads, extra
// constructor arguments), driving AsyncReadWriteStream, AsyncWriteStream and
// AsyncReadStream.

#include "support/TestServer.h"

#include <grpc_async_stream_test/Catch2StringMakers.h>
#include <grpc_async_stream_test/ClientAsyncMocks.h>
#include <grpc_async_stream_test/StreamTestUtils.h>
#include <grpc_async_stream/AsyncReadStream.h>
#include <grpc_async_stream/AsyncReadWriteStream.h>
#include <grpc_async_stream/AsyncWriteStream.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace
{

using namespace TestSupport;
using namespace grpc_async_stream_test;
using grpc_async_stream::StreamState;

using BidiCall   = FakeCall<Request, Response>;
using UploadCall = FakeCall<Request, UploadSummary>;

using BidiMock   = ClientAsyncReaderWriterMock<Request, Response>;
using WriterMock = ClientAsyncWriterMock<Request, UploadSummary>;
using ReaderMock = ClientAsyncReaderMock<Request, Response>;

using ReadWriteStream = grpc_async_stream::AsyncReadWriteStream<Request, Response>;
using WriteStream     = grpc_async_stream::AsyncWriteStream<Request>;
using ReadStream      = grpc_async_stream::AsyncReadStream<Response>;

constexpr std::chrono::seconds poll_timeout(5);

Request makeRequest(std::uint64_t id)
{
    Request request;
    request.set_id(id);
    return request;
}

Response makeResponse(std::uint64_t id)
{
    Response response;
    response.set_id(id);
    return response;
}

// ----- Compile-time checks: the mocks implement the gRPC interfaces and can be derived from -----

static_assert(std::is_base_of_v<grpc::ClientAsyncReaderWriterInterface<Request, Response>, BidiMock>);
static_assert(std::is_base_of_v<grpc::ClientAsyncWriterInterface<Request>, WriterMock>);
static_assert(std::is_base_of_v<grpc::ClientAsyncReaderInterface<Response>, ReaderMock>);
static_assert(!std::is_final_v<BidiMock> && !std::is_final_v<WriterMock> && !std::is_final_v<ReaderMock>);
static_assert(std::has_virtual_destructor_v<BidiMock> && std::has_virtual_destructor_v<WriterMock> &&
              std::has_virtual_destructor_v<ReaderMock>);
static_assert(!std::is_copy_constructible_v<BidiMock> && !std::is_move_constructible_v<BidiMock>);

static_assert(ClientAsyncMock<BidiMock>);
static_assert(ClientAsyncMock<WriterMock>);
static_assert(ClientAsyncMock<WriterMock, UploadSummary*>);
static_assert(ClientAsyncMock<ReaderMock, Request>);
static_assert(!ClientAsyncMock<ReaderMock>); // a server-streaming call needs its request
static_assert(!ClientAsyncMock<BidiCall>);

static_assert(grpc_async_stream::AsyncStreamFactoryFor<
              decltype(makeMockStreamFactory<BidiMock>(std::shared_ptr<BidiCall>())),
              grpc::ClientAsyncReaderWriterInterface<Request, Response>>);
static_assert(grpc_async_stream::AsyncStreamFactoryFor<
              decltype(makeMockStreamFactory<WriterMock>(std::shared_ptr<UploadCall>(), static_cast<UploadSummary*>(nullptr))),
              grpc::ClientAsyncWriterInterface<Request>>);
static_assert(grpc_async_stream::AsyncStreamFactoryFor<
              decltype(makeMockStreamFactory<ReaderMock>(std::shared_ptr<BidiCall>(), Request())),
              grpc::ClientAsyncReaderInterface<Response>>);

// ----- Derived mocks -----

// Completes the start-call as soon as the call is created, and counts the
// writes in a counter shared with the test (an extra constructor argument)
class AutoStartedCountingMock : public BidiMock
{
public:
    AutoStartedCountingMock(std::shared_ptr<Call> call, std::shared_ptr<int> write_count)
        : BidiMock(std::move(call))
        , m_write_count(std::move(write_count))
    {
    }

    void start(grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) override
    {
        BidiMock::start(context, cq, tag);
        call().completeStart();
    }

    using BidiMock::Write; // keeps the overload with WriteOptions visible
    void Write(const Request& msg, void* tag) override
    {
        ++*m_write_count;
        BidiMock::Write(msg, tag);
    }

private:
    std::shared_ptr<int> m_write_count;
};

// The start-call fails, as with a server that cannot be reached
class FailingStartWriterMock : public WriterMock
{
public:
    using WriterMock::WriterMock;

    void start(grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) override
    {
        WriterMock::start(context, cq, tag);
        call().completeStart(false);
    }
};

// Answers every read with the next of `count` responses whose ids follow the
// id of the request, then ends the stream: no completion by the test needed
class ScriptedReaderMock : public ReaderMock
{
public:
    ScriptedReaderMock(std::shared_ptr<Call> call, Request request, std::size_t count)
        : ReaderMock(std::move(call), std::move(request))
        , m_count(count)
    {
    }

    void start(grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) override
    {
        ReaderMock::start(context, cq, tag);
        call().completeStart();
    }

    void Read(Response* msg, void* tag) override
    {
        ReaderMock::Read(msg, tag);
        if (m_delivered < m_count)
            call().deliver(makeResponse(request().id() + m_delivered++));
        else
            call().endReads();
    }

private:
    std::size_t m_count;
    std::size_t m_delivered = 0;
};

} // namespace

TEST_CASE("Client async mocks record operations in their FakeCall", "[mocks]")
{
    auto call = std::make_shared<BidiCall>();
    {
        BidiMock mock(call);
        CHECK(mock.sharedCall() == call);
        CHECK(&mock.call() == call.get());

        mock.StartCall(nullptr);
        mock.ReadInitialMetadata(nullptr);
        CHECK(mock.startCallCalls() == 1);
        CHECK(mock.readInitialMetadataCalls() == 1);

        int tag = 0;
        mock.Write(makeRequest(1), &tag);
        CHECK(call->writePending());
        mock.Write(makeRequest(2), grpc::WriteOptions(), &tag);
        CHECK(call->violations() == std::vector<std::string>{"Write while another write operation is outstanding"});
    }
    CHECK(grpc_async_stream_test::idsOf(call->writes()) == std::vector<std::uint64_t>{1, 2});
}

TEST_CASE("Derived bidirectional mock drives AsyncReadWriteStream", "[mocks]")
{
    auto                  call        = std::make_shared<BidiCall>();
    auto                  write_count = std::make_shared<int>(0);
    ReadWriteStream       stream;
    std::vector<Response> received;
    auto handler = [&](Response&& response) { received.push_back(std::move(response)); };

    REQUIRE(stream.open(makeMockStreamFactory<AutoStartedCountingMock>(call, write_count)));
    auto* mock = dynamic_cast<AutoStartedCountingMock*>(stream.getReaderWriter());
    REQUIRE(mock != nullptr);
    CHECK(&mock->call() == call.get());
    CHECK_FALSE(call->startPending()); // completed by the mock

    // No completeStart() needed: the request is written once the start-call event is processed
    stream.sendRequest(makeRequest(1));
    REQUIRE(pollUntil(stream, handler, [&] { return call->writePending(); }, poll_timeout));
    CHECK(*write_count == 1);
    call->completeWrite();
    REQUIRE(pollUntil(stream, handler, [&] { return !stream.isWriteInProgress(); }, poll_timeout));

    call->deliver(makeResponse(7));
    REQUIRE(pollUntil(stream, handler, [&] { return received.size() == 1; }, poll_timeout));
    CHECK(received.front().id() == 7);

    REQUIRE(stream.writesDone());
    REQUIRE(pollUntil(stream, handler, [&] { return call->writesDonePending(); }, poll_timeout));
    call->completeWritesDone();
    REQUIRE(pollUntil(stream, handler, [&] { return stream.state() == StreamState::HalfClosed; }, poll_timeout));
    call->endReads();
    REQUIRE(pollUntil(stream, handler, [&] { return call->finishPending(); }, poll_timeout));
    call->completeFinish(grpc::Status::OK);
    REQUIRE(pollUntilClosed(stream, handler, poll_timeout));

    CHECK(stream.status().ok());
    CHECK(call->writes().size() == 1);
    CHECK(*write_count == 1);
    CHECK(call->violations().empty());
}

TEST_CASE("Client-streaming mock receives the response of the call", "[mocks]")
{
    auto          call = std::make_shared<UploadCall>();
    UploadSummary summary; // declared before the stream: must outlive the call
    WriteStream   stream;

    REQUIRE(stream.open(makeMockStreamFactory<WriterMock>(call, &summary)));
    CHECK(call->startPending());
    call->completeStart();

    for (const std::uint64_t id : {1U, 2U})
    {
        stream.sendRequest(makeRequest(id));
        REQUIRE(pollUntil(stream, [&] { return call->writePending(); }, poll_timeout));
        call->completeWrite();
        REQUIRE(pollUntil(stream, [&] { return stream.writeQueueStats().written_messages == id; }, poll_timeout));
    }
    REQUIRE(stream.writesDone());
    REQUIRE(pollUntil(stream, [&] { return call->writesDonePending(); }, poll_timeout));
    call->completeWritesDone();
    REQUIRE(pollUntil(stream, [&] { return call->finishPending(); }, poll_timeout));

    UploadSummary response;
    response.set_received_count(2);
    call->completeFinish(grpc::Status::OK, response);
    REQUIRE(pollUntilClosed(stream, poll_timeout));

    CHECK(stream.status().ok());
    CHECK(summary.received_count() == 2);
    CHECK(grpc_async_stream_test::idsOf(call->writes()) == std::vector<std::uint64_t>{1, 2});
    CHECK(call->violations().empty());
}

TEST_CASE("Derived client-streaming mock fails the start-call", "[mocks]")
{
    auto          call = std::make_shared<UploadCall>();
    UploadSummary summary;
    WriteStream   stream;

    REQUIRE(stream.open(makeMockStreamFactory<FailingStartWriterMock>(call, &summary)));
    stream.sendRequest(makeRequest(1)); // queued: the start-call has not been processed yet
    REQUIRE(pollUntil(stream, [&] { return call->finishPending(); }, poll_timeout));
    call->completeFinish(grpc::Status(grpc::StatusCode::UNAVAILABLE, "server unreachable"));
    REQUIRE(pollUntilClosed(stream, poll_timeout));

    CHECK(stream.status().error_code() == grpc::StatusCode::UNAVAILABLE);
    CHECK(call->writes().empty());
    CHECK(call->violations().empty());
}

TEST_CASE("Derived server-streaming mock scripts the responses", "[mocks]")
{
    auto                  call = std::make_shared<BidiCall>();
    ReadStream            stream;
    std::vector<Response> received;
    auto handler = [&](Response&& response) { received.push_back(std::move(response)); };

    REQUIRE(stream.open(makeMockStreamFactory<ScriptedReaderMock>(call, makeRequest(100), std::size_t{3})));
    REQUIRE(call->initialRequest().has_value());
    CHECK(call->initialRequest()->id() == 100);

    REQUIRE(pollUntil(stream, handler, [&] { return call->finishPending(); }, poll_timeout));
    call->completeFinish(grpc::Status::OK);
    REQUIRE(pollUntilClosed(stream, handler, poll_timeout));

    CHECK(stream.status().ok());
    CHECK(grpc_async_stream_test::idsOf(received) == std::vector<std::uint64_t>{100, 101, 102});
    CHECK(call->violations().empty());
}

TEST_CASE("Closing the stream destroys its derived mock", "[mocks]")
{
    // Tells the test when the stream releases it
    class DestructionTrackingMock : public ReaderMock
    {
    public:
        DestructionTrackingMock(std::shared_ptr<Call> call, Request request, std::shared_ptr<bool> destroyed)
            : ReaderMock(std::move(call), std::move(request))
            , m_destroyed(std::move(destroyed))
        {
        }
        ~DestructionTrackingMock() override { *m_destroyed = true; }

    private:
        std::shared_ptr<bool> m_destroyed;
    };

    auto       call      = std::make_shared<BidiCall>();
    auto       destroyed = std::make_shared<bool>(false);
    ReadStream stream;

    REQUIRE(stream.open(makeMockStreamFactory<DestructionTrackingMock>(call, makeRequest(1), destroyed)));
    CHECK(call->startPending());
    CHECK(call->readPending());
    CHECK_FALSE(*destroyed);

    stream.close();
    CHECK(*destroyed);
    CHECK_FALSE(stream.isOpen());
    CHECK(stream.status().error_code() == grpc::StatusCode::CANCELLED);
    CHECK(call->violations().empty());
}
