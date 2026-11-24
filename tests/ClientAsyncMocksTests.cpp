// Tests of the extensible client stream mocks (grpc_async_stream_test/ClientAsyncMocks.h):
// the mocks used as they are and through derived mocks that change the default
// behaviour (start-call completed right away or failing, echoed and scripted
// responses pushed with enqueueResponse()), driving AsyncReadWriteStream,
// AsyncWriteStream and AsyncReadStream.

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

using BidiMock   = ClientAsyncReaderWriterMock<Request, Response>;
using WriterMock = ClientAsyncWriterMock<Request>;
using ReaderMock = ClientAsyncReaderMock<Response>;

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

// Constructed from the completion queue only
static_assert(std::is_constructible_v<BidiMock, grpc::CompletionQueue*> && !std::is_default_constructible_v<BidiMock>);
static_assert(std::is_constructible_v<WriterMock, grpc::CompletionQueue*> && !std::is_default_constructible_v<WriterMock>);
static_assert(std::is_constructible_v<ReaderMock, grpc::CompletionQueue*> && !std::is_default_constructible_v<ReaderMock>);
static_assert(!std::is_convertible_v<grpc::CompletionQueue*, BidiMock>); // explicit

static_assert(ClientAsyncMock<BidiMock> && ClientAsyncMock<WriterMock> && ClientAsyncMock<ReaderMock>);
static_assert(!ClientAsyncMock<FakeCall<Request, Response>>);

static_assert(grpc_async_stream::AsyncStreamFactoryFor<decltype(makeMockStreamFactory<BidiMock>()),
                                                       grpc::ClientAsyncReaderWriterInterface<Request, Response>>);
static_assert(grpc_async_stream::AsyncStreamFactoryFor<decltype(makeMockStreamFactory<WriterMock>()),
                                                       grpc::ClientAsyncWriterInterface<Request>>);
static_assert(grpc_async_stream::AsyncStreamFactoryFor<decltype(makeMockStreamFactory<ReaderMock>()),
                                                       grpc::ClientAsyncReaderInterface<Response>>);

// ----- Derived mocks -----

// Completes the start-call at once and echoes every written request: the write
// completes right away and its response is pushed with enqueueResponse()
class EchoMock : public BidiMock
{
public:
    using BidiMock::BidiMock;

    void StartCall(void* tag) override
    {
        BidiMock::StartCall(tag);
        call().completeStart();
    }

    using BidiMock::Write; // keeps the overload with WriteOptions visible
    void Write(const Request& msg, void* tag) override
    {
        BidiMock::Write(msg, tag);
        call().completeWrite();
        pushResponse(msg.id());
    }

    // Response pushed by the test, as if the server sent it on its own
    void pushResponse(std::uint64_t id) { enqueueResponse(makeResponse(id)); }
};

// The start-call fails, as with a server that cannot be reached
class FailingStartWriterMock : public WriterMock
{
public:
    using WriterMock::WriterMock;

    void StartCall(void* tag) override
    {
        WriterMock::StartCall(tag);
        call().completeStart(false);
    }
};

// Answers the reads with the scripted responses, then ends the stream: no
// completion by the test needed except the final status
class ScriptedReaderMock : public ReaderMock
{
public:
    using ReaderMock::ReaderMock;

    void script(std::uint64_t first_id, std::size_t count)
    {
        for (std::size_t i = 0; i < count; ++i)
            enqueueResponse(makeResponse(first_id + i));
    }

    void StartCall(void* tag) override
    {
        ReaderMock::StartCall(tag);
        call().completeStart();
    }

    void Read(Response* msg, void* tag) override
    {
        ReaderMock::Read(msg, tag);
        if (call().readPending() && queuedResponses() == 0) // nothing left to deliver
            call().endReads();
    }
};

// Tells the test when the stream releases it
class DestructionTrackingMock : public ReaderMock
{
public:
    using ReaderMock::ReaderMock;
    ~DestructionTrackingMock() override
    {
        if (destroyed)
            *destroyed = true;
    }

    std::shared_ptr<bool> destroyed;
};

} // namespace

TEST_CASE("Client async mocks record operations in their own FakeCall", "[mocks]")
{
    grpc::CompletionQueue           cq;
    std::shared_ptr<BidiMock::Call> call;
    {
        BidiMock mock(&cq);
        call = mock.sharedCall();
        CHECK(&mock.call() == call.get());
        CHECK(mock.completionQueue() == &cq);
        CHECK_FALSE(mock.started());

        int tag = 0;
        mock.Write(makeRequest(1), &tag);
        CHECK(call->writePending());
        mock.Write(makeRequest(2), grpc::WriteOptions(), &tag);
        CHECK(call->violations() == std::vector<std::string>{"Write while another write operation is outstanding"});

        mock.StartCall(&tag);
        mock.ReadInitialMetadata(&tag);
        CHECK(mock.started());
        CHECK(call->startPending());
        CHECK(mock.startCallCalls() == 1);
        CHECK(mock.readInitialMetadataCalls() == 1);
        CHECK(mock.queuedResponses() == 0);
    }
    CHECK(grpc_async_stream_test::idsOf(call->writes()) == std::vector<std::uint64_t>{1, 2});
    cq.Shutdown();
}

TEST_CASE("Derived bidirectional mock echoes the requests through enqueueResponse", "[mocks]")
{
    EchoMock*             mock = nullptr;
    ReadWriteStream       stream;
    std::vector<Response> received;
    auto handler = [&](Response&& response) { received.push_back(std::move(response)); };

    REQUIRE(stream.open(makeMockStreamFactory<EchoMock>([&](EchoMock& created) { mock = &created; })));
    REQUIRE(mock != nullptr);
    CHECK(stream.getReaderWriter() == mock);
    CHECK(mock->startCallCalls() == 1); // started by the factory
    const std::shared_ptr<EchoMock::Call> call = mock->sharedCall();

    // No completion by the test: the mock completes the start-call and the writes, and echoes
    for (const std::uint64_t id : {1U, 2U, 3U})
        stream.sendRequest(makeRequest(id));
    REQUIRE(pollUntil(stream, handler, [&] { return received.size() == 3; }, poll_timeout));

    // Responses pushed by the test are delivered in order, one per Read
    mock->pushResponse(10);
    mock->pushResponse(11);
    REQUIRE(pollUntil(stream, handler, [&] { return received.size() == 5; }, poll_timeout));
    CHECK(mock->queuedResponses() == 0);
    CHECK(grpc_async_stream_test::idsOf(received) == std::vector<std::uint64_t>{1, 2, 3, 10, 11});

    REQUIRE(stream.writesDone());
    REQUIRE(pollUntil(stream, handler, [&] { return call->writesDonePending(); }, poll_timeout));
    call->completeWritesDone();
    REQUIRE(pollUntil(stream, handler, [&] { return stream.state() == StreamState::HalfClosed; }, poll_timeout));
    call->endReads();
    REQUIRE(pollUntil(stream, handler, [&] { return call->finishPending(); }, poll_timeout));
    call->completeFinish(grpc::Status::OK);
    REQUIRE(pollUntilClosed(stream, handler, poll_timeout));

    CHECK(stream.status().ok());
    CHECK(grpc_async_stream_test::idsOf(call->writes()) == std::vector<std::uint64_t>{1, 2, 3});
    CHECK(call->violations().empty());
}

TEST_CASE("Client-streaming mock completed by the test", "[mocks]")
{
    std::shared_ptr<WriterMock::Call> call;
    WriteStream                       stream;

    REQUIRE(stream.open(makeMockStreamFactory<WriterMock>([&](WriterMock& created) { call = created.sharedCall(); })));
    REQUIRE(call != nullptr);
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
    call->completeFinish(grpc::Status::OK);
    REQUIRE(pollUntilClosed(stream, poll_timeout));

    CHECK(stream.status().ok());
    CHECK(grpc_async_stream_test::idsOf(call->writes()) == std::vector<std::uint64_t>{1, 2});
    CHECK(call->violations().empty());
}

TEST_CASE("Derived client-streaming mock fails the start-call", "[mocks]")
{
    std::shared_ptr<WriterMock::Call> call;
    WriteStream                       stream;

    REQUIRE(stream.open(
        makeMockStreamFactory<FailingStartWriterMock>([&](FailingStartWriterMock& created) { call = created.sharedCall(); })));
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
    std::shared_ptr<ReaderMock::Call> call;
    ReadStream                        stream;
    std::vector<Response>             received;
    auto handler = [&](Response&& response) { received.push_back(std::move(response)); };

    REQUIRE(stream.open(makeMockStreamFactory<ScriptedReaderMock>([&](ScriptedReaderMock& created) {
        created.script(100, 3);
        call = created.sharedCall();
    })));

    REQUIRE(pollUntil(stream, handler, [&] { return call->finishPending(); }, poll_timeout));
    call->completeFinish(grpc::Status::OK);
    REQUIRE(pollUntilClosed(stream, handler, poll_timeout));

    CHECK(stream.status().ok());
    CHECK(grpc_async_stream_test::idsOf(received) == std::vector<std::uint64_t>{100, 101, 102});
    CHECK(call->violations().empty());
}

TEST_CASE("Closing the stream destroys its derived mock", "[mocks]")
{
    auto                              destroyed = std::make_shared<bool>(false);
    std::shared_ptr<ReaderMock::Call> call;
    ReadStream                        stream;

    REQUIRE(stream.open(makeMockStreamFactory<DestructionTrackingMock>([&](DestructionTrackingMock& created) {
        created.destroyed = destroyed;
        call              = created.sharedCall();
    })));
    CHECK(call->startPending());
    CHECK(call->readPending());
    CHECK_FALSE(*destroyed);

    stream.close();
    CHECK(*destroyed);
    CHECK_FALSE(stream.isOpen());
    CHECK(stream.status().error_code() == grpc::StatusCode::CANCELLED);
    CHECK(call->violations().empty());
}
