// Deterministic tests of the AsyncReadWriteStream state machine driven through
// a fake ClientAsyncReaderWriterInterface (see grpc_test_utils/FakeReaderWriter.h).

#include "support/TestServer.h"

#include <grpc_test_utils/Catch2StringMakers.h>
#include <grpc_test_utils/EnumReflection.h>
#include <grpc_test_utils/FakeReaderWriter.h>
#include <grpc_test_utils/StreamTestUtils.h>
#include <grpc_utils/AsyncReadWriteStream.h>

#include <catch2/catch_test_macros.hpp>

#include <functional>
#include <ranges>
#include <string>
#include <vector>

namespace
{

using namespace TestSupport;
using namespace grpc_test_utils;
using grpc_utils::SendResult;
using grpc_utils::StreamState;

using Stream = grpc_utils::AsyncReadWriteStream<Request, Response>;
using Call   = FakeCall<Request, Response>;

Response makeResponse(std::uint64_t id)
{
    Response response;
    response.set_id(id);
    return response;
}

struct FakeStreamFixture
{
    std::shared_ptr<Call> call = std::make_shared<Call>();
    std::vector<Response> responses;
    Stream                stream;

    std::function<void(Response&&)> handler = [this](Response&& response) {
        responses.push_back(std::move(response));
    };

    bool open() { return stream.open(makeFakeStreamFactory(call)); }

    // Opens the stream and completes the start-call
    void openStarted()
    {
        REQUIRE(open());
        call->completeStart();
        REQUIRE(pump([&] { return !stream.isWriteInProgress(); }));
    }

    template<typename Predicate>
    bool pump(Predicate&& predicate)
    {
        return pollUntil(stream, handler, std::forward<Predicate>(predicate), std::chrono::seconds(5));
    }

    bool pumpUntilClosed() { return pollUntilClosed(stream, handler, std::chrono::seconds(5)); }

    // Completes `count` writes one by one, waiting for each next write to be issued
    void completeWrites(std::size_t count)
    {
        for (std::size_t i = 0; i < count; ++i)
        {
            REQUIRE(pump([&] { return call->writePending(); }));
            const std::size_t written_before = stream.writeQueueStats().written_messages;
            call->completeWrite();
            REQUIRE(pump([&] { return stream.writeQueueStats().written_messages == written_before + 1; }));
        }
    }

    std::vector<std::uint64_t> writtenIds() const { return idsOf(call->writes()); }

    void checkNoViolations() const
    {
        for (const std::string& violation : call->violations())
            UNSCOPED_INFO("gRPC API violation: " << violation);
        CHECK(call->violations().empty());
    }
};

} // namespace

TEST_CASE("A stream is closed until opened", "[lifecycle]")
{
    Stream stream;
    CHECK(stream.state() == StreamState::Closed);
    CHECK_FALSE(stream.isOpen());
    CHECK_FALSE(stream.canWrite());
    CHECK(stream.getReaderWriter() == nullptr);
    CHECK(stream.status().ok());
    CHECK(stream.sendRequest(makeEcho(1)) == SendResult::NotWritable);
    CHECK_FALSE(stream.writesDone());
    CHECK_FALSE(stream.poll([](Response&&) {}));
    CHECK(stream.writeQueueStats().submitted_messages == 0);
}

TEST_CASE("open() fails when the factory returns no stream", "[lifecycle]")
{
    Stream stream;
    const bool opened = stream.open([](grpc::ClientContext*, grpc::CompletionQueue*, void*) {
        return std::unique_ptr<Stream::AsyncReaderWriterInterface>();
    });
    CHECK_FALSE(opened);
    CHECK_FALSE(stream.isOpen());
    CHECK_FALSE(stream.canWrite());
    CHECK(stream.status().error_code() == grpc::StatusCode::INTERNAL);
}

TEST_CASE("open() passes a fresh context and completion queue to the factory", "[lifecycle]")
{
    Stream                 stream;
    auto                   call = std::make_shared<Call>();
    grpc::ClientContext*   first_context = nullptr;
    grpc::CompletionQueue* first_queue   = nullptr;

    auto factory = makeFakeStreamFactory(call);
    REQUIRE(stream.open([&](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
        first_context = context;
        first_queue   = cq;
        return factory(context, cq, tag);
    }));
    CHECK(first_context != nullptr);
    CHECK(first_queue != nullptr);
    CHECK(stream.getReaderWriter() != nullptr);
}

TEST_CASE("Requests sent before the start-call completes are queued, not written", "[lifecycle]")
{
    FakeStreamFixture f;
    REQUIRE(f.open());

    CHECK(f.stream.isOpen());
    CHECK(f.stream.canWrite());
    CHECK(f.stream.state() == StreamState::Open);
    CHECK(f.call->readPending());
    CHECK(f.call->startPending());
    CHECK(f.stream.isWriteInProgress());

    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);
    CHECK(f.call->writes().empty());
    CHECK(f.stream.writeQueueStats().queued_messages == 2);

    f.call->completeStart();
    REQUIRE(f.pump([&] { return f.call->writePending(); }));
    CHECK(f.writtenIds() == std::vector<std::uint64_t>{1});
    CHECK(f.stream.writeQueueStats().queued_messages == 1);

    f.completeWrites(2);
    CHECK(f.writtenIds() == std::vector<std::uint64_t>{1, 2});
    CHECK(f.stream.writeQueueStats().written_messages == 2);
    CHECK_FALSE(f.stream.isWriteInProgress());
    f.checkNoViolations();
}

TEST_CASE("Writes are serialized and sent in FIFO order", "[lifecycle]")
{
    FakeStreamFixture f;
    f.openStarted();

    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Written);
    for (std::uint64_t id = 2; id <= 10; ++id)
        CHECK(f.stream.sendRequest(makeEcho(id)) == SendResult::Queued);

    CHECK(f.call->writes().size() == 1);
    f.completeWrites(10);

    CHECK(f.writtenIds() == std::vector<std::uint64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.submitted_messages == 10);
    CHECK(stats.written_messages == 10);
    CHECK(stats.queued_messages == 0);
    CHECK(stats.peak_queued_messages == 9);
    CHECK_FALSE(stats.write_in_flight);

    // With the write slot free, the next request is written immediately again
    CHECK(f.stream.sendRequest(makeEcho(11)) == SendResult::Written);
    f.checkNoViolations();
}

TEST_CASE("Responses are delivered in order and the read is re-armed", "[lifecycle]")
{
    FakeStreamFixture f;
    f.openStarted();

    for (const std::uint64_t id : std::views::iota(std::uint64_t{1}, std::uint64_t{6}))
    {
        REQUIRE(f.call->readPending());
        f.call->deliver(makeResponse(id));
        REQUIRE(f.pump([&] { return f.responses.size() == id; }));
    }

    CHECK(idsOf(f.responses) == std::vector<std::uint64_t>{1, 2, 3, 4, 5});
    CHECK(f.call->readPending());
    CHECK(f.stream.isOpen());
    f.checkNoViolations();
}

TEST_CASE("poll() without events returns immediately and keeps the stream open", "[lifecycle]")
{
    FakeStreamFixture f;
    f.openStarted();
    for (int i = 0; i < 100; ++i)
        CHECK(f.stream.poll(f.handler));
    CHECK(f.responses.empty());
}

TEST_CASE("pollFor() processes one event per call by default", "[lifecycle]")
{
    FakeStreamFixture f;
    f.openStarted();
    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Written);

    // Two completions at once: the write and a response
    f.call->completeWrite();
    f.call->deliver(makeResponse(42));
    auto processed = [&] { return f.responses.size() + f.stream.writeQueueStats().written_messages; };

    // Each call waits for one event (both are posted, the waits end at once)
    REQUIRE(f.stream.pollFor(f.handler, std::chrono::seconds(5)));
    CHECK(processed() == 1);
    REQUIRE(f.stream.pollFor(f.handler, std::chrono::seconds(5)));
    CHECK(processed() == 2);
    CHECK(idsOf(f.responses) == std::vector<std::uint64_t>{42});
    CHECK(f.call->readPending());
    f.checkNoViolations();
}

TEST_CASE("writesDone() flushes the queued requests before half-closing", "[lifecycle]")
{
    FakeStreamFixture f;
    REQUIRE(f.open());
    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);

    CHECK(f.stream.writesDone());
    CHECK(f.stream.state() == StreamState::HalfClosing);
    CHECK_FALSE(f.stream.canWrite());
    CHECK(f.stream.isOpen());
    CHECK_FALSE(f.stream.writesDone());
    CHECK(f.stream.sendRequest(makeEcho(3)) == SendResult::NotWritable);
    CHECK(f.stream.writeQueueStats().submitted_messages == 2);

    f.call->completeStart();
    f.completeWrites(2);
    REQUIRE(f.pump([&] { return f.call->writesDonePending(); }));
    CHECK(f.writtenIds() == std::vector<std::uint64_t>{1, 2});

    f.call->completeWritesDone();
    REQUIRE(f.pump([&] { return f.stream.state() == StreamState::HalfClosed; }));
    // Still reading after the half-close
    f.call->deliver(makeResponse(42));
    REQUIRE(f.pump([&] { return f.responses.size() == 1; }));
    CHECK(f.responses.front().id() == 42);
    CHECK(f.stream.isOpen());
    CHECK_FALSE(f.call->finishPending());

    f.call->endReads();
    REQUIRE(f.pump([&] { return f.call->finishPending(); }));
    f.call->completeFinish(grpc::Status::OK);
    REQUIRE(f.pumpUntilClosed());

    CHECK(f.stream.status().ok());
    CHECK_FALSE(f.stream.isOpen());
    CHECK(f.stream.state() == StreamState::Closed);
    CHECK(f.call->finishCalls() == 1);
    f.checkNoViolations();
}

TEST_CASE("writesDone() on an idle stream is issued immediately", "[lifecycle]")
{
    FakeStreamFixture f;
    f.openStarted();
    CHECK(f.stream.writesDone());
    CHECK(f.call->writesDonePending());
    f.checkNoViolations();
}

TEST_CASE("Server end of stream: Finish waits for the outstanding write", "[lifecycle]")
{
    FakeStreamFixture f;
    f.openStarted();
    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Written);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(3)) == SendResult::Queued);

    f.call->endReads();
    REQUIRE(f.pump([&] { return !f.stream.canWrite(); }));

    // Queued requests are discarded, Finish is deferred until the write completes
    CHECK(f.stream.state() == StreamState::Finishing);
    CHECK(f.stream.isOpen());
    CHECK_FALSE(f.call->finishPending());
    CHECK(f.stream.writeQueueStats().queued_messages == 0);
    CHECK(f.stream.writeQueueStats().discarded_messages == 2);
    CHECK(f.stream.sendRequest(makeEcho(4)) == SendResult::NotWritable);

    f.call->completeWrite(false);
    REQUIRE(f.pump([&] { return f.call->finishPending(); }));
    f.call->completeFinish(grpc::Status(grpc::StatusCode::UNAVAILABLE, "gone"));
    REQUIRE(f.pumpUntilClosed());

    CHECK(f.stream.status().error_code() == grpc::StatusCode::UNAVAILABLE);
    CHECK(f.stream.status().error_message() == "gone");
    CHECK(f.call->writes().size() == 1);

    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.discarded_messages == 3);
    CHECK(stats.isConsistent());
    f.checkNoViolations();
}

TEST_CASE("A failed write ends the call with the status reported by Finish", "[lifecycle]")
{
    FakeStreamFixture f;
    f.openStarted();
    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Written);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);

    f.call->completeWrite(false);
    REQUIRE(f.pump([&] { return !f.stream.canWrite(); }));
    CHECK(f.stream.writeQueueStats().discarded_messages == 2);

    // Finish must wait for the outstanding read to end
    CHECK_FALSE(f.call->finishPending());
    f.call->endReads();
    REQUIRE(f.pump([&] { return f.call->finishPending(); }));
    f.call->completeFinish(grpc::Status(grpc::StatusCode::INTERNAL, "broken"));
    REQUIRE(f.pumpUntilClosed());

    CHECK(f.stream.status().error_code() == grpc::StatusCode::INTERNAL);
    f.checkNoViolations();
}

TEST_CASE("A failed start-call discards the queued requests and reports the error", "[lifecycle]")
{
    FakeStreamFixture f;
    REQUIRE(f.open());
    for (std::uint64_t id = 1; id <= 3; ++id)
        CHECK(f.stream.sendRequest(makeEcho(id)) == SendResult::Queued);

    f.call->completeStart(false);
    REQUIRE(f.pump([&] { return !f.stream.canWrite(); }));
    CHECK(f.stream.writeQueueStats().discarded_messages == 3);

    f.call->endReads();
    REQUIRE(f.pump([&] { return f.call->finishPending(); }));
    f.call->completeFinish(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no connection"));
    REQUIRE(f.pumpUntilClosed());

    CHECK(f.call->writes().empty());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::UNAVAILABLE);
    f.checkNoViolations();
}

TEST_CASE("close() cancels an active call and discards the queued requests", "[lifecycle]")
{
    FakeStreamFixture f;
    f.openStarted();
    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Written);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(3)) == SendResult::Queued);

    f.stream.close();

    CHECK_FALSE(f.stream.isOpen());
    CHECK_FALSE(f.stream.canWrite());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
    CHECK_FALSE(f.stream.poll(f.handler));
    CHECK(f.stream.sendRequest(makeEcho(4)) == SendResult::NotWritable);

    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.queued_messages == 0);
    CHECK(stats.discarded_messages == 3); // two queued + the unconfirmed in-flight write
    CHECK_FALSE(stats.write_in_flight);

    // Closing again keeps the status
    f.stream.close();
    CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
}

TEST_CASE("The response handler may send requests and close the stream re-entrantly", "[lifecycle]")
{
    FakeStreamFixture f;
    f.openStarted();

    int  handled   = 0;
    auto reentrant = [&](Response&& response) {
        ++handled;
        if (response.id() == 1)
            CHECK(f.stream.sendRequest(makeEcho(100)) == SendResult::Written);
        else if (response.id() == 2)
            f.stream.close();
    };

    f.call->deliver(makeResponse(1));
    REQUIRE(pollUntil(f.stream, reentrant, [&] { return handled == 1; }));
    CHECK(f.writtenIds() == std::vector<std::uint64_t>{100});

    f.call->completeWrite();
    f.call->deliver(makeResponse(2));
    pollUntil(f.stream, reentrant, [&] { return handled == 2; });

    CHECK(handled == 2);
    CHECK_FALSE(f.stream.isOpen());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
}

TEST_CASE("A stream can be reopened and its statistics are reset", "[lifecycle]")
{
    FakeStreamFixture f;
    f.openStarted();
    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Written);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);

    auto second_call = std::make_shared<Call>();
    REQUIRE(f.stream.open(makeFakeStreamFactory(second_call)));

    CHECK(f.stream.isOpen());
    CHECK(f.stream.status().ok());
    CHECK(f.stream.writeQueueStats().submitted_messages == 0);
    CHECK(f.stream.writeQueueStats().discarded_messages == 0);
    CHECK(second_call->startPending());
    CHECK(second_call->readPending());
}

TEST_CASE("The destructor releases an active call", "[lifecycle]")
{
    auto call = std::make_shared<Call>();
    {
        Stream stream;
        REQUIRE(stream.open(makeFakeStreamFactory(call)));
        for (std::uint64_t id = 0; id < 100; ++id)
            stream.sendRequest(makeEcho(id, std::string(1000, 'x')));
    }
    // The fake stream object has been destroyed (the shared call state survives)
    CHECK(call.use_count() == 1);
}

TEST_CASE("Concepts accept valid arguments and reject invalid ones", "[lifecycle][concepts]")
{
    using grpc_utils::ProtobufMessage;
    using grpc_utils::ResponseHandlerFor;
    using grpc_utils::StreamFactoryFor;

    // Message types
    static_assert(ProtobufMessage<Request>);
    static_assert(ProtobufMessage<Response>);
    static_assert(!ProtobufMessage<int>);
    static_assert(!ProtobufMessage<std::string>);

    // Stream factories (named lambdas: lambdas in unevaluated operands are not
    // fully supported by every compiler yet)
    using FakeFactory            = decltype(makeFakeStreamFactory(std::shared_ptr<Call>()));
    [[maybe_unused]] constexpr auto wrong_factory = [](grpc::ClientContext*, grpc::CompletionQueue*, void*) { return 42; };
    static_assert(StreamFactoryFor<FakeFactory, Request, Response>);
    static_assert(!StreamFactoryFor<FakeFactory, Response, Request>); // wrong message order
    static_assert(!StreamFactoryFor<int (*)(), Request, Response>);
    static_assert(!StreamFactoryFor<decltype(wrong_factory), Request, Response>);

    // Response handlers: rvalue, const reference and by-value parameters all work
    [[maybe_unused]] constexpr auto takes_rvalue       = [](Response&&) {};
    [[maybe_unused]] constexpr auto takes_const_ref    = [](const Response&) {};
    [[maybe_unused]] constexpr auto takes_value        = [](Response) {};
    [[maybe_unused]] constexpr auto takes_mutable_ref  = [](Response&) {};
    [[maybe_unused]] constexpr auto takes_wrong_type   = [](const Request&) {};
    static_assert(ResponseHandlerFor<decltype(takes_rvalue), Response>);
    static_assert(ResponseHandlerFor<decltype(takes_const_ref), Response>);
    static_assert(ResponseHandlerFor<decltype(takes_value), Response>);
    static_assert(ResponseHandlerFor<std::function<void(Response&&)>, Response>);
    static_assert(!ResponseHandlerFor<decltype(takes_mutable_ref), Response>); // cannot bind an rvalue
    static_assert(!ResponseHandlerFor<decltype(takes_wrong_type), Response>);
    static_assert(!ResponseHandlerFor<int, Response>);

    SUCCEED("checked at compile time");
}
