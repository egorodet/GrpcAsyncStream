// Tests of AsyncReadStream, the wrapper over server-streaming calls: the state
// machine driven through a fake ClientAsyncReaderInterface (see
// grpc_test_utils/FakeReaderWriter.h), the composition of the three stream
// classes, and end-to-end calls of the Subscribe method of the in-process
// test server (see support/TestServer.h).

#include "support/TestServer.h"

#include <grpc_test_utils/Catch2StringMakers.h>
#include <grpc_test_utils/EnumReflection.h>
#include <grpc_test_utils/FakeReaderWriter.h>
#include <grpc_test_utils/InProcessServer.h>
#include <grpc_test_utils/StreamTestUtils.h>
#include <grpc_utils/AsyncReadStream.h>
#include <grpc_utils/AsyncReadWriteStream.h>
#include <grpc_utils/AsyncWriteStream.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <algorithm>
#include <functional>
#include <ranges>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

namespace
{

using namespace TestSupport;
using namespace grpc_test_utils;
using grpc_utils::StreamState;

using ReadStream = grpc_utils::AsyncReadStream<Response>;
using Call       = FakeCall<Request, Response>;

Response makeResponse(std::uint64_t id)
{
    Response response;
    response.set_id(id);
    return response;
}

struct FakeReadFixture
{
    std::shared_ptr<Call> call = std::make_shared<Call>();
    std::vector<Response> responses;
    ReadStream            stream;

    std::function<void(Response&&)> handler = [this](Response&& response) {
        responses.push_back(std::move(response));
    };

    bool open(std::uint64_t request_id = 7) { return stream.open(makeFakeReaderFactory(call, makeEcho(request_id))); }

    // Opens the stream and completes the start-call
    void openStarted()
    {
        REQUIRE(open());
        call->completeStart();
        REQUIRE(pump([&] { return !call->startPending(); }));
        // Let the stream process the start-call completion
        REQUIRE(stream.pollFor(handler, std::chrono::milliseconds(20)));
    }

    template<typename Predicate>
    bool pump(Predicate&& predicate)
    {
        return pollUntil(stream, handler, std::forward<Predicate>(predicate), std::chrono::seconds(5));
    }

    bool pumpUntilClosed() { return pollUntilClosed(stream, handler, std::chrono::seconds(5)); }

    void checkNoViolations() const
    {
        for (const std::string& violation : call->violations())
            UNSCOPED_INFO("gRPC API violation: " << violation);
        CHECK(call->violations().empty());
        CHECK(call->writes().empty());
        CHECK_FALSE(call->writesDoneCalled());
    }
};

struct SubscribeFixture
{
    TestServer                               server;
    std::unique_ptr<proto::TestStream::Stub> stub = server.makeStub();
    ReadStream                               stream;
    std::vector<Response>                    responses;
    std::function<void(Response&&)>          handler = [this](Response&& response) {
        responses.push_back(std::move(response));
    };

    bool open(const Request& request)
    {
        responses.clear();
        return stream.open([this, &request](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
            return stub->AsyncSubscribe(context, request, cq, tag);
        });
    }

    template<typename Predicate>
    bool pump(Predicate&& predicate, std::chrono::milliseconds timeout = std::chrono::seconds(20))
    {
        return pollUntil(stream, handler, std::forward<Predicate>(predicate), timeout);
    }

    bool finish(std::chrono::milliseconds timeout = std::chrono::seconds(20))
    {
        return pollUntilClosed(stream, handler, timeout);
    }
};

// Waits for a server-side condition updated from server threads
template<typename Predicate>
bool waitFor(Predicate&& predicate, std::chrono::milliseconds timeout = std::chrono::seconds(10))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return true;
}

} // namespace

// ---------------------------------------------------------------------------
// Composition of the stream classes and concepts
// ---------------------------------------------------------------------------

TEST_CASE("The stream classes are layers of single inheritance over one call core", "[read-stream][concepts]")
{
    using grpc_utils::AsyncReaderInterfaceFor;
    using grpc_utils::AsyncStreamFactoryFor;
    using grpc_utils::detail::AsyncCallBase;
    using BidiInterface   = grpc::ClientAsyncReaderWriterInterface<Request, Response>;
    using BidiCore        = AsyncCallBase<BidiInterface>;
    using BidiWriteLayer  = grpc_utils::AsyncWriteStream<Request, BidiCore>;
    using BidiReadLayer   = grpc_utils::AsyncReadStream<Response, BidiWriteLayer>;
    using ReadWriteStream = grpc_utils::AsyncReadWriteStream<Request, Response>;
    using WriteStream     = grpc_utils::AsyncWriteStream<Request>;

    // AsyncReadWriteStream -> AsyncReadStream -> AsyncWriteStream -> AsyncCallBase
    static_assert(std::is_base_of_v<BidiReadLayer, ReadWriteStream>);
    static_assert(std::is_base_of_v<BidiWriteLayer, BidiReadLayer>);
    static_assert(std::is_base_of_v<BidiCore, BidiWriteLayer>);
    static_assert(std::is_same_v<ReadWriteStream::AsyncStreamInterface, BidiInterface>);

    // The stand-alone streams sit directly on their own core
    static_assert(std::is_base_of_v<AsyncCallBase<grpc::ClientAsyncReaderInterface<Response>>, ReadStream>);
    static_assert(std::is_same_v<ReadStream::AsyncStreamInterface, grpc::ClientAsyncReaderInterface<Response>>);
    static_assert(std::is_base_of_v<AsyncCallBase<grpc::ClientAsyncWriterInterface<Request>>, WriteStream>);

    // No virtual inheritance and no virtual functions: one core per stream object
    static_assert(!std::is_polymorphic_v<ReadWriteStream>);
    static_assert(!std::is_polymorphic_v<ReadStream>);
    static_assert(!std::is_polymorphic_v<WriteStream>);

    // Both gRPC client stream interfaces with a read side qualify, write-only ones do not
    static_assert(AsyncReaderInterfaceFor<grpc::ClientAsyncReaderInterface<Response>, Response>);
    static_assert(AsyncReaderInterfaceFor<BidiInterface, Response>);
    static_assert(!AsyncReaderInterfaceFor<grpc::ClientAsyncWriterInterface<Request>, Response>);
    static_assert(!AsyncReaderInterfaceFor<grpc::ClientAsyncReaderInterface<Request>, Response>);

    using ReaderFactory = decltype(makeFakeReaderFactory(std::shared_ptr<Call>(), Request()));
    using BidiFactory   = decltype(makeFakeStreamFactory(std::shared_ptr<Call>()));
    static_assert(AsyncStreamFactoryFor<ReaderFactory, grpc::ClientAsyncReaderInterface<Response>>);
    static_assert(!AsyncStreamFactoryFor<BidiFactory, grpc::ClientAsyncReaderInterface<Response>>);
    static_assert(!AsyncStreamFactoryFor<ReaderFactory, BidiInterface>);

    // A read stream is polled with a response handler only, and cannot write
    static_assert(PollableWithHandler<ReadStream, std::function<void(Response&&)>>);
    static_assert(!PollableWithoutHandler<ReadStream>);

    SUCCEED("checked at compile time");
}

// ---------------------------------------------------------------------------
// State machine with a fake call
// ---------------------------------------------------------------------------

TEST_CASE("A read stream is closed until opened", "[read-stream]")
{
    ReadStream stream;
    CHECK(stream.state() == StreamState::Closed);
    CHECK_FALSE(stream.isOpen());
    CHECK(stream.getStream() == nullptr);
    CHECK(stream.status().ok());
    CHECK_FALSE(stream.poll([](Response&&) {}));
    CHECK_FALSE(stream.pollFor([](Response&&) {}, std::chrono::milliseconds(1)));
}

TEST_CASE("Read stream open() fails when the factory returns no stream", "[read-stream]")
{
    ReadStream stream;
    const bool opened = stream.open([](grpc::ClientContext*, grpc::CompletionQueue*, void*) {
        return std::unique_ptr<ReadStream::AsyncStreamInterface>();
    });
    CHECK_FALSE(opened);
    CHECK_FALSE(stream.isOpen());
    CHECK(stream.status().error_code() == grpc::StatusCode::INTERNAL);
}

TEST_CASE("A read stream passes the request to the factory and reads right away", "[read-stream]")
{
    FakeReadFixture f;
    REQUIRE(f.open(42));

    CHECK(f.stream.isOpen());
    CHECK(f.stream.state() == StreamState::Open);
    CHECK(f.stream.getStream() != nullptr);
    REQUIRE(f.call->initialRequest().has_value());
    CHECK(f.call->initialRequest()->id() == 42);
    CHECK(f.call->startPending());
    CHECK(f.call->readPending()); // reading does not wait for the start-call

    // Responses may even arrive before the start-call completion is processed
    f.call->deliver(makeResponse(1));
    REQUIRE(f.pump([&] { return f.responses.size() == 1; }));
    f.call->completeStart();
    for (std::uint64_t id = 2; id <= 5; ++id)
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

TEST_CASE("The end of the server stream requests the final status", "[read-stream]")
{
    FakeReadFixture f;
    f.openStarted();
    f.call->deliver(makeResponse(1));
    REQUIRE(f.pump([&] { return f.responses.size() == 1; }));

    f.call->endReads();
    REQUIRE(f.pump([&] { return f.call->finishPending(); }));
    CHECK(f.stream.state() == StreamState::Finishing);
    CHECK(f.stream.isOpen());

    f.call->completeFinish(grpc::Status(grpc::StatusCode::DATA_LOSS, "partial"));
    REQUIRE(f.pumpUntilClosed());
    CHECK(f.stream.state() == StreamState::Closed);
    CHECK(f.stream.status().error_code() == grpc::StatusCode::DATA_LOSS);
    CHECK(f.stream.status().error_message() == "partial");
    CHECK(f.call->finishCalls() == 1);
    f.checkNoViolations();
}

TEST_CASE("Finish waits for the start-call when the reads end before it completes", "[read-stream]")
{
    FakeReadFixture f;
    REQUIRE(f.open());

    f.call->endReads();
    REQUIRE(f.pump([&] { return f.stream.state() == StreamState::Finishing; }));
    CHECK_FALSE(f.call->finishPending());

    f.call->completeStart();
    REQUIRE(f.pump([&] { return f.call->finishPending(); }));
    f.call->completeFinish(grpc::Status::OK);
    REQUIRE(f.pumpUntilClosed());
    CHECK(f.stream.status().ok());
    f.checkNoViolations();
}

TEST_CASE("A failed start-call of a read stream reports the error", "[read-stream]")
{
    FakeReadFixture f;
    REQUIRE(f.open());

    f.call->completeStart(false);
    REQUIRE(f.pump([&] { return f.stream.state() == StreamState::Finishing; }));
    CHECK_FALSE(f.call->finishPending()); // the read is still outstanding

    f.call->endReads();
    REQUIRE(f.pump([&] { return f.call->finishPending(); }));
    f.call->completeFinish(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no connection"));
    REQUIRE(f.pumpUntilClosed());
    CHECK(f.responses.empty());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::UNAVAILABLE);
    f.checkNoViolations();
}

TEST_CASE("close() cancels an active read stream", "[read-stream]")
{
    FakeReadFixture f;
    f.openStarted();

    f.stream.close();
    CHECK_FALSE(f.stream.isOpen());
    CHECK(f.stream.state() == StreamState::Closed);
    CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
    CHECK_FALSE(f.stream.poll(f.handler));

    f.stream.close();
    CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
}

TEST_CASE("The response handler may close the read stream re-entrantly", "[read-stream]")
{
    FakeReadFixture f;
    f.openStarted();

    int  handled = 0;
    auto closing = [&](Response&&) {
        ++handled;
        f.stream.close();
    };
    f.call->deliver(makeResponse(1));
    pollUntil(f.stream, closing, [&] { return handled == 1; });

    CHECK(handled == 1);
    CHECK_FALSE(f.stream.isOpen());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
}

TEST_CASE("A read stream can be reopened", "[read-stream]")
{
    FakeReadFixture f;
    f.openStarted();

    auto second_call = std::make_shared<Call>();
    REQUIRE(f.stream.open(makeFakeReaderFactory(second_call, makeEcho(2))));
    CHECK(f.stream.isOpen());
    CHECK(f.stream.status().ok());
    CHECK(second_call->startPending());
    CHECK(second_call->readPending());
    CHECK(f.call.use_count() == 1); // the first fake stream has been destroyed
}

TEST_CASE("The read stream destructor releases an active call", "[read-stream]")
{
    auto call = std::make_shared<Call>();
    {
        ReadStream stream;
        REQUIRE(stream.open(makeFakeReaderFactory(call, makeEcho(1))));
    }
    CHECK(call.use_count() == 1);
}

// ---------------------------------------------------------------------------
// End-to-end with the in-process server
// ---------------------------------------------------------------------------

TEST_CASE("Subscribe delivers a burst of responses completely and in order", "[read-stream][integration]")
{
    SubscribeFixture f;
    REQUIRE(f.stream.open([&](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
        context->AddMetadata(std::string(metadata_key), "subscribe-metadata");
        return f.stub->AsyncSubscribe(context, makeBurst(100, 5000, std::string(64, 's')), cq, tag);
    }));

    REQUIRE(f.finish());
    CHECK(f.stream.status().ok());
    REQUIRE(f.responses.size() == 5000);
    for (std::uint32_t i = 0; i < 5000; ++i)
    {
        CHECK(f.responses[i].id() == 100 + i);
        CHECK(f.responses[i].server_seq() == i);
    }
    CHECK(f.responses.front().metadata_value() == "subscribe-metadata");
    CHECK(f.server.service().sentResponses() == 5000);
}

TEST_CASE("Subscribe with a single response or none ends with OK", "[read-stream][integration]")
{
    SubscribeFixture f;

    REQUIRE(f.open(makeEcho(9, "one")));
    REQUIRE(f.finish());
    CHECK(f.stream.status().ok());
    REQUIRE(f.responses.size() == 1);
    CHECK(f.responses.front().payload() == "one");

    REQUIRE(f.open(makeEnd()));
    REQUIRE(f.finish());
    CHECK(f.stream.status().ok());
    CHECK(f.responses.empty());
}

TEST_CASE("A Subscribe server error status is reported", "[read-stream][integration]")
{
    SubscribeFixture f;
    const auto       code = GENERATE(grpc::StatusCode::NOT_FOUND, grpc::StatusCode::PERMISSION_DENIED);

    REQUIRE(f.open(makeFail(code, "no such topic")));
    REQUIRE(f.finish());
    CHECK(f.stream.status().error_code() == code);
    CHECK(f.stream.status().error_message() == "no such topic");
    CHECK(f.responses.empty());
}

TEST_CASE("close() cancels a subscription on both sides", "[read-stream][integration]")
{
    SubscribeFixture f;
    REQUIRE(f.open(makeBurst(0, 10'000'000)));
    REQUIRE(f.pump([&] { return f.responses.size() >= 10; }));

    f.stream.close();
    CHECK_FALSE(f.stream.isOpen());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
    CHECK(waitFor([&] { return f.server.service().activeCalls() == 0; }));
    CHECK(f.server.service().cancelledCalls() == 1);
    CHECK(idsStrictlyIncrease(f.responses));
}

TEST_CASE("A deadline set in the factory ends a stalled subscription", "[read-stream][integration]")
{
    SubscribeFixture f;
    Request          slow = makeEcho(1);
    slow.set_delay_us(5'000'000);
    REQUIRE(f.stream.open([&](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
        context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
        return f.stub->AsyncSubscribe(context, slow, cq, tag);
    }));

    const auto started = std::chrono::steady_clock::now();
    REQUIRE(f.finish());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::DEADLINE_EXCEEDED);
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(4));
    CHECK(f.responses.empty());
}

TEST_CASE("A subscription to an unreachable server reports UNAVAILABLE", "[read-stream][integration]")
{
    auto channel = grpc::CreateChannel(std::string(unreachable_server_address), grpc::InsecureChannelCredentials());
    auto stub    = proto::TestStream::NewStub(channel);

    ReadStream stream;
    REQUIRE(stream.open([&](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
        context->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
        return stub->AsyncSubscribe(context, makeEcho(1), cq, tag);
    }));

    auto handler = [](Response&&) {};
    REQUIRE(pollUntilClosed(stream, handler, std::chrono::seconds(15)));
    CHECK(stream.status().error_code() == grpc::StatusCode::UNAVAILABLE);
}

TEST_CASE("A read stream object can run many subscriptions sequentially", "[read-stream][integration]")
{
    SubscribeFixture f;
    for (std::uint64_t call = 0; call < 30; ++call)
    {
        if (call % 2 == 0)
        {
            REQUIRE(f.open(makeBurst(call * 10, 3)));
            REQUIRE(f.finish());
            CHECK(f.stream.status().ok());
            CHECK(idsOf(f.responses) == std::vector<std::uint64_t>{call * 10, call * 10 + 1, call * 10 + 2});
        }
        else
        {
            // A burst the server cannot finish before the client closes: a short one
            // may be delivered completely, Finish included, by the poll that
            // receives its first response, leaving nothing to cancel.
            REQUIRE(f.open(makeBurst(call * 10, 10'000'000)));
            REQUIRE(f.pump([&] { return !f.responses.empty(); }));
            REQUIRE(f.stream.isOpen());
            f.stream.close();
            CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
        }
    }
    CHECK(waitFor([&] { return f.server.service().activeCalls() == 0; }));
}
