// Tests of AsyncWriteStream, the wrapper over client-streaming calls: the state
// machine driven through a fake ClientAsyncWriterInterface (see
// grpc_async_stream_test/FakeReaderWriter.h), and end-to-end calls of the Upload
// method of the in-process test server (see support/TestServer.h).

#include "support/TestServer.h"

#include <grpc_async_stream_test/Catch2StringMakers.h>
#include <grpc_async_stream_test/EnumReflection.h>
#include <grpc_async_stream_test/FakeReaderWriter.h>
#include <grpc_async_stream_test/InProcessServer.h>
#include <grpc_async_stream_test/StreamTestUtils.h>
#include <grpc_async_stream/AsyncReadWriteStream.h>
#include <grpc_async_stream/AsyncWriteStream.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/generators/catch_generators_range.hpp>

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
using namespace grpc_async_stream_test;
using namespace Catch::Generators;
using grpc_async_stream::SendResult;
using grpc_async_stream::StreamState;
using grpc_async_stream::WriteOverflowPolicy;
using grpc_async_stream::WriteQueueLimits;

using WriteStream = grpc_async_stream::AsyncWriteStream<Request>;
using Call        = FakeCall<Request, UploadSummary>;

UploadSummary makeSummary(std::uint64_t received_count)
{
    UploadSummary summary;
    summary.set_received_count(received_count);
    return summary;
}

struct FakeWriteFixture
{
    std::shared_ptr<Call> call = std::make_shared<Call>();
    UploadSummary         summary; // declared before the stream: must outlive the call
    WriteStream           stream;

    bool open() { return stream.open(makeFakeWriterFactory(call, &summary)); }

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
        return pollUntil(stream, std::forward<Predicate>(predicate), std::chrono::seconds(5));
    }

    bool pumpUntilClosed() { return pollUntilClosed(stream, std::chrono::seconds(5)); }

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

struct UploadFixture
{
    TestServer                               server;
    std::unique_ptr<proto::TestStream::Stub> stub = server.makeStub();
    UploadSummary                            summary; // declared before the stream: must outlive the call
    WriteStream                              stream;

    bool open()
    {
        summary.Clear();
        return stream.open([this](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
            return stub->AsyncUpload(context, &summary, cq, tag);
        });
    }

    bool finish(std::chrono::milliseconds timeout = std::chrono::seconds(20))
    {
        return pollUntilClosed(stream, timeout);
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
// Class structure and concepts
// ---------------------------------------------------------------------------

TEST_CASE("AsyncReadWriteStream is an AsyncWriteStream over the bidirectional interface", "[write-stream][concepts]")
{
    using grpc_async_stream::AsyncStreamFactoryFor;
    using grpc_async_stream::AsyncWriterInterfaceFor;
    using ReadWriteStream = grpc_async_stream::AsyncReadWriteStream<Request, Response>;
    using BidiInterface   = grpc::ClientAsyncReaderWriterInterface<Request, Response>;
    using BidiCore        = grpc_async_stream::detail::AsyncCallBase<BidiInterface>;

    static_assert(std::is_base_of_v<grpc_async_stream::AsyncWriteStream<Request, BidiCore>, ReadWriteStream>);
    static_assert(std::is_same_v<WriteStream::AsyncStreamInterface, grpc::ClientAsyncWriterInterface<Request>>);
    static_assert(std::is_base_of_v<grpc_async_stream::detail::AsyncCallBase<grpc::ClientAsyncWriterInterface<Request>>,
                                    WriteStream>);

    // Both gRPC client stream interfaces with a write side qualify, read-only ones do not
    static_assert(AsyncWriterInterfaceFor<grpc::ClientAsyncWriterInterface<Request>, Request>);
    static_assert(AsyncWriterInterfaceFor<BidiInterface, Request>);
    static_assert(!AsyncWriterInterfaceFor<grpc::ClientAsyncReaderInterface<Response>, Request>);
    static_assert(!AsyncWriterInterfaceFor<grpc::ClientAsyncWriterInterface<Response>, Request>);

    using WriterFactory = decltype(makeFakeWriterFactory(std::shared_ptr<Call>(), static_cast<UploadSummary*>(nullptr)));
    using BidiFactory   = decltype(makeFakeStreamFactory(std::shared_ptr<FakeCall<Request, Response>>()));
    static_assert(AsyncStreamFactoryFor<WriterFactory, grpc::ClientAsyncWriterInterface<Request>>);
    static_assert(!AsyncStreamFactoryFor<BidiFactory, grpc::ClientAsyncWriterInterface<Request>>);
    static_assert(AsyncStreamFactoryFor<BidiFactory, BidiInterface>);
    static_assert(!AsyncStreamFactoryFor<WriterFactory, BidiInterface>);

    // A write-only stream is polled without a handler; the bidirectional stream
    // hides that overload, so its responses cannot be lost by mistake
    static_assert(PollableWithoutHandler<WriteStream>);
    static_assert(!PollableWithoutHandler<ReadWriteStream>);
    static_assert(PollableWithHandler<ReadWriteStream, std::function<void(Response&&)>>);
    static_assert(!PollableWithHandler<WriteStream, std::function<void(Response&&)>>);

    SUCCEED("checked at compile time");
}

// ---------------------------------------------------------------------------
// State machine with a fake call
// ---------------------------------------------------------------------------

TEST_CASE("A write stream is closed until opened", "[write-stream]")
{
    WriteStream stream;
    CHECK(stream.state() == StreamState::Closed);
    CHECK_FALSE(stream.isOpen());
    CHECK_FALSE(stream.canWrite());
    CHECK(stream.getStream() == nullptr);
    CHECK(stream.status().ok());
    CHECK(stream.sendRequest(makeEcho(1)) == SendResult::NotWritable);
    CHECK_FALSE(stream.writesDone());
    CHECK_FALSE(stream.poll());
    CHECK_FALSE(stream.pollFor(std::chrono::milliseconds(1)));
    CHECK(stream.writeQueueStats().submitted_messages == 0);
}

TEST_CASE("Write stream open() fails when the factory returns no stream", "[write-stream]")
{
    WriteStream stream;
    const bool  opened = stream.open([](grpc::ClientContext*, grpc::CompletionQueue*, void*) {
        return std::unique_ptr<WriteStream::AsyncStreamInterface>();
    });
    CHECK_FALSE(opened);
    CHECK_FALSE(stream.isOpen());
    CHECK(stream.status().error_code() == grpc::StatusCode::INTERNAL);
}

TEST_CASE("A write stream starts no reads and queues requests until the start-call completes", "[write-stream]")
{
    FakeWriteFixture f;
    REQUIRE(f.open());

    CHECK(f.stream.isOpen());
    CHECK(f.stream.canWrite());
    CHECK(f.stream.state() == StreamState::Open);
    CHECK(f.stream.getStream() != nullptr);
    CHECK(f.call->startPending());
    CHECK_FALSE(f.call->readPending());
    CHECK(f.stream.isWriteInProgress());

    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);
    CHECK(f.call->writes().empty());

    f.call->completeStart();
    f.completeWrites(2);
    CHECK(f.writtenIds() == std::vector<std::uint64_t>{1, 2});

    // With the write slot free, the next request is written immediately
    CHECK(f.stream.sendRequest(makeEcho(3)) == SendResult::Written);
    f.completeWrites(1);
    CHECK(f.writtenIds() == std::vector<std::uint64_t>{1, 2, 3});
    CHECK(f.stream.writeQueueStats().written_messages == 3);
    CHECK(f.stream.writeQueueStats().isConsistent());
    f.checkNoViolations();
}

TEST_CASE("poll() and pollFor() of an idle write stream keep it open", "[write-stream]")
{
    FakeWriteFixture f;
    f.openStarted();
    for (int i = 0; i < 100; ++i)
        CHECK(f.stream.poll());

    const auto started = std::chrono::steady_clock::now();
    CHECK(f.stream.pollFor(std::chrono::milliseconds(20)));
    CHECK(std::chrono::steady_clock::now() - started >= std::chrono::milliseconds(10));
    CHECK(f.stream.isOpen());
    f.checkNoViolations();
}

TEST_CASE("poll() of an idle write stream does not wait for the completion queue", "[write-stream][integration]")
{
    // With no operation outstanding no event can arrive, so poll() returns at
    // once. Asking the completion queue anyway would take up to the timer slack
    // (50 us by default on Linux) per call here, because the server's threads
    // poll completion queues of the same process.
    UploadFixture f;
    REQUIRE(f.open());
    REQUIRE(pollUntil(f.stream, [&] { return !f.stream.isWriteInProgress(); }));

    bool       open    = true;
    const auto started = std::chrono::steady_clock::now();
    for (int i = 0; i < 1000; ++i)
        open = f.stream.poll() && open;
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::milliseconds(25));
    CHECK(open);

    REQUIRE(f.stream.writesDone());
    REQUIRE(f.finish());
    CHECK(f.stream.status().ok());
}

TEST_CASE("writesDone() flushes the queue, half-closes and then requests the response", "[write-stream]")
{
    FakeWriteFixture f;
    REQUIRE(f.open());
    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);

    CHECK(f.stream.writesDone());
    CHECK(f.stream.state() == StreamState::HalfClosing);
    CHECK_FALSE(f.stream.canWrite());
    CHECK_FALSE(f.stream.writesDone());
    CHECK(f.stream.sendRequest(makeEcho(3)) == SendResult::NotWritable);

    f.call->completeStart();
    f.completeWrites(2);
    REQUIRE(f.pump([&] { return f.call->writesDonePending(); }));
    CHECK_FALSE(f.call->finishPending());

    // Without a read side, nothing is left to wait for after the half-close
    f.call->completeWritesDone();
    REQUIRE(f.pump([&] { return f.call->finishPending(); }));
    CHECK(f.stream.state() == StreamState::Finishing);
    CHECK(f.stream.isOpen());

    f.call->completeFinish(grpc::Status::OK, makeSummary(2));
    REQUIRE(f.pumpUntilClosed());

    CHECK(f.stream.status().ok());
    CHECK(f.stream.state() == StreamState::Closed);
    CHECK(f.summary.received_count() == 2);
    CHECK(f.writtenIds() == std::vector<std::uint64_t>{1, 2});
    CHECK(f.call->finishCalls() == 1);
    CHECK(f.stream.writeQueueStats().isConsistent());
    f.checkNoViolations();
}

TEST_CASE("writesDone() on an idle write stream is issued immediately", "[write-stream]")
{
    FakeWriteFixture f;
    f.openStarted();
    CHECK(f.stream.writesDone());
    CHECK(f.call->writesDonePending());

    f.call->completeWritesDone();
    REQUIRE(f.pump([&] { return f.call->finishPending(); }));
    f.call->completeFinish(grpc::Status(grpc::StatusCode::ABORTED, "rejected"));
    REQUIRE(f.pumpUntilClosed());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::ABORTED);
    CHECK(f.stream.status().error_message() == "rejected");
    f.checkNoViolations();
}

TEST_CASE("A failed write of a write stream requests the final status right away", "[write-stream]")
{
    FakeWriteFixture f;
    f.openStarted();
    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Written);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(3)) == SendResult::Queued);

    f.call->completeWrite(false);
    REQUIRE(f.pump([&] { return f.call->finishPending(); }));
    CHECK(f.stream.state() == StreamState::Finishing);
    CHECK_FALSE(f.stream.canWrite());
    CHECK(f.stream.sendRequest(makeEcho(4)) == SendResult::NotWritable);
    CHECK_FALSE(f.call->writesDoneCalled());

    f.call->completeFinish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "bad request"));
    REQUIRE(f.pumpUntilClosed());

    CHECK(f.stream.status().error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.discarded_messages == 3); // the failed write and the two queued requests
    CHECK(stats.written_messages == 0);
    CHECK(stats.isConsistent());
    f.checkNoViolations();
}

TEST_CASE("A failed start-call of a write stream reports the error", "[write-stream]")
{
    FakeWriteFixture f;
    REQUIRE(f.open());
    for (std::uint64_t id = 1; id <= 3; ++id)
        CHECK(f.stream.sendRequest(makeEcho(id)) == SendResult::Queued);

    f.call->completeStart(false);
    REQUIRE(f.pump([&] { return f.call->finishPending(); }));
    CHECK(f.stream.writeQueueStats().discarded_messages == 3);

    f.call->completeFinish(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no connection"));
    REQUIRE(f.pumpUntilClosed());
    CHECK(f.call->writes().empty());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::UNAVAILABLE);
    f.checkNoViolations();
}

TEST_CASE("close() cancels an active write stream and discards the queued requests", "[write-stream]")
{
    FakeWriteFixture f;
    f.openStarted();
    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Written);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);

    f.stream.close();

    CHECK_FALSE(f.stream.isOpen());
    CHECK(f.stream.state() == StreamState::Closed);
    CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
    CHECK_FALSE(f.stream.poll());
    CHECK(f.stream.sendRequest(makeEcho(3)) == SendResult::NotWritable);

    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.discarded_messages == 2);
    CHECK_FALSE(stats.write_in_flight);
    CHECK(stats.isConsistent());
}

TEST_CASE("The bounded write queue applies to write streams", "[write-stream]")
{
    FakeWriteFixture f;
    f.stream.setWriteQueueLimits({.max_messages = 2, .overflow_policy = WriteOverflowPolicy::DropOldest});
    REQUIRE(f.open());

    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(3)) == SendResult::QueuedDroppedOldest);
    CHECK(f.stream.sendRequest(makeEcho(4)) == SendResult::QueuedDroppedOldest);

    f.stream.setWriteQueueLimits({.max_messages = 1, .overflow_policy = WriteOverflowPolicy::DropNewest});
    CHECK(f.stream.writeQueueStats().queued_messages == 1);
    CHECK(f.stream.sendRequest(makeEcho(5)) == SendResult::Dropped);

    f.call->completeStart();
    f.completeWrites(1);
    CHECK(f.writtenIds() == std::vector<std::uint64_t>{3});

    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.dropped_messages == 4);
    CHECK(stats.peak_queued_messages == 2);
    CHECK(stats.isConsistent());
    f.checkNoViolations();
}

TEST_CASE("A write stream can be reopened and its statistics are reset", "[write-stream]")
{
    FakeWriteFixture f;
    f.openStarted();
    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Written);

    auto second_call = std::make_shared<Call>();
    REQUIRE(f.stream.open(makeFakeWriterFactory(second_call, &f.summary)));
    CHECK(f.stream.isOpen());
    CHECK(f.stream.status().ok());
    CHECK(f.stream.writeQueueStats().submitted_messages == 0);
    CHECK(second_call->startPending());
    CHECK(f.call.use_count() == 1); // the first fake stream has been destroyed
}

TEST_CASE("The write stream destructor releases an active call", "[write-stream]")
{
    auto          call = std::make_shared<Call>();
    UploadSummary summary;
    {
        WriteStream stream;
        REQUIRE(stream.open(makeFakeWriterFactory(call, &summary)));
        for (std::uint64_t id = 0; id < 100; ++id)
            stream.sendRequest(makeEcho(id, std::string(1000, 'x')));
    }
    CHECK(call.use_count() == 1);
}

// ---------------------------------------------------------------------------
// End-to-end with the in-process server
// ---------------------------------------------------------------------------

TEST_CASE("Upload delivers all requests in order and receives the response", "[write-stream][integration]")
{
    UploadFixture f;
    REQUIRE(f.stream.open([&](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
        context->AddMetadata(std::string(metadata_key), "upload-metadata");
        return f.stub->AsyncUpload(context, &f.summary, cq, tag);
    }));

    constexpr std::uint64_t count = 1000;
    for (std::uint64_t id = 0; id < count; ++id)
    {
        REQUIRE(grpc_async_stream::isAccepted(f.stream.sendRequest(makeEcho(id, "payload"))));
        if (id % 16 == 0)
            f.stream.poll();
    }
    REQUIRE(f.stream.writesDone());
    REQUIRE(f.finish());

    CHECK(f.stream.status().ok());
    CHECK(f.summary.received_count() == count);
    CHECK(std::ranges::equal(f.summary.ids(), std::views::iota(std::uint64_t{0}, count)));
    CHECK(f.summary.payload_bytes() == count * std::string("payload").size());
    CHECK(f.summary.metadata_value() == "upload-metadata");

    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.written_messages == count);
    CHECK(stats.dropped_messages == 0);
    CHECK(stats.discarded_messages == 0);
    CHECK(f.server.service().receivedRequests() == count);
}

TEST_CASE("An upload without requests receives an empty response", "[write-stream][integration]")
{
    UploadFixture f;
    REQUIRE(f.open());
    REQUIRE(f.stream.writesDone());
    REQUIRE(f.finish());
    CHECK(f.stream.status().ok());
    CHECK(f.summary.received_count() == 0);
}

TEST_CASE("An upload server error status is reported", "[write-stream][integration]")
{
    UploadFixture f;
    REQUIRE(f.open());

    const auto code = GENERATE(grpc::StatusCode::INVALID_ARGUMENT, grpc::StatusCode::RESOURCE_EXHAUSTED);

    f.stream.sendRequest(makeEcho(1));
    f.stream.sendRequest(makeFail(code, "upload rejected"));
    // Polling sends the queued requests (and keeps the stream open until the client acts)
    REQUIRE(pollUntil(f.stream, [&] { return f.server.service().finishedCalls() == 1; }));

    // The client notices the end of the call with its next write or the half-close
    f.stream.writesDone();
    REQUIRE(f.finish());
    CHECK(f.stream.status().error_code() == code);
    CHECK(f.stream.status().error_message() == "upload rejected");
    CHECK_FALSE(f.stream.canWrite());
}

TEST_CASE("The upload server may reply before the client half-closes", "[write-stream][integration]")
{
    UploadFixture f;
    REQUIRE(f.open());

    f.stream.sendRequest(makeEcho(1));
    f.stream.sendRequest(makeEnd());
    REQUIRE(pollUntil(f.stream, [&] { return f.server.service().finishedCalls() == 1; }));

    // The client learns about the end of the call with the half-close. No request
    // is written after the server reply here: over the in-process transport, a
    // write issued after the server has returned stays pending until the client
    // requests the status, which the gRPC API allows only after the half-close
    // (over TCP, such writes complete and the call ends with the server status).
    CHECK(f.stream.canWrite());
    REQUIRE(f.stream.writesDone());
    REQUIRE(f.finish());

    CHECK(f.stream.status().ok());
    CHECK(f.summary.received_count() == 2);
    CHECK(f.server.service().receivedRequests() == 2);
}

TEST_CASE("close() cancels an upload on both sides", "[write-stream][integration]")
{
    UploadFixture f;
    REQUIRE(f.open());

    f.stream.sendRequest(makeEcho(1));
    REQUIRE(waitFor([&] {
        f.stream.poll();
        return f.server.service().receivedRequests() == 1;
    }));

    f.stream.close();
    CHECK_FALSE(f.stream.isOpen());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
    CHECK(waitFor([&] { return f.server.service().activeCalls() == 0; }));
    CHECK(f.server.service().cancelledCalls() == 1);
}

TEST_CASE("A deadline set in the factory ends a stalled upload", "[write-stream][integration]")
{
    UploadFixture f;
    REQUIRE(f.stream.open([&](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
        context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
        return f.stub->AsyncUpload(context, &f.summary, cq, tag);
    }));

    Request slow = makeEcho(1);
    slow.set_delay_us(5'000'000);
    f.stream.sendRequest(slow);
    REQUIRE(f.stream.writesDone());

    const auto started = std::chrono::steady_clock::now();
    REQUIRE(f.finish());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::DEADLINE_EXCEEDED);
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(4));
}

TEST_CASE("An upload to an unreachable server reports UNAVAILABLE", "[write-stream][integration]")
{
    auto channel = grpc::CreateChannel(std::string(unreachable_server_address), grpc::InsecureChannelCredentials());
    auto stub    = proto::TestStream::NewStub(channel);

    UploadSummary summary;
    WriteStream   stream;
    REQUIRE(stream.open([&](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
        context->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
        return stub->AsyncUpload(context, &summary, cq, tag);
    }));
    stream.sendRequest(makeEcho(1));
    stream.writesDone();

    REQUIRE(pollUntilClosed(stream, std::chrono::seconds(15)));
    CHECK(stream.status().error_code() == grpc::StatusCode::UNAVAILABLE);
    CHECK(stream.writeQueueStats().written_messages == 0);
}

TEST_CASE("A write stream object can run many uploads sequentially", "[write-stream][integration]")
{
    UploadFixture f;
    for (std::uint64_t call = 0; call < 30; ++call)
    {
        REQUIRE(f.open());
        f.stream.sendRequest(makeEcho(call));
        if (call % 2 == 0)
        {
            REQUIRE(f.stream.writesDone());
            REQUIRE(f.finish());
            CHECK(f.stream.status().ok());
            CHECK(f.summary.ids_size() == 1);
        }
        else
        {
            f.stream.close();
            CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
        }
    }
    CHECK(waitFor([&] { return f.server.service().activeCalls() == 0; }));
}

TEST_CASE("A slow upload server makes the bounded queue drop excess requests", "[write-stream][integration][load]")
{
    const auto policy = GENERATE(from_range(magic_enum::enum_values<WriteOverflowPolicy>()));

    UploadFixture f;
    f.server.service().setReadDelay(std::chrono::microseconds(100));

    const WriteQueueLimits limits{.max_messages = 64, .max_bytes = 32 * 1024, .overflow_policy = policy};
    f.stream.setWriteQueueLimits(limits);
    REQUIRE(f.open());

    const std::string       payload(1024, 'U');
    constexpr std::uint64_t total = 20000;
    for (std::uint64_t id = 0; id < total; ++id)
    {
        REQUIRE(f.stream.sendRequest(makeEcho(id, payload)) != SendResult::NotWritable);
        CHECK(f.stream.writeQueueStats().queued_messages <= limits.max_messages);
        if (id % 64 == 0)
            f.stream.poll();
    }
    REQUIRE(f.stream.writesDone());
    REQUIRE(f.finish(std::chrono::seconds(60)));
    CHECK(f.stream.status().ok());

    const auto& stats = f.stream.writeQueueStats();
    INFO("written=" << stats.written_messages << " dropped=" << stats.dropped_messages);
    CHECK(stats.peak_queued_bytes <= limits.max_bytes);
    CHECK(stats.dropped_messages > 0);
    CHECK(stats.discarded_messages == 0);
    CHECK(stats.written_messages + stats.dropped_messages == total);
    CHECK(stats.isConsistent());

    // Everything written has been received by the server, in order
    CHECK(f.summary.received_count() == stats.written_messages);
    CHECK(std::ranges::adjacent_find(f.summary.ids(), std::ranges::greater_equal{}) == f.summary.ids().end());
}

