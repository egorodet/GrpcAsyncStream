// End-to-end tests of AsyncReadWriteStream against a real gRPC server running
// in-process (see support/TestServer.h).

#include "support/StreamTestUtils.h"
#include "support/TestServer.h"

#include <AsyncReadWriteStream.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <algorithm>
#include <functional>
#include <ranges>
#include <string>
#include <thread>
#include <vector>

namespace
{

using namespace TestSupport;
using grpc_utils::SendResult;
using grpc_utils::WriteOverflowPolicy;
using grpc_utils::WriteQueueLimits;

using Stream = grpc_utils::AsyncReadWriteStream<Request, Response>;

struct ServerFixture
{
    TestServer                               server;
    std::unique_ptr<proto::TestStream::Stub> stub = server.makeStub();
    Stream                                   stream;
    std::vector<Response>                    responses;
    std::function<void(Response&&)>          handler = [this](Response&& response) {
        responses.push_back(std::move(response));
    };

    bool open()
    {
        return stream.open([this](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
            return stub->AsyncChat(context, cq, tag);
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

TEST_CASE("Echo round-trip preserves order and payloads", "[integration]")
{
    ServerFixture f;
    REQUIRE(f.open());

    constexpr std::uint64_t count = 1000;
    for (std::uint64_t id = 0; id < count; ++id)
    {
        // Requests sent right after open() are queued until the start-call completes
        const SendResult result = f.stream.sendRequest(makeEcho(id, "payload-" + std::to_string(id)));
        REQUIRE(grpc_utils::isAccepted(result));
    }

    REQUIRE(f.pump([&] { return f.responses.size() == count; }));
    CHECK(std::ranges::equal(idsOf(f.responses), std::views::iota(std::uint64_t{0}, count)));
    for (std::uint64_t id = 0; id < count; ++id)
    {
        CHECK(f.responses[id].payload() == "payload-" + std::to_string(id));
        CHECK(f.responses[id].server_seq() == id);
    }

    REQUIRE(f.stream.writesDone());
    REQUIRE(f.finish());
    CHECK(f.stream.status().ok());

    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.submitted_messages == count);
    CHECK(stats.written_messages == count);
    CHECK(stats.dropped_messages == 0);
    CHECK(stats.discarded_messages == 0);
    CHECK(f.server.service().receivedRequests() == count);
}

TEST_CASE("Ping-pong: the handler sends the next request for each response", "[integration]")
{
    ServerFixture f;
    REQUIRE(f.open());

    constexpr std::uint64_t rounds   = 200;
    std::uint64_t           received = 0;
    auto                    ping     = [&](Response&& response) {
        CHECK(response.id() == received);
        ++received;
        if (received < rounds)
            CHECK(grpc_utils::isAccepted(f.stream.sendRequest(makeEcho(received))));
        else
            CHECK(f.stream.writesDone());
    };

    REQUIRE(grpc_utils::isAccepted(f.stream.sendRequest(makeEcho(0))));
    REQUIRE(pollUntilClosed(f.stream, ping, std::chrono::seconds(20)));
    CHECK(received == rounds);
    CHECK(f.stream.status().ok());
}

TEST_CASE("Client metadata configured by the factory reaches the server", "[integration]")
{
    ServerFixture f;
    REQUIRE(f.stream.open([&](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
        context->AddMetadata(std::string(metadata_key), "hello-metadata");
        return f.stub->AsyncChat(context, cq, tag);
    }));

    f.stream.sendRequest(makeEcho(1));
    REQUIRE(f.pump([&] { return f.responses.size() == 1; }));
    CHECK(f.responses.front().metadata_value() == "hello-metadata");
}

TEST_CASE("A burst of server responses is delivered completely and in order", "[integration]")
{
    ServerFixture f;
    REQUIRE(f.open());

    constexpr std::uint32_t burst = 5000;
    f.stream.sendRequest(makeBurst(100, burst, std::string(64, 'b')));
    REQUIRE(f.pump([&] { return f.responses.size() == burst; }));
    for (std::uint32_t i = 0; i < burst; ++i)
    {
        CHECK(f.responses[i].id() == 100 + i);
        CHECK(f.responses[i].server_seq() == i);
    }
    CHECK(f.server.service().sentResponses() == burst);
}

TEST_CASE("A server error status is reported after the stream ends", "[integration]")
{
    ServerFixture f;
    REQUIRE(f.open());

    const auto code = GENERATE(grpc::StatusCode::INVALID_ARGUMENT, grpc::StatusCode::PERMISSION_DENIED,
                               grpc::StatusCode::RESOURCE_EXHAUSTED, grpc::StatusCode::INTERNAL);

    f.stream.sendRequest(makeEcho(1));
    f.stream.sendRequest(makeFail(code, "server says no"));
    REQUIRE(f.finish());

    CHECK(f.responses.size() == 1);
    CHECK(f.stream.status().error_code() == code);
    CHECK(f.stream.status().error_message() == "server says no");
    CHECK_FALSE(f.stream.canWrite());
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::NotWritable);
}

TEST_CASE("The server may end the call before the client half-closes", "[integration]")
{
    ServerFixture f;
    REQUIRE(f.open());

    f.stream.sendRequest(makeEcho(1));
    f.stream.sendRequest(makeEnd());
    REQUIRE(f.finish());
    CHECK(f.stream.status().ok());
    CHECK(f.responses.size() == 1);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::NotWritable);
}

TEST_CASE("close() cancels the call on both sides", "[integration]")
{
    ServerFixture f;
    REQUIRE(f.open());

    f.stream.sendRequest(makeEcho(1));
    REQUIRE(f.pump([&] { return f.responses.size() == 1; }));
    REQUIRE(waitFor([&] { return f.server.service().activeCalls() == 1; }));

    f.stream.close();
    CHECK_FALSE(f.stream.isOpen());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
    CHECK(waitFor([&] { return f.server.service().activeCalls() == 0; }));
    CHECK(f.server.service().cancelledCalls() == 1);
}

TEST_CASE("A deadline set in the factory ends a stalled call", "[integration]")
{
    ServerFixture f;
    REQUIRE(f.stream.open([&](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
        context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
        return f.stub->AsyncChat(context, cq, tag);
    }));

    Request slow = makeEcho(1);
    slow.set_delay_us(5'000'000);
    f.stream.sendRequest(slow);

    const auto started = std::chrono::steady_clock::now();
    REQUIRE(f.finish());
    CHECK(f.stream.status().error_code() == grpc::StatusCode::DEADLINE_EXCEEDED);
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(4));
}

TEST_CASE("An unreachable server reports UNAVAILABLE", "[integration]")
{
    auto channel = grpc::CreateChannel(std::string(unreachable_server_address), grpc::InsecureChannelCredentials());
    auto   stub    = proto::TestStream::NewStub(channel);
    Stream stream;
    REQUIRE(stream.open([&](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
        context->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
        return stub->AsyncChat(context, cq, tag);
    }));
    stream.sendRequest(makeEcho(1));

    auto handler = [](Response&&) {};
    REQUIRE(pollUntilClosed(stream, handler, std::chrono::seconds(15)));
    CHECK(stream.status().error_code() == grpc::StatusCode::UNAVAILABLE);
    CHECK(stream.writeQueueStats().written_messages == 0);
}

TEST_CASE("A stream object can run many calls sequentially", "[integration]")
{
    ServerFixture f;
    for (std::uint64_t call = 0; call < 50; ++call)
    {
        f.responses.clear();
        REQUIRE(f.open());
        f.stream.sendRequest(makeEcho(call));
        REQUIRE(f.pump([&] { return f.responses.size() == 1; }));
        CHECK(f.responses.front().id() == call);

        if (call % 2 == 0)
        {
            REQUIRE(f.stream.writesDone());
            REQUIRE(f.finish());
            CHECK(f.stream.status().ok());
        }
        else
        {
            f.stream.close();
            CHECK(f.stream.status().error_code() == grpc::StatusCode::CANCELLED);
        }
    }
    CHECK(waitFor([&] { return f.server.service().activeCalls() == 0; }));
}

TEST_CASE("A slow server makes the bounded queue drop excess requests", "[integration][load]")
{
    const auto policy = GENERATE(WriteOverflowPolicy::DropNewest, WriteOverflowPolicy::DropOldest);

    ServerFixture f;
    f.server.service().setReadDelay(std::chrono::microseconds(100));

    constexpr std::size_t  max_bytes = 32 * 1024;
    const WriteQueueLimits limits{.max_messages = 64, .max_bytes = max_bytes, .overflow_policy = policy};
    f.stream.setWriteQueueLimits(limits);
    REQUIRE(f.open());

    const std::string       payload(1024, 'L');
    constexpr std::uint64_t total = 20000;
    std::size_t             max_observed_queue = 0;
    std::size_t             max_observed_bytes = 0;

    for (std::uint64_t id = 0; id < total; ++id)
    {
        const SendResult result = f.stream.sendRequest(makeEcho(id, payload));
        REQUIRE(result != SendResult::NotWritable);
        max_observed_queue = std::max(max_observed_queue, f.stream.writeQueueStats().queued_messages);
        max_observed_bytes = std::max(max_observed_bytes, f.stream.writeQueueStats().queued_bytes);
        if (id % 64 == 0)
            f.stream.poll(f.handler);
    }

    REQUIRE(f.stream.writesDone());
    REQUIRE(f.finish(std::chrono::seconds(60)));
    CHECK(f.stream.status().ok());

    const auto& stats = f.stream.writeQueueStats();
    INFO("written=" << stats.written_messages << " dropped=" << stats.dropped_messages);
    CHECK(max_observed_queue <= limits.max_messages);
    CHECK(max_observed_bytes <= limits.max_bytes);
    CHECK(stats.peak_queued_bytes <= limits.max_bytes);
    CHECK(stats.dropped_messages > 0);
    CHECK(stats.discarded_messages == 0);
    CHECK(stats.submitted_messages == total);
    CHECK(stats.written_messages + stats.dropped_messages == total);

    // Everything that was written has been received by the server and echoed back
    CHECK(f.server.service().receivedRequests() == stats.written_messages);
    CHECK(f.responses.size() == stats.written_messages);

    // Echoes arrive in order of the written requests
    CHECK(idsStrictlyIncrease(f.responses));
    CHECK(stats.isConsistent());
}
