// Usage of the reusable test utilities (GrpcAsyncStream::TestUtils) the way a
// product's own tests would use them: only <grpc_async_stream/...> and
// <grpc_async_stream_test/...> are included, nothing from this repository's tests.
// The "product" message is a minimal stand-in for a generated protobuf type.

#include <grpc_async_stream_test/Catch2StringMakers.h>
#include <grpc_async_stream_test/EnumReflection.h>
#include <grpc_async_stream_test/FakeReaderWriter.h>
#include <grpc_async_stream_test/InProcessServer.h>
#include <grpc_async_stream_test/StreamTestUtils.h>
#include <grpc_async_stream/AsyncReadWriteStream.h>

#include <catch2/catch_test_macros.hpp>

#include <grpcpp/generic/callback_generic_service.h>
#include <grpcpp/generic/generic_stub.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <memory>
#include <vector>

namespace
{

// Stand-in for a product's generated message: what AsyncReadWriteStream needs
// (ProtobufMessage) plus the id() used by idsOf()
struct ProductMessage
{
    std::uint64_t value = 0;

    [[nodiscard]] std::uint64_t id() const { return value; }
    [[nodiscard]] std::size_t   ByteSizeLong() const { return sizeof(value); }
    void                        Clear() { value = 0; }
};

using ProductStream = grpc_async_stream::AsyncReadWriteStream<ProductMessage, ProductMessage>;
using ProductCall   = grpc_async_stream_test::FakeCall<ProductMessage, ProductMessage>;

static_assert(grpc_async_stream::ProtobufMessage<ProductMessage>);

// Generic (method-agnostic) callback service echoing every message back
class EchoGenericService final : public grpc::CallbackGenericService
{
public:
    [[nodiscard]] int calls() const { return m_calls; }

private:
    class Reactor final : public grpc::ServerGenericBidiReactor
    {
    public:
        Reactor() { StartRead(&m_buffer); }

        void OnReadDone(bool ok) override
        {
            if (ok)
                StartWrite(&m_buffer);
            else
                Finish(grpc::Status::OK); // client half-closed
        }
        void OnWriteDone(bool ok) override
        {
            if (ok)
                StartRead(&m_buffer);
            else
                Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, "write failed"));
        }
        void OnDone() override { delete this; }

    private:
        grpc::ByteBuffer m_buffer;
    };

    grpc::ServerGenericBidiReactor* CreateReactor(grpc::GenericCallbackServerContext*) override
    {
        ++m_calls;
        return new Reactor();
    }

    std::atomic<int> m_calls{0};
};

std::string toString(const grpc::ByteBuffer& buffer)
{
    std::vector<grpc::Slice> slices;
    if (!buffer.Dump(&slices).ok())
        return {};
    std::string text;
    for (const grpc::Slice& slice : slices)
        text.append(reinterpret_cast<const char*>(slice.begin()), slice.size());
    return text;
}

} // namespace

TEST_CASE("Test utilities drive a product stream through a fake call", "[test-utils]")
{
    auto          call = std::make_shared<ProductCall>();
    ProductStream stream(grpc_async_stream::WriteQueueLimits{
        .max_messages    = 2,
        .overflow_policy = grpc_async_stream::WriteOverflowPolicy::DropNewest,
    });
    std::vector<ProductMessage> received;
    auto handler = [&](ProductMessage&& message) { received.push_back(message); };

    REQUIRE(stream.open(grpc_async_stream_test::makeFakeStreamFactory(call)));
    CHECK(stream.state() == grpc_async_stream::StreamState::Open);

    // Until the start-call completes, requests are queued; the third one overflows
    CHECK(stream.sendRequest({.value = 1}) == grpc_async_stream::SendResult::Queued);
    CHECK(stream.sendRequest({.value = 2}) == grpc_async_stream::SendResult::Queued);
    CHECK(stream.sendRequest({.value = 3}) == grpc_async_stream::SendResult::Dropped);
    CHECK(grpc_async_stream_test::enumName(grpc_async_stream::SendResult::Dropped) == "Dropped");

    call->completeStart();
    for (std::size_t written = 1; written <= 2; ++written)
    {
        REQUIRE(grpc_async_stream_test::pollUntil(stream, handler, [&] { return call->writePending(); }));
        call->completeWrite();
        REQUIRE(grpc_async_stream_test::pollUntil(stream, handler,
                                                  [&] { return stream.writeQueueStats().written_messages == written; }));
    }
    CHECK(grpc_async_stream_test::idsOf(call->writes()) == std::vector<std::uint64_t>{1, 2});

    // Server messages arrive one read at a time
    for (const std::uint64_t value : {10U, 11U})
    {
        call->deliver({.value = value});
        REQUIRE(grpc_async_stream_test::pollUntil(stream, handler,
                                                  [&] { return !received.empty() && received.back().value == value; }));
    }
    CHECK(grpc_async_stream_test::idsOf(received) == std::vector<std::uint64_t>{10, 11});
    CHECK(grpc_async_stream_test::idsStrictlyIncrease(received));

    call->endReads();
    REQUIRE(grpc_async_stream_test::pollUntil(stream, handler, [&] { return call->finishPending(); }));
    call->completeFinish(grpc::Status(grpc::StatusCode::ABORTED, "done"));
    REQUIRE(grpc_async_stream_test::pollUntilClosed(stream, handler));
    CHECK(stream.status().error_code() == grpc::StatusCode::ABORTED);
    CHECK(call->violations().empty()); // the stream used the gRPC async API correctly
    CHECK(grpc_async_stream_test::statusCodeName(grpc::StatusCode::ABORTED) == "ABORTED (10)");
}

TEST_CASE("InProcessServer serves calls without network ports", "[test-utils]")
{
    // A product passes its generated service: InProcessServer server(service).
    // Here a generic echo service (no generated code) is registered through the
    // configure hook and called through a generic stub.
    EchoGenericService                      echo_service;
    grpc_async_stream_test::InProcessServer server({}, [&](grpc::ServerBuilder& builder) {
        builder.RegisterCallbackGenericService(&echo_service);
    });

    grpc::GenericStub     stub(server.channel());
    grpc::CompletionQueue queue;
    grpc::ClientContext   context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

    grpc::Slice      request_slice(std::string("ping"));
    grpc::ByteBuffer request(&request_slice, 1);
    grpc::ByteBuffer response;
    grpc::Status     status;

    auto call = stub.PrepareUnaryCall(&context, "/product.Echo/Call", request, &queue);
    call->StartCall();
    call->Finish(&response, &status, &status);

    void* tag = nullptr;
    bool  ok  = false;
    REQUIRE(queue.Next(&tag, &ok));
    CHECK(tag == &status);
    CHECK(ok);
    INFO(status.error_message());
    CHECK(status.error_code() == grpc::StatusCode::OK);
    CHECK(toString(response) == "ping");
    CHECK(echo_service.calls() == 1);

    queue.Shutdown();
    while (queue.Next(&tag, &ok))
    {
    }

    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(1024);
    CHECK(server.makeChannel(args) != nullptr);

    server.shutdown();
    server.shutdown(); // idempotent
}
