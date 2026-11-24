#pragma once

// Requires C++20 (concepts, designated initializers, defaulted comparisons).
// Depends only on gRPC and the C++ standard library.

#include <grpc_utils/AsyncWriteStream.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>

#include <chrono>
#include <concepts>
#include <cstddef>
#include <memory>
#include <utility>

namespace grpc_utils
{

// ---------------------------------------------------------------------------
// Concepts
// ---------------------------------------------------------------------------

// The callable passed to AsyncReadWriteStream::open(): creates the call with an
// Async* stub method, e.g. [&](auto* ctx, auto* cq, void* tag) { return stub->AsyncChat(ctx, cq, tag); }
template<typename Factory, typename RequestProto, typename ResponseProto>
concept StreamFactoryFor =
    WriteStreamFactoryFor<Factory, grpc::ClientAsyncReaderWriterInterface<RequestProto, ResponseProto>>;

// The callable receiving responses in AsyncReadWriteStream::poll() / pollFor()
template<typename Handler, typename ResponseProto>
concept ResponseHandlerFor = std::invocable<Handler&, ResponseProto&&>;

// A wrapper over the asynchronous bidirectional client stream
// (grpc::ClientAsyncReaderWriterInterface). The write side (the completion
// queue, the operation tags, write serialization with a bounded write queue,
// half-close and the final status retrieval) is inherited from
// AsyncWriteStream; this class adds the read side: exactly one read is kept
// outstanding for the whole lifetime of the call, and the received responses
// are passed to the handler given to poll() / pollFor(). The protocol-specific
// parts (creating the stream via the stub method and processing the received
// responses) are provided by the caller.
//
// Threading: NOT thread-safe. open(), close(), sendRequest(), writesDone(),
// setWriteQueueLimits() and poll() must all be called from the same thread.
template<ProtobufMessage RequestProto, ProtobufMessage ResponseProto>
class AsyncReadWriteStream
    : public AsyncWriteStream<RequestProto, grpc::ClientAsyncReaderWriterInterface<RequestProto, ResponseProto>>
{
    using Base = AsyncWriteStream<RequestProto, grpc::ClientAsyncReaderWriterInterface<RequestProto, ResponseProto>>;

public:
    using AsyncReaderWriterInterface = ::grpc::ClientAsyncReaderWriterInterface<RequestProto, ResponseProto>;
    using Request                    = RequestProto;
    using Response                   = ResponseProto;

    AsyncReadWriteStream()
        : AsyncReadWriteStream(WriteQueueLimits{})
    {
    }
    explicit AsyncReadWriteStream(const WriteQueueLimits& limits)
        : Base(typename Base::ReadSideTag{}, limits)
    {
    }
    // The outstanding read refers to m_pending_response: the call must be
    // released before this member is destroyed (the base destructor runs later)
    ~AsyncReadWriteStream() { this->close(); }

    AsyncReadWriteStream(const AsyncReadWriteStream&)            = delete;
    AsyncReadWriteStream& operator=(const AsyncReadWriteStream&) = delete;
    AsyncReadWriteStream(AsyncReadWriteStream&&)                 = delete;
    AsyncReadWriteStream& operator=(AsyncReadWriteStream&&)      = delete;

    // Opens a new stream (see AsyncWriteStream::open()):
    //
    //   stream.open([&](grpc::ClientContext* ctx, grpc::CompletionQueue* cq, void* tag) {
    //       return stub->AsyncOpenChatStream(ctx, cq, tag);
    //   });
    template<StreamFactoryFor<RequestProto, ResponseProto> StreamFactory>
    bool open(StreamFactory&& stream_factory)
    {
        if (!Base::open(std::forward<StreamFactory>(stream_factory)))
            return false;

        // Exactly one read is kept outstanding for the whole lifetime of the call,
        // so server messages and the end of the stream are always reported.
        m_pending_response.Clear();
        startRead();
        return true;
    }

    [[nodiscard]] AsyncReaderWriterInterface* getReaderWriter() const noexcept { return this->stream(); }

    // Non-blocking processing of up to max_events currently available stream
    // events. Received responses are passed one by one to the response handler,
    // which may call sendRequest(), writesDone() or close(). Returns false once
    // the call has ended (status() then holds the reason), true otherwise.
    template<ResponseHandlerFor<ResponseProto> ResponseHandler>
    bool poll(ResponseHandler&& response_handler, std::size_t max_events = 64)
    {
        // An already expired deadline makes AsyncNext return immediately with
        // GOT_EVENT if an event is available, or TIMEOUT otherwise
        auto on_read = [this, &response_handler](bool ok) { onRead(ok, response_handler); };
        return this->processEvents(on_read, max_events, gpr_inf_past(GPR_CLOCK_MONOTONIC));
    }

    // Like poll(), but waits up to `timeout` for the first event, which avoids
    // busy polling in a dedicated stream thread. Further already available
    // events are processed without waiting.
    template<ResponseHandlerFor<ResponseProto> ResponseHandler, typename Rep, typename Period>
    bool pollFor(ResponseHandler&& response_handler, std::chrono::duration<Rep, Period> timeout,
                 std::size_t max_events = 64)
    {
        auto on_read = [this, &response_handler](bool ok) { onRead(ok, response_handler); };
        return this->processEvents(on_read, max_events, Base::deadlineAfter(timeout));
    }

private:
    using Tag = typename Base::Tag;

    template<typename ResponseHandler>
    void onRead(bool ok, ResponseHandler& response_handler)
    {
        this->setReadInProgress(false);
        if (!ok) [[unlikely]]
        {
            // No more messages from the server: stop writing and collect the
            // final status once all outstanding operations are done.
            this->enterFinishing();
            return;
        }

        // Move the message out before re-arming the read: the read buffer is
        // overwritten by the next read, and the handler may keep the message.
        ResponseProto response = std::move(m_pending_response);
        m_pending_response.Clear();
        startRead();
        response_handler(std::move(response));
    }

    void startRead()
    {
        this->stream()->Read(&m_pending_response, Base::toTag(Tag::Read));
        this->setReadInProgress(true);
    }

    ResponseProto m_pending_response;
};

} // namespace grpc_utils
