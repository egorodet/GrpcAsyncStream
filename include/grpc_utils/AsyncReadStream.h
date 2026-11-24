#pragma once

// Requires C++20 (concepts).
// Depends only on gRPC and the C++ standard library.

#include <grpc_utils/AsyncCallBase.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>

#include <chrono>
#include <concepts>
#include <cstddef>
#include <utility>

namespace grpc_utils
{

// The read side of an asynchronous client stream: satisfied by
// grpc::ClientAsyncReaderInterface<Response> (server-streaming calls) and
// grpc::ClientAsyncReaderWriterInterface<Request, Response> (bidirectional calls)
template<typename StreamInterface, typename ResponseProto>
concept AsyncReaderInterfaceFor =
    requires(StreamInterface& stream, ResponseProto* response, grpc::Status* status, void* tag) {
        stream.Read(response, tag);
        stream.Finish(status, tag);
    };

// The callable receiving responses in poll() / pollFor()
template<typename Handler, typename ResponseProto>
concept ResponseHandlerFor = std::invocable<Handler&, ResponseProto&&>;

// A wrapper over the asynchronous server-streaming call
// (grpc::ClientAsyncReaderInterface): one request, sent by the Async* stub
// method when the call is created, and a stream of responses. Exactly one read
// is kept outstanding for the whole lifetime of the call, the received
// responses are passed to the handler given to poll() / pollFor(), and the
// final status is retrieved once the server has ended the stream. The call
// core (detail::AsyncCallBase) provides the completion queue, the operation
// tags, the non-blocking event polling, cancellation and Finish.
//
//   stream.open([&](grpc::ClientContext* ctx, grpc::CompletionQueue* cq, void* tag) {
//       return stub->AsyncSubscribe(ctx, request, cq, tag); // the request is serialized right away
//   });
//
// `Base` is the call core by default. AsyncReadWriteStream layers this class
// over AsyncWriteStream with the bidirectional stream interface; the read side
// is always the top layer.
//
// Threading: NOT thread-safe. open(), close() and poll() must all be called
// from the same thread.
template<ProtobufMessage ResponseProto,
         typename Base = detail::AsyncCallBase<grpc::ClientAsyncReaderInterface<ResponseProto>>>
    requires std::derived_from<Base, detail::AsyncCallBase<typename Base::AsyncStreamInterface>> &&
             AsyncReaderInterfaceFor<typename Base::AsyncStreamInterface, ResponseProto>
class AsyncReadStream : public Base
{
public:
    using Response = ResponseProto;

    AsyncReadStream() { this->registerReadSide(); }

    // Passes the arguments to the layer below, e.g. the WriteQueueLimits of AsyncWriteStream
    template<typename... BaseArgs>
        requires(sizeof...(BaseArgs) > 0) && std::constructible_from<Base, BaseArgs...>
    explicit AsyncReadStream(BaseArgs&&... base_args)
        : Base(std::forward<BaseArgs>(base_args)...)
    {
        this->registerReadSide();
    }

    // The outstanding read refers to m_pending_response: the call must be
    // released before this member is destroyed (the base destructors run later)
    ~AsyncReadStream() { close(); }

    AsyncReadStream(const AsyncReadStream&)            = delete;
    AsyncReadStream& operator=(const AsyncReadStream&) = delete;
    AsyncReadStream(AsyncReadStream&&)                 = delete;
    AsyncReadStream& operator=(AsyncReadStream&&)      = delete;

    // Opens a new call (closing any previous one) and starts reading.
    template<AsyncStreamFactoryFor<typename Base::AsyncStreamInterface> StreamFactory>
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

    // Cancels the call (if still active) and releases all resources, including
    // the memory of the read buffers.
    void close()
    {
        Base::close();
        m_pending_response = ResponseProto{};
        m_spare_response   = ResponseProto{};
    }

    // Non-blocking processing of up to max_events currently available stream
    // events. Received responses are passed one by one to the response handler,
    // which may call any member function, including close(). Returns false once
    // the call has ended (status() then holds the reason), true otherwise.
    // When no event is available, gRPC may take up to the thread's timer slack
    // to report it (see pollFor()).
    template<ResponseHandlerFor<ResponseProto> ResponseHandler>
    bool poll(ResponseHandler&& response_handler, std::size_t max_events = 64)
    {
        return this->processEvents(eventHandler(response_handler), max_events, gpr_inf_past(GPR_CLOCK_MONOTONIC));
    }

    // Waits up to `timeout` for the next event and processes it, which avoids
    // busy polling in a dedicated stream thread; returns like poll().
    //
    // One event per call by default: the loop calling pollFor() takes the
    // following ones, and the next call returns at once with an event that is
    // already available. With max_events > 1, the further available events are
    // processed in the same call, but finding out that none is left is not
    // free: when another thread of the process polls a gRPC completion queue
    // (a gRPC server, the thread of another stream), gRPC then waits up to the
    // thread's timer slack (50 us by default on Linux) before returning, which
    // would delay whatever the caller does next, such as sending a request.
    template<ResponseHandlerFor<ResponseProto> ResponseHandler, typename Rep, typename Period>
    bool pollFor(ResponseHandler&& response_handler, std::chrono::duration<Rep, Period> timeout,
                 std::size_t max_events = 1)
    {
        return this->processEvents(eventHandler(response_handler), max_events, Base::deadlineAfter(timeout));
    }

protected:
    using Tag = detail::StreamTag;

    // Handles the read completions and passes the others down to the layer below.
    template<typename ResponseHandler>
    void handleEvent(Tag tag, bool ok, ResponseHandler& response_handler)
    {
        if (tag != Tag::Read)
        {
            Base::handleEvent(tag, ok, response_handler);
            return;
        }

        this->setReadOperationPending(false);
        if (!ok) [[unlikely]]
        {
            // No more messages from the server: stop the call and collect the
            // final status once all outstanding operations are done (resolves
            // to the write side's enterFinishing(), if any, which discards its queue)
            this->enterFinishing();
            return;
        }

        // Move the message out before re-arming the read: the read buffer is
        // overwritten by the next read, and the handler may keep the message.
        // The next read goes into the spare buffer, which holds the cleared
        // message of the previous event: Clear() keeps the capacity of string,
        // bytes and repeated fields, so a handler that does not take the
        // message saves gRPC from reallocating them for every response.
        ResponseProto response = std::move(m_pending_response);
        m_pending_response     = std::move(m_spare_response);
        startRead();
        response_handler(std::move(response));
        // Recycle what the handler left (an empty message if it took it), unless
        // it closed the stream, which releases the buffers.
        if (this->isOpen())
        {
            response.Clear();
            m_spare_response = std::move(response);
        }
    }

private:
    template<typename ResponseHandler>
    auto eventHandler(ResponseHandler& response_handler)
    {
        return [this, &response_handler](Tag tag, bool ok) { handleEvent(tag, ok, response_handler); };
    }

    void startRead()
    {
        this->stream()->Read(&m_pending_response, Base::toTag(Tag::Read));
        this->setReadOperationPending(true);
    }

    ResponseProto m_pending_response; // target of the outstanding read
    ResponseProto m_spare_response;   // cleared buffer for the next read (see handleEvent)
};

} // namespace grpc_utils
