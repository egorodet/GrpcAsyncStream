// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

#pragma once

// Read side of asynchronous client streams.
// Requires C++20; depends only on gRPC and the C++ standard library.

#include <grpc_async_stream/AsyncCallBase.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>

#include <chrono>
#include <concepts>
#include <cstddef>
#include <utility>

namespace grpc_async_stream
{

// Read side of a client stream: ClientAsyncReaderInterface or ClientAsyncReaderWriterInterface
template<typename StreamInterface, typename ResponseProto>
concept AsyncReaderInterfaceFor =
    requires(StreamInterface& stream, ResponseProto* response, grpc::Status* status, void* tag) {
        stream.Read(response, tag);
        stream.Finish(status, tag);
    };

// Callable receiving responses in poll() / pollFor()
template<typename Handler, typename ResponseProto>
concept ResponseHandlerFor = std::invocable<Handler&, ResponseProto&&>;

// Server-streaming call wrapper: keeps one read outstanding and passes responses to the poll() handler.
//   stream.open([&](auto* ctx, auto* cq, void* tag) { return stub->AsyncSubscribe(ctx, request, cq, tag); });
// Not thread-safe: call all members from one thread.
template<ProtobufMessage ResponseProto,
         typename Base = detail::AsyncCallBase<grpc::ClientAsyncReaderInterface<ResponseProto>>>
    requires std::derived_from<Base, detail::AsyncCallBase<typename Base::AsyncStreamInterface>> &&
             AsyncReaderInterfaceFor<typename Base::AsyncStreamInterface, ResponseProto>
class AsyncReadStream : public Base
{
public:
    using Response = ResponseProto;

    AsyncReadStream() { this->registerReadSide(); }

    // Forwards the arguments to the layer below, e.g. WriteQueueLimits of AsyncWriteStream
    template<typename... BaseArgs>
        requires(sizeof...(BaseArgs) > 0) && std::constructible_from<Base, BaseArgs...>
    explicit AsyncReadStream(BaseArgs&&... base_args)
        : Base(std::forward<BaseArgs>(base_args)...)
    {
        this->registerReadSide();
    }

    // The outstanding read refers to m_pending_response: release the call before it is destroyed
    ~AsyncReadStream() { close(); }

    AsyncReadStream(const AsyncReadStream&)            = delete;
    AsyncReadStream& operator=(const AsyncReadStream&) = delete;
    AsyncReadStream(AsyncReadStream&&)                 = delete;
    AsyncReadStream& operator=(AsyncReadStream&&)      = delete;

    // Opens a new call (closing the previous one) and starts reading.
    template<AsyncStreamFactoryFor<typename Base::AsyncStreamInterface> StreamFactory>
    bool open(StreamFactory&& stream_factory)
    {
        if (!Base::open(std::forward<StreamFactory>(stream_factory)))
            return false;

        // One read is always outstanding, so messages and the end of stream are reported
        m_pending_response.Clear();
        startRead();
        return true;
    }

    // Cancels the active call and releases all resources, including the read buffers.
    void close()
    {
        Base::close();
        m_pending_response = ResponseProto{};
        m_spare_response   = ResponseProto{};
    }

    // Processes up to max_events available events, passing responses to the handler,
    // which may call any member, including close(). Returns false once the call has ended.
    template<ResponseHandlerFor<ResponseProto> ResponseHandler>
    bool poll(ResponseHandler&& response_handler, std::size_t max_events = 64)
    {
        return this->processEvents(eventHandler(response_handler), max_events, gpr_inf_past(GPR_CLOCK_MONOTONIC));
    }

    // Waits up to `timeout` for the next event and processes it; returns like poll().
    // With max_events > 1, detecting that no event is left may cost the timer slack
    // (50 us on Linux) when another thread polls gRPC, so one event per call is the default.
    template<ResponseHandlerFor<ResponseProto> ResponseHandler, typename Rep, typename Period>
    bool pollFor(ResponseHandler&& response_handler, std::chrono::duration<Rep, Period> timeout,
                 std::size_t max_events = 1)
    {
        return this->processEvents(eventHandler(response_handler), max_events, Base::deadlineAfter(timeout));
    }

protected:
    using Tag = detail::StreamTag;

    // Handles read completions; passes the rest to the layer below.
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
            // End of stream: finish the call (the write side, if any, discards its queue)
            this->enterFinishing();
            return;
        }

        // Move the response out and read the next one into the spare buffer: a cleared message
        // keeps the capacity of its fields, saving reallocations when the handler does not take it.
        ResponseProto response = std::move(m_pending_response);
        m_pending_response     = std::move(m_spare_response);
        startRead();
        response_handler(std::move(response));
        // Recycle what the handler left, unless it closed the stream
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
    ResponseProto m_spare_response;   // cleared buffer for the next read
};

} // namespace grpc_async_stream
