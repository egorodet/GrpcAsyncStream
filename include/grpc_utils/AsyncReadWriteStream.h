#pragma once

// Requires C++20 (concepts, designated initializers, defaulted comparisons).
// Depends only on gRPC and the C++ standard library.

#include <grpc_utils/AsyncCallBase.h>
#include <grpc_utils/AsyncReadStream.h>
#include <grpc_utils/AsyncWriteStream.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>

namespace grpc_utils
{

// The callable passed to AsyncReadWriteStream::open(): creates the call with an
// Async* stub method, e.g. [&](auto* ctx, auto* cq, void* tag) { return stub->AsyncChat(ctx, cq, tag); }
template<typename Factory, typename RequestProto, typename ResponseProto>
concept StreamFactoryFor =
    AsyncStreamFactoryFor<Factory, grpc::ClientAsyncReaderWriterInterface<RequestProto, ResponseProto>>;

// A wrapper over the asynchronous bidirectional client stream
// (grpc::ClientAsyncReaderWriterInterface), assembled by single inheritance
// from the read side over the write side over the call core:
//
//   AsyncReadWriteStream -> AsyncReadStream -> AsyncWriteStream -> detail::AsyncCallBase
//
// so it offers the members of both AsyncWriteStream (sendRequest(),
// writesDone(), the bounded write queue and its statistics) and
// AsyncReadStream (poll() / pollFor() with a response handler). The
// handler-less poll() of the write side is hidden, so responses cannot be
// ignored by mistake. The protocol-specific parts (creating the stream via the
// stub method and processing the received responses) are provided by the caller.
//
// Threading: NOT thread-safe. open(), close(), sendRequest(), writesDone(),
// setWriteQueueLimits() and poll() must all be called from the same thread.
template<ProtobufMessage RequestProto, ProtobufMessage ResponseProto>
class AsyncReadWriteStream
    : public AsyncReadStream<
          ResponseProto,
          AsyncWriteStream<RequestProto,
                           detail::AsyncCallBase<grpc::ClientAsyncReaderWriterInterface<RequestProto, ResponseProto>>>>
{
    using Base = AsyncReadStream<
        ResponseProto,
        AsyncWriteStream<RequestProto,
                         detail::AsyncCallBase<grpc::ClientAsyncReaderWriterInterface<RequestProto, ResponseProto>>>>;

public:
    using AsyncReaderWriterInterface = ::grpc::ClientAsyncReaderWriterInterface<RequestProto, ResponseProto>;
    using Request                    = RequestProto;
    using Response                   = ResponseProto;

    AsyncReadWriteStream() = default;
    // Not inherited from the forwarding constructor of AsyncReadStream, so that
    // designated initializers work: AsyncReadWriteStream<Req, Resp> stream({.max_messages = 256});
    explicit AsyncReadWriteStream(const WriteQueueLimits& limits)
        : Base(limits)
    {
    }

    [[nodiscard]] AsyncReaderWriterInterface* getReaderWriter() const noexcept { return this->getStream(); }
};

} // namespace grpc_utils
