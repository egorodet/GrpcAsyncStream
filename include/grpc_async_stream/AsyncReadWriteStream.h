// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

#pragma once

// Bidirectional asynchronous client stream.
// Requires C++20; depends only on gRPC and the C++ standard library.

#include <grpc_async_stream/AsyncCallBase.h>
#include <grpc_async_stream/AsyncReadStream.h>
#include <grpc_async_stream/AsyncWriteStream.h>

#include <grpcpp/support/async_stream.h>

namespace grpc_async_stream
{

// Callable passed to AsyncReadWriteStream::open(), see AsyncStreamFactoryFor
template<typename Factory, typename RequestProto, typename ResponseProto>
concept StreamFactoryFor =
    AsyncStreamFactoryFor<Factory, grpc::ClientAsyncReaderWriterInterface<RequestProto, ResponseProto>>;

// Bidirectional call wrapper: AsyncReadWriteStream -> AsyncReadStream -> AsyncWriteStream -> detail::AsyncCallBase
// Combines the write queue of AsyncWriteStream with poll() / pollFor() taking a response handler.
// Not thread-safe: call all members from one thread.
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
    // Explicit (not forwarded) to allow: AsyncReadWriteStream<Req, Resp> stream({.max_messages = 256});
    explicit AsyncReadWriteStream(const WriteQueueLimits& limits)
        : Base(limits)
    {
    }

    [[nodiscard]] AsyncReaderWriterInterface* getReaderWriter() const noexcept { return this->getStream(); }
};

} // namespace grpc_async_stream
