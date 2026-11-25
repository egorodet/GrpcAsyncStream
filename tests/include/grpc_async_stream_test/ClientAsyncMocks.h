// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

#pragma once

// Extensible mocks of the gRPC client stream interfaces, recording into a FakeCall:
//   ClientAsyncReaderWriterMock<W, R>, ClientAsyncWriterMock<W>, ClientAsyncReaderMock<R>
// Derive, override operations calling the base, push responses with enqueueResponse(); overriding
// one Write() hides the other (add a using-declaration). See EchoMock in tests/ClientAsyncMocksTests.cpp.
// Test-framework independent; requires only gRPC.

#include <grpc_async_stream_test/FakeReaderWriter.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>

#include <concepts>
#include <cstddef>
#include <deque>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace grpc_async_stream_test
{

namespace detail
{

// Message type of the direction a mock does not have
struct NoMessage
{
};

// Common part of all mocks: completion queue and the FakeCall recording operations
template<typename W, typename R>
class ClientAsyncMockBase
{
public:
    using Call = FakeCall<W, R>;

    ClientAsyncMockBase(const ClientAsyncMockBase&)            = delete;
    ClientAsyncMockBase& operator=(const ClientAsyncMockBase&) = delete;
    ClientAsyncMockBase(ClientAsyncMockBase&&)                 = delete;
    ClientAsyncMockBase& operator=(ClientAsyncMockBase&&)      = delete;

    // Call recording the operations; sharedCall() keeps it alive after the mock is destroyed
    [[nodiscard]] Call&                        call() const noexcept { return *m_call; }
    [[nodiscard]] const std::shared_ptr<Call>& sharedCall() const noexcept { return m_call; }

    [[nodiscard]] grpc::CompletionQueue* completionQueue() const noexcept { return m_cq; }
    [[nodiscard]] bool                   autoComplete() const noexcept { return m_auto_complete; }
    [[nodiscard]] bool                   started() const noexcept { return m_start_call_calls > 0; }
    [[nodiscard]] int                    startCallCalls() const noexcept { return m_start_call_calls; }
    [[nodiscard]] int readInitialMetadataCalls() const noexcept { return m_read_initial_metadata_calls; }

protected:
    ClientAsyncMockBase(grpc::CompletionQueue* cq, bool auto_complete)
        : m_cq(cq)
        , m_call(std::make_shared<Call>())
        , m_auto_complete(auto_complete)
    {
    }

    // Destroyed after the completion queue was drained
    ~ClientAsyncMockBase() { m_call->onDestroyed(); }

    // Shared operations: record with the FakeCall and, with auto-completion, succeed at once
    void startCall(void* tag)
    {
        ++m_start_call_calls;
        m_call->onStartCall(m_cq, tag);
        if (m_auto_complete)
            m_call->completeStart();
    }

    void write(const W& msg, void* tag)
    {
        m_call->onWrite(msg, tag);
        if (autoCompletes())
            m_call->completeWrite();
    }

    void writesDone(void* tag)
    {
        m_call->onWritesDone(tag);
        if (autoCompletes())
            m_call->completeWritesDone();
    }

    void finish(grpc::Status* status, void* tag)
    {
        m_call->onFinish(status, tag);
        if (autoCompletes())
            m_call->completeFinish(grpc::Status::OK);
    }

    void recordReadInitialMetadata() noexcept { ++m_read_initial_metadata_calls; }

private:
    // Operations recorded before the start-call stay pending (no completion queue yet)
    [[nodiscard]] bool autoCompletes() const noexcept { return m_auto_complete && started(); }

    grpc::CompletionQueue* m_cq;
    std::shared_ptr<Call>  m_call;
    bool                   m_auto_complete;
    int                    m_start_call_calls            = 0;
    int                    m_read_initial_metadata_calls = 0;
};

// Responses queued by the reading mocks until a Read takes them
template<typename W, typename R>
class ClientAsyncReadingMockBase : public ClientAsyncMockBase<W, R>
{
public:
    // Responses enqueued and not yet delivered to a Read
    [[nodiscard]] std::size_t queuedResponses() const noexcept { return m_responses.size(); }

protected:
    using ClientAsyncMockBase<W, R>::ClientAsyncMockBase;

    // Queues a response for the outstanding or the next Read
    void enqueueResponse(R response)
    {
        m_responses.push_back(std::move(response));
        deliverQueuedResponse();
    }

    // Delivers the oldest queued response to an outstanding Read of a started call
    void deliverQueuedResponse()
    {
        if (m_responses.empty() || !this->started() || !this->call().readPending())
            return;
        R response = std::move(m_responses.front());
        m_responses.pop_front();
        this->call().deliver(std::move(response));
    }

private:
    std::deque<R> m_responses;
};

} // namespace detail

// Mock of grpc::ClientAsyncReaderWriterInterface<W, R> for bidirectional calls
template<typename W, typename R>
class ClientAsyncReaderWriterMock
    : public grpc::ClientAsyncReaderWriterInterface<W, R>
    , public detail::ClientAsyncReadingMockBase<W, R>
{
public:
    using Interface = grpc::ClientAsyncReaderWriterInterface<W, R>;
    using Call      = FakeCall<W, R>;

    // With auto_complete, start-call, writes, half-close and Finish (OK) succeed at once;
    // reads still need queued responses and call().endReads(). Self-completing derived
    // mocks should check !autoComplete().
    explicit ClientAsyncReaderWriterMock(grpc::CompletionQueue* cq, bool auto_complete = false)
        : detail::ClientAsyncReadingMockBase<W, R>(cq, auto_complete)
    {
    }

    void StartCall(void* tag) override
    {
        this->startCall(tag);
        this->deliverQueuedResponse();
    }
    void ReadInitialMetadata(void* /*tag*/) override { this->recordReadInitialMetadata(); }
    void Finish(grpc::Status* status, void* tag) override { this->finish(status, tag); }
    void WritesDone(void* tag) override { this->writesDone(tag); }
    void Write(const W& msg, void* tag) override { this->write(msg, tag); }
    void Write(const W& msg, grpc::WriteOptions /*options*/, void* tag) override { this->write(msg, tag); }

    // Requests passed to Write() so far, in order (same as call().writes())
    [[nodiscard]] const std::vector<W>& writtenRequests() const noexcept { return this->call().writes(); }

    // Copies the next not yet popped written request; false when all were popped
    [[nodiscard]] bool popWrittenRequest(W& request)
    {
        const std::vector<W>& written = this->call().writes();
        if (m_popped_requests >= written.size())
            return false;
        request = written[m_popped_requests++];
        return true;
    }

    void Read(R* msg, void* tag) override
    {
        this->call().onRead(msg, tag);
        this->deliverQueuedResponse();
    }

private:
    std::size_t m_popped_requests = 0;
};

// Mock of grpc::ClientAsyncWriterInterface<W> for client-streaming calls
template<typename W>
class ClientAsyncWriterMock
    : public grpc::ClientAsyncWriterInterface<W>
    , public detail::ClientAsyncMockBase<W, detail::NoMessage>
{
public:
    using Interface = grpc::ClientAsyncWriterInterface<W>;
    using Call      = FakeCall<W, detail::NoMessage>;

    // With auto_complete, the mock completes its operations itself
    explicit ClientAsyncWriterMock(grpc::CompletionQueue* cq, bool auto_complete = false)
        : detail::ClientAsyncMockBase<W, detail::NoMessage>(cq, auto_complete)
    {
    }

    void StartCall(void* tag) override { this->startCall(tag); }
    void ReadInitialMetadata(void* /*tag*/) override { this->recordReadInitialMetadata(); }
    void Finish(grpc::Status* status, void* tag) override { this->finish(status, tag); }
    void WritesDone(void* tag) override { this->writesDone(tag); }
    void Write(const W& msg, void* tag) override { this->write(msg, tag); }
    void Write(const W& msg, grpc::WriteOptions /*options*/, void* tag) override { this->write(msg, tag); }

    // Requests passed to Write() so far, in order (same as call().writes())
    [[nodiscard]] const std::vector<W>& writtenRequests() const noexcept { return this->call().writes(); }

    // Copies the next not yet popped written request; false when all were popped
    [[nodiscard]] bool popWrittenRequest(W& request)
    {
        const std::vector<W>& written = this->call().writes();
        if (m_popped_requests >= written.size())
            return false;
        request = written[m_popped_requests++];
        return true;
    }

private:
    std::size_t m_popped_requests = 0;
};

// Mock of grpc::ClientAsyncReaderInterface<R> for server-streaming calls
template<typename R>
class ClientAsyncReaderMock
    : public grpc::ClientAsyncReaderInterface<R>
    , public detail::ClientAsyncReadingMockBase<detail::NoMessage, R>
{
public:
    using Interface = grpc::ClientAsyncReaderInterface<R>;
    using Call      = FakeCall<detail::NoMessage, R>;

    // With auto_complete, the mock completes its operations itself
    explicit ClientAsyncReaderMock(grpc::CompletionQueue* cq, bool auto_complete = false)
        : detail::ClientAsyncReadingMockBase<detail::NoMessage, R>(cq, auto_complete)
    {
    }

    void StartCall(void* tag) override
    {
        this->startCall(tag);
        this->deliverQueuedResponse();
    }
    void ReadInitialMetadata(void* /*tag*/) override { this->recordReadInitialMetadata(); }
    void Finish(grpc::Status* status, void* tag) override { this->finish(status, tag); }
    void Read(R* msg, void* tag) override
    {
        this->call().onRead(msg, tag);
        this->deliverQueuedResponse();
    }
};

// One of the mocks above or a class derived from it
template<typename Mock>
concept ClientAsyncMock = std::derived_from<Mock, typename Mock::Interface> &&
                          std::constructible_from<Mock, grpc::CompletionQueue*> &&
                          std::same_as<std::remove_cvref_t<decltype(std::declval<const Mock&>().call())>,
                                       typename Mock::Call>;

namespace detail
{

// Stream factory creating mocks with `make_mock(cq)`
template<typename Mock, typename MakeMock, typename OnCreated>
auto makeMockStreamFactory(MakeMock make_mock, OnCreated on_created)
{
    return [make_mock = std::move(make_mock), on_created = std::move(on_created)](
               grpc::ClientContext* /*context*/, grpc::CompletionQueue* cq,
               void* tag) mutable -> std::unique_ptr<typename Mock::Interface> {
        std::unique_ptr<Mock> stream = make_mock(cq);
        stream->StartCall(tag);
        on_created(*stream);
        return stream;
    };
}

} // namespace detail

// Stream factory for open(): like an Async* stub method, creates a Mock on the stream's
// completion queue, calls StartCall(tag) and passes the mock to `on_created`
// (a pointer to it is valid until the stream releases the call).
template<ClientAsyncMock Mock, typename OnCreated = void (*)(Mock&)>
    requires std::invocable<OnCreated&, Mock&>
auto makeMockStreamFactory(OnCreated on_created = [](Mock&) {})
{
    return detail::makeMockStreamFactory<Mock>([](grpc::CompletionQueue* cq) { return std::make_unique<Mock>(cq); },
                                               std::move(on_created));
}

// Same, constructing Mock(cq, auto_complete)
template<ClientAsyncMock Mock, typename OnCreated = void (*)(Mock&)>
    requires std::constructible_from<Mock, grpc::CompletionQueue*, bool> && std::invocable<OnCreated&, Mock&>
auto makeMockStreamFactory(bool auto_complete, OnCreated on_created = [](Mock&) {})
{
    return detail::makeMockStreamFactory<Mock>(
        [auto_complete](grpc::CompletionQueue* cq) { return std::make_unique<Mock>(cq, auto_complete); },
        std::move(on_created));
}

} // namespace grpc_async_stream_test
