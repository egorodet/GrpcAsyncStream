#pragma once

// Extensible mocks of the gRPC client stream interfaces:
//
//   ClientAsyncReaderWriterMock<W, R>  grpc::ClientAsyncReaderWriterInterface<W, R>  (bidirectional calls)
//   ClientAsyncWriterMock<W>           grpc::ClientAsyncWriterInterface<W>           (client-streaming calls)
//   ClientAsyncReaderMock<R>           grpc::ClientAsyncReaderInterface<R>           (server-streaming calls)
//
// Unlike the final fakes of FakeReaderWriter.h, these classes are meant to be
// derived from. A mock is constructed from the completion queue of the call
// only, like the stream of a PrepareAsync* stub method, and creates the
// FakeCall it records its operations with (see call()); StartCall(tag)
// starts the call. Every operation is virtual and, by default, behaves like
// the fakes: the FakeCall checks the gRPC async API contract and completes
// the operation only when the test asks for it (call().completeStart(),
// completeWrite(), ...).
//
// The reading mocks keep a queue of responses: the protected enqueueResponse()
// adds one, which is delivered to the outstanding Read, or to the next one.
// Derived mocks build their own response pushing methods on it, and override
// the operations they need to change, calling the base implementation to keep
// the recording:
//
//   class EchoMock : public grpc_async_stream_test::ClientAsyncReaderWriterMock<Req, Resp>
//   {
//   public:
//       using ClientAsyncReaderWriterMock::ClientAsyncReaderWriterMock;
//
//       void StartCall(void* tag) override
//       {
//           ClientAsyncReaderWriterMock::StartCall(tag);
//           call().completeStart();
//       }
//
//       using ClientAsyncReaderWriterMock::Write;
//       void Write(const Req& msg, void* tag) override
//       {
//           ClientAsyncReaderWriterMock::Write(msg, tag);
//           call().completeWrite();
//           enqueueResponse(makeEcho(msg));
//       }
//   };
//
//   EchoMock* mock = nullptr;
//   stream.open(grpc_async_stream_test::makeMockStreamFactory<EchoMock>([&](EchoMock& created) { mock = &created; }));
//
// A derived mock overriding one Write() overload hides the other one: bring it
// back with a using-declaration, as above.
//
// Test-framework independent; requires only gRPC.

#include <grpc_async_stream_test/FakeReaderWriter.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>

#include <concepts>
#include <deque>
#include <memory>
#include <type_traits>
#include <utility>

namespace grpc_async_stream_test
{

namespace detail
{

// Message type of the direction a mock does not have (the responses of a
// client-streaming call, the requests of a server-streaming call)
struct NoMessage
{
};

// What all mocks have in common: the completion queue of the call and the
// FakeCall recording the operations
template<typename W, typename R>
class ClientAsyncMockBase
{
public:
    using Call = FakeCall<W, R>;

    ClientAsyncMockBase(const ClientAsyncMockBase&)            = delete;
    ClientAsyncMockBase& operator=(const ClientAsyncMockBase&) = delete;
    ClientAsyncMockBase(ClientAsyncMockBase&&)                 = delete;
    ClientAsyncMockBase& operator=(ClientAsyncMockBase&&)      = delete;

    // The call recording the operations, which the test completes. sharedCall()
    // keeps it, e.g. to inspect the writes after the stream destroyed the mock.
    [[nodiscard]] Call&                        call() const noexcept { return *m_call; }
    [[nodiscard]] const std::shared_ptr<Call>& sharedCall() const noexcept { return m_call; }

    [[nodiscard]] grpc::CompletionQueue* completionQueue() const noexcept { return m_cq; }
    [[nodiscard]] bool                   started() const noexcept { return m_start_call_calls > 0; }
    [[nodiscard]] int                    startCallCalls() const noexcept { return m_start_call_calls; }
    [[nodiscard]] int readInitialMetadataCalls() const noexcept { return m_read_initial_metadata_calls; }

protected:
    explicit ClientAsyncMockBase(grpc::CompletionQueue* cq)
        : m_cq(cq)
        , m_call(std::make_shared<Call>())
    {
    }

    // The stream is destroyed after its completion queue was drained
    ~ClientAsyncMockBase() { m_call->onDestroyed(); }

    void startCall(void* tag)
    {
        ++m_start_call_calls;
        m_call->onStartCall(m_cq, tag);
    }

    void recordReadInitialMetadata() noexcept { ++m_read_initial_metadata_calls; }

private:
    grpc::CompletionQueue* m_cq;
    std::shared_ptr<Call>  m_call;
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

    // Queues a response for the outstanding Read, or for the next one: helper
    // of the response pushing methods of derived mocks
    void enqueueResponse(R response)
    {
        m_responses.push_back(std::move(response));
        deliverQueuedResponse();
    }

    // Delivers the oldest queued response if a Read is outstanding on a started call
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

    explicit ClientAsyncReaderWriterMock(grpc::CompletionQueue* cq)
        : detail::ClientAsyncReadingMockBase<W, R>(cq)
    {
    }

    void StartCall(void* tag) override
    {
        this->startCall(tag);
        this->deliverQueuedResponse();
    }
    void ReadInitialMetadata(void* /*tag*/) override { this->recordReadInitialMetadata(); }
    void Finish(grpc::Status* status, void* tag) override { this->call().onFinish(status, tag); }
    void WritesDone(void* tag) override { this->call().onWritesDone(tag); }
    void Write(const W& msg, void* tag) override { this->call().onWrite(msg, tag); }
    void Write(const W& msg, grpc::WriteOptions /*options*/, void* tag) override { this->call().onWrite(msg, tag); }
    void Read(R* msg, void* tag) override
    {
        this->call().onRead(msg, tag);
        this->deliverQueuedResponse();
    }
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

    explicit ClientAsyncWriterMock(grpc::CompletionQueue* cq)
        : detail::ClientAsyncMockBase<W, detail::NoMessage>(cq)
    {
    }

    void StartCall(void* tag) override { this->startCall(tag); }
    void ReadInitialMetadata(void* /*tag*/) override { this->recordReadInitialMetadata(); }
    void Finish(grpc::Status* status, void* tag) override { this->call().onFinish(status, tag); }
    void WritesDone(void* tag) override { this->call().onWritesDone(tag); }
    void Write(const W& msg, void* tag) override { this->call().onWrite(msg, tag); }
    void Write(const W& msg, grpc::WriteOptions /*options*/, void* tag) override { this->call().onWrite(msg, tag); }
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

    explicit ClientAsyncReaderMock(grpc::CompletionQueue* cq)
        : detail::ClientAsyncReadingMockBase<detail::NoMessage, R>(cq)
    {
    }

    void StartCall(void* tag) override
    {
        this->startCall(tag);
        this->deliverQueuedResponse();
    }
    void ReadInitialMetadata(void* /*tag*/) override { this->recordReadInitialMetadata(); }
    void Finish(grpc::Status* status, void* tag) override { this->call().onFinish(status, tag); }
    void Read(R* msg, void* tag) override
    {
        this->call().onRead(msg, tag);
        this->deliverQueuedResponse();
    }
};

// One of the mocks above or a class derived from one of them
template<typename Mock>
concept ClientAsyncMock = std::derived_from<Mock, typename Mock::Interface> &&
                          std::constructible_from<Mock, grpc::CompletionQueue*> &&
                          std::same_as<std::remove_cvref_t<decltype(std::declval<const Mock&>().call())>,
                                       typename Mock::Call>;

// Stream factory for the open() of AsyncReadWriteStream, AsyncWriteStream or
// AsyncReadStream (depending on Mock::Interface). Like a generated Async* stub
// method, every call creates a Mock on the stream's completion queue and
// starts it with StartCall(tag), then passes it to `on_created`, e.g. to keep
// a pointer to it (valid until the stream releases the call) or its sharedCall().
template<ClientAsyncMock Mock, typename OnCreated = void (*)(Mock&)>
    requires std::invocable<OnCreated&, Mock&>
auto makeMockStreamFactory(OnCreated on_created = [](Mock&) {})
{
    return [on_created = std::move(on_created)](grpc::ClientContext* /*context*/, grpc::CompletionQueue* cq,
                                                void* tag) mutable -> std::unique_ptr<typename Mock::Interface> {
        auto stream = std::make_unique<Mock>(cq);
        stream->StartCall(tag);
        on_created(*stream);
        return stream;
    };
}

} // namespace grpc_async_stream_test
