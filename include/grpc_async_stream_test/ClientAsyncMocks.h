#pragma once

// Extensible mocks of the gRPC client stream interfaces:
//
//   ClientAsyncReaderWriterMock<W, R>  grpc::ClientAsyncReaderWriterInterface<W, R>  (bidirectional calls)
//   ClientAsyncWriterMock<W, R>        grpc::ClientAsyncWriterInterface<W>           (client-streaming calls)
//   ClientAsyncReaderMock<W, R>        grpc::ClientAsyncReaderInterface<R>           (server-streaming calls)
//
// Unlike the final fakes of FakeReaderWriter.h, these classes are meant to be
// derived from. Every operation is virtual and, by default, behaves like the
// fakes: it is recorded by the shared FakeCall, which checks the gRPC async API
// contract and completes the operation only when the test asks for it
// (FakeCall::completeStart(), deliver(), completeWrite(), ...). A derived mock
// overrides the operations it needs to change and calls the base
// implementation to keep the recording, e.g. to complete the start-call
// right away, to inspect the WriteOptions or the client context, or to inject
// failures:
//
//   template<typename W, typename R>
//   class AutoStartedMock : public grpc_async_stream_test::ClientAsyncReaderWriterMock<W, R>
//   {
//   public:
//       using grpc_async_stream_test::ClientAsyncReaderWriterMock<W, R>::ClientAsyncReaderWriterMock;
//
//       void start(grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) override
//       {
//           grpc_async_stream_test::ClientAsyncReaderWriterMock<W, R>::start(context, cq, tag);
//           this->call().completeStart();
//       }
//   };
//
//   stream.open(grpc_async_stream_test::makeMockStreamFactory<AutoStartedMock<Req, Resp>>(call));
//
// A derived mock overriding one Write() overload hides the other one: bring it
// back with `using Base::Write;`.
//
// Test-framework independent; requires only gRPC.

#include <grpc_async_stream_test/FakeReaderWriter.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>

#include <cassert>
#include <concepts>
#include <memory>
#include <utility>

namespace grpc_async_stream_test
{

namespace detail
{

// What the three mocks have in common: the FakeCall shared with the test and
// the calls of the operations the Async* stub methods make unnecessary
template<typename W, typename R>
class ClientAsyncMockBase
{
public:
    using Request  = W;
    using Response = R;
    using Call     = FakeCall<W, R>;

    ClientAsyncMockBase(const ClientAsyncMockBase&)            = delete;
    ClientAsyncMockBase& operator=(const ClientAsyncMockBase&) = delete;
    ClientAsyncMockBase(ClientAsyncMockBase&&)                 = delete;
    ClientAsyncMockBase& operator=(ClientAsyncMockBase&&)      = delete;

    // The call the operations are recorded by
    [[nodiscard]] Call&                        call() const noexcept { return *m_call; }
    [[nodiscard]] const std::shared_ptr<Call>& sharedCall() const noexcept { return m_call; }

    // Calls of StartCall() and ReadInitialMetadata(): the streams of the Async*
    // stub methods are started already, so the library calls neither
    [[nodiscard]] int startCallCalls() const noexcept { return m_start_call_calls; }
    [[nodiscard]] int readInitialMetadataCalls() const noexcept { return m_read_initial_metadata_calls; }

protected:
    explicit ClientAsyncMockBase(std::shared_ptr<Call> call)
        : m_call(std::move(call))
    {
        assert(m_call && "a client async mock needs a FakeCall");
    }

    // The stream is destroyed after its completion queue was drained
    ~ClientAsyncMockBase() { m_call->onDestroyed(); }

    void recordStartCall() noexcept { ++m_start_call_calls; }
    void recordReadInitialMetadata() noexcept { ++m_read_initial_metadata_calls; }

private:
    std::shared_ptr<Call> m_call;
    int                   m_start_call_calls            = 0;
    int                   m_read_initial_metadata_calls = 0;
};

} // namespace detail

// Mock of grpc::ClientAsyncReaderWriterInterface<W, R> for bidirectional calls
template<typename W, typename R>
class ClientAsyncReaderWriterMock
    : public grpc::ClientAsyncReaderWriterInterface<W, R>
    , public detail::ClientAsyncMockBase<W, R>
{
public:
    using Interface = grpc::ClientAsyncReaderWriterInterface<W, R>;
    using Call      = FakeCall<W, R>;

    explicit ClientAsyncReaderWriterMock(std::shared_ptr<Call> call)
        : detail::ClientAsyncMockBase<W, R>(std::move(call))
    {
    }

    // Called by makeMockStreamFactory() like a generated Async* stub method
    // starts the call: the start-call is pending until FakeCall::completeStart()
    virtual void start(grpc::ClientContext* /*context*/, grpc::CompletionQueue* cq, void* tag)
    {
        this->call().onStartCall(cq, tag);
    }

    void StartCall(void* /*tag*/) override { this->recordStartCall(); }
    void ReadInitialMetadata(void* /*tag*/) override { this->recordReadInitialMetadata(); }
    void Finish(grpc::Status* status, void* tag) override { this->call().onFinish(status, tag); }
    void WritesDone(void* tag) override { this->call().onWritesDone(tag); }
    void Write(const W& msg, void* tag) override { this->call().onWrite(msg, tag); }
    void Write(const W& msg, grpc::WriteOptions /*options*/, void* tag) override { this->call().onWrite(msg, tag); }
    void Read(R* msg, void* tag) override { this->call().onRead(msg, tag); }
};

// Mock of grpc::ClientAsyncWriterInterface<W> for client-streaming calls. Like
// a generated Async* stub method, it takes the message receiving the response,
// which FakeCall::completeFinish(status, response) fills.
template<typename W, typename R>
class ClientAsyncWriterMock
    : public grpc::ClientAsyncWriterInterface<W>
    , public detail::ClientAsyncMockBase<W, R>
{
public:
    using Interface = grpc::ClientAsyncWriterInterface<W>;
    using Call      = FakeCall<W, R>;

    explicit ClientAsyncWriterMock(std::shared_ptr<Call> call, R* response = nullptr)
        : detail::ClientAsyncMockBase<W, R>(std::move(call))
        , m_response(response)
    {
    }

    // Called by makeMockStreamFactory() like a generated Async* stub method
    // starts the call: the start-call is pending until FakeCall::completeStart()
    virtual void start(grpc::ClientContext* /*context*/, grpc::CompletionQueue* cq, void* tag)
    {
        this->call().onStartCall(cq, tag, m_response);
    }

    void StartCall(void* /*tag*/) override { this->recordStartCall(); }
    void ReadInitialMetadata(void* /*tag*/) override { this->recordReadInitialMetadata(); }
    void Finish(grpc::Status* status, void* tag) override { this->call().onFinish(status, tag); }
    void WritesDone(void* tag) override { this->call().onWritesDone(tag); }
    void Write(const W& msg, void* tag) override { this->call().onWrite(msg, tag); }
    void Write(const W& msg, grpc::WriteOptions /*options*/, void* tag) override { this->call().onWrite(msg, tag); }

protected:
    // The message receiving the response of the call (may be null)
    [[nodiscard]] R* response() const noexcept { return m_response; }

private:
    R* m_response;
};

// Mock of grpc::ClientAsyncReaderInterface<R> for server-streaming calls. Like
// a generated Async* stub method, it takes the request, which
// FakeCall::initialRequest() returns once the call is started.
template<typename W, typename R>
class ClientAsyncReaderMock
    : public grpc::ClientAsyncReaderInterface<R>
    , public detail::ClientAsyncMockBase<W, R>
{
public:
    using Interface = grpc::ClientAsyncReaderInterface<R>;
    using Call      = FakeCall<W, R>;

    ClientAsyncReaderMock(std::shared_ptr<Call> call, W request)
        : detail::ClientAsyncMockBase<W, R>(std::move(call))
        , m_request(std::move(request))
    {
    }

    // Called by makeMockStreamFactory() like a generated Async* stub method
    // sends the request and starts the call: the start-call is pending until
    // FakeCall::completeStart()
    virtual void start(grpc::ClientContext* /*context*/, grpc::CompletionQueue* cq, void* tag)
    {
        this->call().onInitialRequest(m_request);
        this->call().onStartCall(cq, tag);
    }

    void StartCall(void* /*tag*/) override { this->recordStartCall(); }
    void ReadInitialMetadata(void* /*tag*/) override { this->recordReadInitialMetadata(); }
    void Finish(grpc::Status* status, void* tag) override { this->call().onFinish(status, tag); }
    void Read(R* msg, void* tag) override { this->call().onRead(msg, tag); }

protected:
    // The request of the call
    [[nodiscard]] const W& request() const noexcept { return m_request; }

private:
    W m_request;
};

// One of the mocks above or a class derived from one of them, constructible
// from its FakeCall followed by `Args`
template<typename Mock, typename... Args>
concept ClientAsyncMock =
    std::derived_from<Mock, typename Mock::Interface> &&
    std::derived_from<Mock, detail::ClientAsyncMockBase<typename Mock::Request, typename Mock::Response>> &&
    std::constructible_from<Mock, std::shared_ptr<typename Mock::Call>, const Args&...> &&
    requires(Mock& mock, grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
        mock.start(context, cq, tag);
    };

// Stream factory for the open() of AsyncReadWriteStream, AsyncWriteStream or
// AsyncReadStream (depending on Mock::Interface): every call creates a Mock
// from `call` and `args` (the response message of a ClientAsyncWriterMock,
// the request of a ClientAsyncReaderMock, any arguments of a derived mock)
// and starts it with Mock::start(), e.g.
//   stream.open(makeMockStreamFactory<ClientAsyncWriterMock<Req, Resp>>(call, &response));
template<typename Mock, typename... Args>
    requires ClientAsyncMock<Mock, Args...>
auto makeMockStreamFactory(std::shared_ptr<typename Mock::Call> call, Args... args)
{
    return [call = std::move(call), ... args = std::move(args)](
               grpc::ClientContext* context, grpc::CompletionQueue* cq,
               void* tag) -> std::unique_ptr<typename Mock::Interface> {
        auto stream = std::make_unique<Mock>(call, args...);
        stream->start(context, cq, tag);
        return stream;
    };
}

} // namespace grpc_async_stream_test
