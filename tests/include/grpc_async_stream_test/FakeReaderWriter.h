// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

#pragma once

// Deterministic fakes of the gRPC client stream interfaces (FakeReaderWriter, FakeWriter,
// FakeReader), driven by a FakeCall that records every operation and completes it only
// when the test asks, via a grpc::Alarm on the stream's completion queue.
// FakeCall also records every violation of the gRPC async API contract.

#include <grpcpp/alarm.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>

#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace grpc_async_stream_test
{

template<typename W, typename R>
class FakeCall
{
public:
    // ----- Inspection -----

    [[nodiscard]] bool startPending() const { return m_start_tag != nullptr; }
    [[nodiscard]] bool readPending() const { return m_read_tag != nullptr; }
    [[nodiscard]] bool writePending() const { return m_write_tag != nullptr; }
    [[nodiscard]] bool writesDonePending() const { return m_writes_done_tag != nullptr; }
    [[nodiscard]] bool finishPending() const { return m_finish_tag != nullptr; }
    [[nodiscard]] bool writesDoneCalled() const { return m_writes_done_called; }
    [[nodiscard]] int  finishCalls() const { return m_finish_calls; }

    // All messages passed to Write(), in order
    [[nodiscard]] const std::vector<W>& writes() const { return m_writes; }

    // Request of a server-streaming call, passed to the stub method
    [[nodiscard]] const std::optional<W>& initialRequest() const { return m_initial_request; }

    // gRPC API contract violations committed by the stream
    [[nodiscard]] const std::vector<std::string>& violations() const { return m_violations; }

    // ----- Completion control -----

    void completeStart(bool ok = true) { post(m_start_tag, ok); }

    // Delivers a server message into the outstanding read
    void deliver(R message)
    {
        if (!m_read_tag)
        {
            m_violations.emplace_back("deliver() without an outstanding Read");
            return;
        }
        *m_read_buffer = std::move(message);
        m_read_buffer  = nullptr;
        post(m_read_tag, true);
    }

    // Fails the outstanding read: the server ended the stream
    void endReads()
    {
        m_read_buffer = nullptr;
        post(m_read_tag, false);
    }

    void completeWrite(bool ok = true) { post(m_write_tag, ok); }
    void completeWritesDone(bool ok = true) { post(m_writes_done_tag, ok); }

    // Completes Finish of a client-streaming call, filling the response message
    void completeFinish(const grpc::Status& status, R response)
    {
        if (m_response)
            *m_response = std::move(response);
        else
            m_violations.emplace_back("completeFinish() with a response for a call without a response message");
        completeFinish(status);
    }

    void completeFinish(const grpc::Status& status)
    {
        if (m_finish_status)
            *m_finish_status = status;
        m_finish_status = nullptr;
        post(m_finish_tag, true);
    }

    // ----- Called by the fake streams -----

    void onStartCall(grpc::CompletionQueue* cq, void* tag, R* response = nullptr)
    {
        m_cq        = cq;
        m_start_tag = tag;
        m_response  = response;
    }

    void onInitialRequest(const W& request) { m_initial_request = request; }

    void onRead(R* buffer, void* tag)
    {
        if (m_read_tag)
            m_violations.emplace_back("Read while another Read is outstanding");
        if (m_finish_tag)
            m_violations.emplace_back("Read after Finish");
        m_read_buffer = buffer;
        m_read_tag    = tag;
    }

    void onWrite(const W& message, void* tag)
    {
        if (m_start_tag)
            m_violations.emplace_back("Write before the start-call completed");
        if (m_write_tag || m_writes_done_tag)
            m_violations.emplace_back("Write while another write operation is outstanding");
        if (m_writes_done_called)
            m_violations.emplace_back("Write after WritesDone");
        if (m_finish_tag)
            m_violations.emplace_back("Write after Finish");
        m_writes.push_back(message);
        m_write_tag = tag;
    }

    void onWritesDone(void* tag)
    {
        if (m_start_tag)
            m_violations.emplace_back("WritesDone before the start-call completed");
        if (m_write_tag || m_writes_done_tag)
            m_violations.emplace_back("WritesDone while another write operation is outstanding");
        if (m_writes_done_called)
            m_violations.emplace_back("WritesDone called twice");
        m_writes_done_called = true;
        m_writes_done_tag    = tag;
    }

    void onFinish(grpc::Status* status, void* tag)
    {
        ++m_finish_calls;
        if (m_finish_calls > 1)
            m_violations.emplace_back("Finish called more than once");
        if (m_read_tag)
            m_violations.emplace_back("Finish while a Read is outstanding");
        if (m_write_tag || m_writes_done_tag)
            m_violations.emplace_back("Finish while a write operation is outstanding");
        m_finish_status = status;
        m_finish_tag    = tag;
    }

    // Stream destroyed (after its completion queue was drained)
    void onDestroyed()
    {
        m_alarms.clear();
        m_cq       = nullptr;
        m_response = nullptr;
    }

private:
    void post(void*& tag, bool ok)
    {
        if (!tag || !m_cq)
        {
            m_violations.emplace_back("test tried to complete an operation that is not pending");
            return;
        }
        auto alarm = std::make_unique<grpc::Alarm>();
        if (ok)
        {
            alarm->Set(m_cq, gpr_now(GPR_CLOCK_MONOTONIC), tag);
        }
        else
        {
            alarm->Set(m_cq, gpr_inf_future(GPR_CLOCK_MONOTONIC), tag);
            alarm->Cancel();
        }
        m_alarms.push_back(std::move(alarm));
        tag = nullptr;
    }

    grpc::CompletionQueue* m_cq = nullptr;

    void* m_start_tag       = nullptr;
    void* m_read_tag        = nullptr;
    R*    m_read_buffer     = nullptr;
    void* m_write_tag       = nullptr;
    void* m_writes_done_tag = nullptr;
    void* m_finish_tag      = nullptr;

    R*            m_response           = nullptr; // response message of a client-streaming call
    grpc::Status* m_finish_status      = nullptr;
    bool          m_writes_done_called = false;
    int           m_finish_calls       = 0;

    std::optional<W>                         m_initial_request;
    std::vector<W>                           m_writes;
    std::vector<std::string>                 m_violations;
    std::deque<std::unique_ptr<grpc::Alarm>> m_alarms;
};

template<typename W, typename R>
class FakeReaderWriter final : public grpc::ClientAsyncReaderWriterInterface<W, R>
{
public:
    explicit FakeReaderWriter(std::shared_ptr<FakeCall<W, R>> call)
        : m_call(std::move(call))
    {
    }
    ~FakeReaderWriter() override { m_call->onDestroyed(); }

    // Starts the call, like a generated Async* stub method
    void start(grpc::CompletionQueue* cq, void* tag) { m_call->onStartCall(cq, tag); }

    void StartCall(void* /*tag*/) override {}
    void ReadInitialMetadata(void* /*tag*/) override {}
    void Finish(grpc::Status* status, void* tag) override { m_call->onFinish(status, tag); }
    void WritesDone(void* tag) override { m_call->onWritesDone(tag); }
    void Write(const W& msg, void* tag) override { m_call->onWrite(msg, tag); }
    void Write(const W& msg, grpc::WriteOptions /*options*/, void* tag) override { m_call->onWrite(msg, tag); }
    void Read(R* msg, void* tag) override { m_call->onRead(msg, tag); }

private:
    std::shared_ptr<FakeCall<W, R>> m_call;
};

template<typename W, typename R>
class FakeWriter final : public grpc::ClientAsyncWriterInterface<W>
{
public:
    explicit FakeWriter(std::shared_ptr<FakeCall<W, R>> call)
        : m_call(std::move(call))
    {
    }
    ~FakeWriter() override { m_call->onDestroyed(); }

    // Starts the call, like a generated Async* stub method
    void start(grpc::CompletionQueue* cq, void* tag, R* response) { m_call->onStartCall(cq, tag, response); }

    void StartCall(void* /*tag*/) override {}
    void ReadInitialMetadata(void* /*tag*/) override {}
    void Finish(grpc::Status* status, void* tag) override { m_call->onFinish(status, tag); }
    void WritesDone(void* tag) override { m_call->onWritesDone(tag); }
    void Write(const W& msg, void* tag) override { m_call->onWrite(msg, tag); }
    void Write(const W& msg, grpc::WriteOptions /*options*/, void* tag) override { m_call->onWrite(msg, tag); }

private:
    std::shared_ptr<FakeCall<W, R>> m_call;
};

template<typename W, typename R>
class FakeReader final : public grpc::ClientAsyncReaderInterface<R>
{
public:
    explicit FakeReader(std::shared_ptr<FakeCall<W, R>> call)
        : m_call(std::move(call))
    {
    }
    ~FakeReader() override { m_call->onDestroyed(); }

    // Takes the request and starts the call, like a generated Async* stub method
    void start(grpc::CompletionQueue* cq, void* tag, const W& request)
    {
        m_call->onInitialRequest(request);
        m_call->onStartCall(cq, tag);
    }

    void StartCall(void* /*tag*/) override {}
    void ReadInitialMetadata(void* /*tag*/) override {}
    void Finish(grpc::Status* status, void* tag) override { m_call->onFinish(status, tag); }
    void Read(R* msg, void* tag) override { m_call->onRead(msg, tag); }

private:
    std::shared_ptr<FakeCall<W, R>> m_call;
};

// Stream factory for AsyncReadWriteStream::open() bound to `call`
template<typename W, typename R>
auto makeFakeStreamFactory(std::shared_ptr<FakeCall<W, R>> call)
{
    return [call](grpc::ClientContext*, grpc::CompletionQueue* cq, void* tag)
               -> std::unique_ptr<grpc::ClientAsyncReaderWriterInterface<W, R>> {
        auto stream = std::make_unique<FakeReaderWriter<W, R>>(call);
        stream->start(cq, tag);
        return stream;
    };
}

// Stream factory for AsyncWriteStream::open() bound to `call`;
// completeFinish(status, response) fills `response`
template<typename W, typename R>
auto makeFakeWriterFactory(std::shared_ptr<FakeCall<W, R>> call, R* response)
{
    return [call, response](grpc::ClientContext*, grpc::CompletionQueue* cq, void* tag)
               -> std::unique_ptr<grpc::ClientAsyncWriterInterface<W>> {
        auto stream = std::make_unique<FakeWriter<W, R>>(call);
        stream->start(cq, tag, response);
        return stream;
    };
}

// Stream factory for AsyncReadStream::open() bound to `call`; initialRequest() returns `request`
template<typename W, typename R>
auto makeFakeReaderFactory(std::shared_ptr<FakeCall<W, R>> call, W request)
{
    return [call, request = std::move(request)](grpc::ClientContext*, grpc::CompletionQueue* cq, void* tag)
               -> std::unique_ptr<grpc::ClientAsyncReaderInterface<R>> {
        auto stream = std::make_unique<FakeReader<W, R>>(call);
        stream->start(cq, tag, request);
        return stream;
    };
}

} // namespace grpc_async_stream_test
