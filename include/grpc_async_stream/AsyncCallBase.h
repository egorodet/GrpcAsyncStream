// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

#pragma once

// Call core shared by AsyncWriteStream, AsyncReadStream and AsyncReadWriteStream.
// Requires C++20; depends only on gRPC and the C++ standard library.

#include <grpc/support/time.h>
#include <grpcpp/grpcpp.h>

#include <cassert>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

namespace grpc_async_stream
{

// State of a call, as reported by state()
enum class StreamState
{
    Closed,      // no call
    Open,        // writing and/or reading
    HalfClosing, // writesDone() requested, flushing queued requests
    HalfClosed,  // WritesDone completed, still reading (AsyncReadWriteStream only)
    Finishing,   // call is ending, waiting for the final status
};

// Protobuf message (or a type with the same interface) used as request or response
template<typename Message>
concept ProtobufMessage = std::default_initializable<Message> && std::movable<Message> &&
                          requires(Message& message, const Message& const_message) {
                              { const_message.ByteSizeLong() } -> std::convertible_to<std::size_t>;
                              message.Clear();
                          };

// Callable passed to open(): creates the call with an Async* stub method, e.g.
//   [&](auto* ctx, auto* cq, void* tag) { return stub->AsyncChat(ctx, cq, tag); }
template<typename Factory, typename StreamInterface>
concept AsyncStreamFactoryFor =
    std::invocable<Factory, grpc::ClientContext*, grpc::CompletionQueue*, void*> &&
    std::convertible_to<std::invoke_result_t<Factory, grpc::ClientContext*, grpc::CompletionQueue*, void*>,
                        std::unique_ptr<StreamInterface>>;

namespace detail
{

// Operation tags: each stream owns its completion queue, so small integers are unique.
// Declared outside the class templates to be usable with `using enum`.
enum class StreamTag : std::uintptr_t
{
    Start = 1,
    Read,
    Write,
    WritesDone,
    Finish,
};

// Event context of streams without a read side (no response handler)
struct NoEventContext
{
};

// gRPC client stream interface whose call ends with Finish
template<typename StreamInterface>
concept AsyncCallInterface = requires(StreamInterface& stream, grpc::Status* status, void* tag) {
    stream.Finish(status, tag);
};

// Core of a client streaming call: context, completion queue, stream, state, final status, release.
// Layers extend open(), close(), handleEvent() and enterFinishing() of their `Base` without virtual
// functions; the write side extends enterFinishing(), so it always sits below the read side:
//   AsyncWriteStream -> AsyncCallBase,  AsyncReadStream -> AsyncCallBase,
//   AsyncReadWriteStream -> AsyncReadStream -> AsyncWriteStream -> AsyncCallBase
template<AsyncCallInterface StreamInterface>
class AsyncCallBase
{
public:
    using AsyncStreamInterface = StreamInterface;

    AsyncCallBase(const AsyncCallBase&)            = delete;
    AsyncCallBase& operator=(const AsyncCallBase&) = delete;
    AsyncCallBase(AsyncCallBase&&)                 = delete;
    AsyncCallBase& operator=(AsyncCallBase&&)      = delete;

    // Cancels the active call and releases all resources.
    void close()
    {
        if (m_state != State::Closed)
            m_status = grpc::Status(grpc::StatusCode::CANCELLED, "Stream closed by the client");
        release();
    }

    // True while the call is active (including half-closed and finishing).
    [[nodiscard]] bool isOpen() const noexcept { return m_stream != nullptr; }

    // Final status of the last call: server status, CANCELLED after close(), OK while active.
    [[nodiscard]] const grpc::Status& status() const noexcept { return m_status; }

    // Current state of the call, for diagnostics and logging.
    [[nodiscard]] StreamState state() const noexcept { return m_state; }

    // The underlying gRPC stream; null when closed.
    [[nodiscard]] StreamInterface* getStream() const noexcept { return m_stream.get(); }

protected:
    using State = StreamState;
    using Tag   = StreamTag;

    AsyncCallBase() = default;
    ~AsyncCallBase() { close(); }

    // Opens a new call with a fresh ClientContext (it must not be reused between RPCs).
    // The factory may configure the context and must use the Async* stub flavour (starts the call).
    template<AsyncStreamFactoryFor<StreamInterface> StreamFactory>
    bool open(StreamFactory&& stream_factory)
    {
        close();

        m_context          = std::make_unique<grpc::ClientContext>();
        m_completion_queue = std::make_unique<grpc::CompletionQueue>();
        m_status           = grpc::Status::OK;

        m_stream = std::forward<StreamFactory>(stream_factory)(
            m_context.get(), m_completion_queue.get(), toTag(Tag::Start));
        if (!m_stream)
        {
            release();
            m_status = grpc::Status(grpc::StatusCode::INTERNAL, "Stream factory returned no stream");
            return false;
        }

        m_state         = State::Open;
        m_start_pending = true;
        return true;
    }

    // Passes up to max_events events to `handle_event(tag, ok)`: the first one waits until
    // `first_deadline`, the rest only take available events. Returns false once the call has ended.
    // An empty AsyncNext can cost the timer slack (50 us on Linux) when another thread polls gRPC,
    // so it is skipped while no operation is outstanding (except a first wait with a future deadline).
    template<typename EventHandler>
    bool processEvents(EventHandler&& handle_event, std::size_t max_events, const gpr_timespec& first_deadline)
    {
        const bool wait_for_first = gpr_time_cmp(first_deadline, gpr_inf_past(first_deadline.clock_type)) != 0;

        for (std::size_t i = 0; i < max_events && m_stream; ++i)
        {
            if (!hasPendingOperation() && (i > 0 || !wait_for_first))
                break;

            void* tag = nullptr;
            bool  ok  = false;

            // An expired deadline returns an available event or TIMEOUT
            const grpc::CompletionQueue::NextStatus next_status =
                i == 0 ? m_completion_queue->AsyncNext(&tag, &ok, first_deadline)
                       : m_completion_queue->AsyncNext(&tag, &ok, gpr_inf_past(GPR_CLOCK_MONOTONIC));
            if (next_status != grpc::CompletionQueue::GOT_EVENT)
                break;

            handle_event(fromTag(tag), ok);
        }

        return m_stream != nullptr;
    }

    // Handles events not handled above: start-call (no write side) and Finish.
    template<typename Context>
    void handleEvent(Tag tag, bool ok, Context& /*context*/)
    {
        assert((tag == Tag::Start || tag == Tag::Finish) && "an operation completed that no layer handles");
        switch (tag)
        {
            using enum StreamTag;
        case Start:
            if (!completeStart(ok)) [[unlikely]]
            {
                // Start failed: the pending read fails too, then Finish gets the error status
                enterFinishing();
                break;
            }
            tryFinish();
            break;

        case Finish:
            // m_status holds the final status from gRPC
            m_finish_pending = false;
            m_state          = State::Closed;
            release();
            break;

        default:
            break;
        }
    }

    // Marks the start-call as completed and returns its outcome.
    bool completeStart(bool ok) noexcept
    {
        m_start_pending = false;
        return ok;
    }

    // Ends the call and requests the final status once no operation is outstanding.
    void enterFinishing()
    {
        m_state = State::Finishing;
        tryFinish();
    }

    // Issues Finish exactly once, when finishing and no other operation is outstanding.
    void tryFinish()
    {
        if (m_state != State::Finishing || m_start_pending || m_read_pending || m_write_pending || m_finish_pending)
            return;

        m_stream->Finish(&m_status, toTag(Tag::Finish));
        m_finish_pending = true;
    }

    void setState(State state) noexcept { m_state = state; }

    // Stream of the active call, null when closed
    [[nodiscard]] StreamInterface* stream() const noexcept { return m_stream.get(); }

    // Outstanding operations reported by the layers
    [[nodiscard]] bool hasPendingOperation() const noexcept
    {
        return m_start_pending || m_read_pending || m_write_pending || m_finish_pending;
    }
    [[nodiscard]] bool isStartPending() const noexcept { return m_start_pending; }
    [[nodiscard]] bool isWriteOperationPending() const noexcept { return m_write_pending; }
    void               setWriteOperationPending(bool pending) noexcept { m_write_pending = pending; }
    void               setReadOperationPending(bool pending) noexcept { m_read_pending = pending; }

    // Called by the read side: such a call stays open after the half-close until reads end.
    void               registerReadSide() noexcept { m_has_read_side = true; }
    [[nodiscard]] bool hasReadSide() const noexcept { return m_has_read_side; }

    static void* toTag(Tag tag) noexcept
    {
        return reinterpret_cast<void*>(static_cast<std::underlying_type_t<Tag>>(tag));
    }

    // AsyncNext deadline on the monotonic clock: no per-call clock conversion, immune to wall clock changes.
    template<typename Rep, typename Period>
    static gpr_timespec deadlineAfter(std::chrono::duration<Rep, Period> timeout)
    {
        // Timeouts over a year mean "wait forever" (and must not overflow)
        if (timeout >= std::chrono::hours(24 * 365))
            return gpr_inf_future(GPR_CLOCK_MONOTONIC);
        const auto micros = std::chrono::ceil<std::chrono::microseconds>(timeout).count();
        if (micros <= 0)
            return gpr_inf_past(GPR_CLOCK_MONOTONIC);
        return gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                            gpr_time_from_micros(static_cast<std::int64_t>(micros), GPR_TIMESPAN));
    }

private:
    static Tag fromTag(void* tag) noexcept
    {
        const auto value = reinterpret_cast<std::uintptr_t>(tag);
        // Only this stream posts to its private completion queue
        assert(value >= static_cast<std::uintptr_t>(Tag::Start) && value <= static_cast<std::uintptr_t>(Tag::Finish) &&
               "unknown completion queue tag");
        return static_cast<Tag>(value);
    }

    // Tears the call down, keeping m_status.
    void release()
    {
        // Cancelling completes all outstanding operations with ok == false
        if (m_context)
            m_context->TryCancel();

        // Drain the queue before destroying what pending operations refer to
        if (m_completion_queue)
        {
            m_completion_queue->Shutdown();
            void* tag = nullptr;
            bool  ok  = false;
            while (m_completion_queue->Next(&tag, &ok))
            {
            }
        }

        // Destroy in dependency order: stream -> completion queue -> context
        m_stream.reset();
        m_completion_queue.reset();
        m_context.reset();

        m_start_pending  = false;
        m_read_pending   = false;
        m_write_pending  = false;
        m_finish_pending = false;
        m_state          = State::Closed;
    }

    std::unique_ptr<grpc::ClientContext>   m_context;
    std::unique_ptr<grpc::CompletionQueue> m_completion_queue;
    std::unique_ptr<StreamInterface>       m_stream;
    grpc::Status                           m_status;

    State m_state          = State::Closed;
    bool  m_has_read_side  = false;
    bool  m_start_pending  = false;
    bool  m_read_pending   = false;
    bool  m_write_pending  = false;
    bool  m_finish_pending = false;
};

} // namespace detail

} // namespace grpc_async_stream
