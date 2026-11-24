#pragma once

// Requires C++20 (concepts, `using enum`).
// Depends only on gRPC and the C++ standard library.
//
// The call core shared by AsyncWriteStream, AsyncReadStream and
// AsyncReadWriteStream. Applications use those classes; this header only
// holds what they have in common.

#include <grpc/support/time.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>

#include <cassert>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

namespace grpc_utils
{

// States of a call, as reported by state()
enum class StreamState
{
    Closed,      // no call
    Open,        // writing and/or reading
    HalfClosing, // writesDone() requested, flushing the queued requests before WritesDone
    HalfClosed,  // WritesDone completed, still reading (AsyncReadWriteStream only)
    Finishing,   // waiting for the final status: the server ended the stream, an operation
                 // failed, or the half-close of a write-only stream completed
};

// A protobuf message (or a type with the same interface) usable as request or response
template<typename Message>
concept ProtobufMessage = std::default_initializable<Message> && std::movable<Message> &&
                          requires(Message& message, const Message& const_message) {
                              { const_message.ByteSizeLong() } -> std::convertible_to<std::size_t>;
                              message.Clear();
                          };

// The callable passed to open(): creates the call with an Async* stub method
// and returns the gRPC stream interface `StreamInterface`, e.g.
//   [&](auto* ctx, auto* cq, void* tag) { return stub->AsyncChat(ctx, cq, tag); }
template<typename Factory, typename StreamInterface>
concept AsyncStreamFactoryFor =
    std::invocable<Factory, grpc::ClientContext*, grpc::CompletionQueue*, void*> &&
    std::convertible_to<std::invoke_result_t<Factory, grpc::ClientContext*, grpc::CompletionQueue*, void*>,
                        std::unique_ptr<StreamInterface>>;

namespace detail
{

// Operation tags. Each stream owns its completion queue, so small integers are
// sufficient as unique tags and do not depend on member addresses. Declared
// outside the class templates so that the enum is not a dependent type
// (required for `using enum`).
enum class StreamTag : std::uintptr_t
{
    Start = 1,
    Read,
    Write,
    WritesDone,
    Finish,
};

// Context of the events of streams without a read side (no response handler)
struct NoEventContext
{
};

// A gRPC client stream interface whose call ends with Finish
template<typename StreamInterface>
concept AsyncCallInterface = requires(StreamInterface& stream, grpc::Status* status, void* tag) {
    stream.Finish(status, tag);
};

// The core of a client streaming call: the per-call context, the private
// completion queue, the stream created by the factory, the call state, the
// final status (Finish, issued exactly once when no operation is outstanding),
// cancellation and the release of all resources.
//
// The write side (AsyncWriteStream) and the read side (AsyncReadStream) are
// layered over this class by single inheritance, each taking the class below it
// as its `Base` template parameter:
//
//   AsyncWriteStream<Req>                AsyncWriteStream -> AsyncCallBase<ClientAsyncWriterInterface>
//   AsyncReadStream<Resp>                AsyncReadStream  -> AsyncCallBase<ClientAsyncReaderInterface>
//   AsyncReadWriteStream<Req, Resp>      AsyncReadStream  -> AsyncWriteStream -> AsyncCallBase<ClientAsyncReaderWriterInterface>
//
// The layers cooperate without virtual functions, through member functions of
// the same name that each layer extends and that resolve at compile time to
// the nearest layer below the caller:
//   - open() / close(): a layer calls Base::open() / Base::close() and then
//     starts / releases its own operations and buffers;
//   - handleEvent(): a layer handles the completions of its own operations and
//     passes the others to Base::handleEvent(); this class handles the
//     start-call (for streams without a write side) and Finish;
//   - enterFinishing(): the write side extends it to discard its queue, and is
//     therefore always placed below the read side.
// Each layer reports its outstanding operations to this class, which requests
// Finish once the call is ending and none is outstanding.
template<AsyncCallInterface StreamInterface>
class AsyncCallBase
{
public:
    using AsyncStreamInterface = StreamInterface;

    AsyncCallBase(const AsyncCallBase&)            = delete;
    AsyncCallBase& operator=(const AsyncCallBase&) = delete;
    AsyncCallBase(AsyncCallBase&&)                 = delete;
    AsyncCallBase& operator=(AsyncCallBase&&)      = delete;

    // Cancels the call (if still active) and releases all resources.
    void close()
    {
        if (m_state != State::Closed)
            m_status = grpc::Status(grpc::StatusCode::CANCELLED, "Stream closed by the client");
        release();
    }

    // True while the call is active (including half-closed / finishing states).
    [[nodiscard]] bool isOpen() const noexcept { return m_stream != nullptr; }

    // Final status of the last call: the server status after a regular end of the
    // stream, CANCELLED after a client-side close(), OK while the call is active.
    [[nodiscard]] const grpc::Status& status() const noexcept { return m_status; }

    // Current state of the call, for diagnostics and logging (see StreamState).
    [[nodiscard]] StreamState state() const noexcept { return m_state; }

    // The underlying gRPC stream; null when closed.
    [[nodiscard]] StreamInterface* getStream() const noexcept { return m_stream.get(); }

protected:
    using State = StreamState;
    using Tag   = StreamTag;

    AsyncCallBase() = default;
    ~AsyncCallBase() { close(); }

    // Opens a new call. A fresh grpc::ClientContext is created for every call,
    // because a ClientContext must never be reused for a second RPC. The factory
    // may configure the context (metadata, deadline, ...) and must create the
    // stream with the Async* stub flavour, which calls StartCall internally.
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

    // Processes up to max_events events and passes each to `handle_event(tag, ok)`.
    // The first AsyncNext waits until `first_deadline` (gpr_inf_past: does not
    // wait), the following ones only take events that are already available.
    // Returns false once the call has ended and the stream has been released.
    //
    // An AsyncNext that finds no event is not free: when another thread of the
    // process is gRPC's designated poller (a gRPC server, or the thread of
    // another stream), gRPC waits on a condition variable even for an expired
    // deadline, and Linux extends that zero-timeout wait by the thread's timer
    // slack, 50 us by default. So no AsyncNext is made when no operation is
    // outstanding, since no event can arrive then, except for a first one with
    // a future deadline, which keeps pollFor() pacing the caller's loop.
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

            // An already expired deadline makes AsyncNext return GOT_EVENT if an
            // event is available, or TIMEOUT otherwise
            const grpc::CompletionQueue::NextStatus next_status =
                i == 0 ? m_completion_queue->AsyncNext(&tag, &ok, first_deadline)
                       : m_completion_queue->AsyncNext(&tag, &ok, gpr_inf_past(GPR_CLOCK_MONOTONIC));
            if (next_status != grpc::CompletionQueue::GOT_EVENT)
                break;

            handle_event(fromTag(tag), ok);
        }

        return m_stream != nullptr;
    }

    // Handles the events no layer above has handled: the start-call of streams
    // without a write side, and Finish.
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
                // The call could not be started. An outstanding read fails as
                // well, after which Finish retrieves the actual error status.
                enterFinishing();
                break;
            }
            tryFinish();
            break;

        case Finish:
            // m_status now holds the final status reported by gRPC
            m_finish_pending = false;
            m_state          = State::Closed;
            release();
            break;

        default:
            break;
        }
    }

    // The start-call has completed: returns its outcome.
    bool completeStart(bool ok) noexcept
    {
        m_start_pending = false;
        return ok;
    }

    // Stops the call (the server ended it or an operation failed) and requests
    // the final status once no operation is outstanding.
    void enterFinishing()
    {
        m_state = State::Finishing;
        tryFinish();
    }

    // Finish is issued exactly once, after the call has started to end and no
    // start-call, read or write operation is outstanding.
    void tryFinish()
    {
        if (m_state != State::Finishing || m_start_pending || m_read_pending || m_write_pending || m_finish_pending)
            return;

        m_stream->Finish(&m_status, toTag(Tag::Finish));
        m_finish_pending = true;
    }

    void setState(State state) noexcept { m_state = state; }

    // The stream of the active call, nullptr when closed
    [[nodiscard]] StreamInterface* stream() const noexcept { return m_stream.get(); }

    // Outstanding operations of the layers
    [[nodiscard]] bool hasPendingOperation() const noexcept
    {
        return m_start_pending || m_read_pending || m_write_pending || m_finish_pending;
    }
    [[nodiscard]] bool isStartPending() const noexcept { return m_start_pending; }
    [[nodiscard]] bool isWriteOperationPending() const noexcept { return m_write_pending; }
    void               setWriteOperationPending(bool pending) noexcept { m_write_pending = pending; }
    void               setReadOperationPending(bool pending) noexcept { m_read_pending = pending; }

    // Called by the constructor of the read side: a call with a read side stays
    // open after the half-close until the reads have ended.
    void               registerReadSide() noexcept { m_has_read_side = true; }
    [[nodiscard]] bool hasReadSide() const noexcept { return m_has_read_side; }

    static void* toTag(Tag tag) noexcept
    {
        return reinterpret_cast<void*>(static_cast<std::underlying_type_t<Tag>>(tag));
    }

    // Deadline for AsyncNext on the monotonic clock: gRPC would otherwise convert
    // a system_clock deadline to its monotonic clock on every call, and the wait
    // must not be stretched or cut short by changes of the wall clock.
    template<typename Rep, typename Period>
    static gpr_timespec deadlineAfter(std::chrono::duration<Rep, Period> timeout)
    {
        // Timeouts beyond a year mean "wait for the first event" (and must not overflow)
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
        // Only this stream posts to its private completion queue: anything else is a bug
        assert(value >= static_cast<std::uintptr_t>(Tag::Start) && value <= static_cast<std::uintptr_t>(Tag::Finish) &&
               "unknown completion queue tag");
        return static_cast<Tag>(value);
    }

    // Tears the call down without touching m_status.
    void release()
    {
        // Cancelling completes all outstanding operations with ok == false
        if (m_context)
            m_context->TryCancel();

        // Drain the queue BEFORE destroying anything the pending operations refer to
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

} // namespace grpc_utils
