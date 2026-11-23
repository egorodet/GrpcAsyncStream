#pragma once

// Requires C++20 (concepts, designated initializers, defaulted comparisons).
// Depends only on gRPC and the C++ standard library.

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

namespace grpc_utils
{

// What happens to a request that does not fit into the bounded write queue.
enum class WriteOverflowPolicy
{
    // Keep the queued requests and discard the new one (sendRequest returns
    // SendResult::Dropped). Preserves the oldest data.
    DropNewest,
    // Discard as many of the oldest queued requests as needed to make room for
    // the new one (sendRequest returns SendResult::QueuedDroppedOldest). Keeps
    // the most recent data, which suits telemetry-like streams.
    DropOldest,
};


// Limits of the queue of requests waiting to be written into the stream.
//
// gRPC allows only one outstanding write per call, so requests sent while a
// write is in flight are buffered in this queue. Without a limit a producer
// faster than the network (or a slow / stalled server) grows the queue without
// bound until memory is exhausted. The queue is bounded both by the number of
// messages and by their total serialized size; whichever limit is hit first
// triggers the overflow policy. The single request currently being written is
// not part of the queue, so at most `max_messages + 1` requests are held.
//
// Configure with designated initializers, e.g.
//   WriteQueueLimits{.max_messages = 256, .overflow_policy = WriteOverflowPolicy::DropNewest}
struct WriteQueueLimits
{
    static constexpr std::size_t unlimited = std::numeric_limits<std::size_t>::max();

    // Maximum number of buffered requests; 0 disables buffering altogether
    // (a request is only accepted when it can be written immediately).
    std::size_t max_messages = 1024;
    // Maximum total serialized size (ByteSizeLong) of buffered requests in bytes.
    std::size_t max_bytes = 64U * 1024U * 1024U;
    WriteOverflowPolicy overflow_policy = WriteOverflowPolicy::DropOldest;

    [[nodiscard]] static constexpr WriteQueueLimits unbounded() noexcept
    {
        return {.max_messages = unlimited, .max_bytes = unlimited, .overflow_policy = WriteOverflowPolicy::DropNewest};
    }

    friend constexpr bool operator==(const WriteQueueLimits&, const WriteQueueLimits&) = default;
};

// Counters of the write queue for the current (or the last) call. Reset by open().
struct WriteQueueStats
{
    std::size_t   queued_messages      = 0; // requests currently waiting in the queue
    std::size_t   queued_bytes         = 0; // their total serialized size
    std::size_t   peak_queued_messages = 0; // high-water mark of queued_messages
    std::size_t   peak_queued_bytes    = 0; // high-water mark of queued_bytes
    std::uint64_t submitted_messages   = 0; // requests passed to sendRequest while the stream was writable
    std::uint64_t written_messages     = 0; // writes completed successfully
    std::uint64_t dropped_messages     = 0; // requests discarded by the overflow policy
    std::uint64_t dropped_bytes        = 0; // their total serialized size
    std::uint64_t discarded_messages   = 0; // queued or in-flight requests lost because the call ended or was closed
    bool          write_in_flight      = false; // a request write is currently outstanding

    // Invariant: submitted == written + dropped + discarded + queued + (write_in_flight ? 1 : 0)
    [[nodiscard]] constexpr bool isConsistent() const noexcept
    {
        return submitted_messages ==
               written_messages + dropped_messages + discarded_messages + queued_messages + (write_in_flight ? 1U : 0U);
    }

    friend constexpr bool operator==(const WriteQueueStats&, const WriteQueueStats&) = default;
};

// Outcome of AsyncReadWriteStream::sendRequest().
enum class SendResult
{
    Written,             // the write has been started immediately
    Queued,              // the request has been buffered and will be written later
    QueuedDroppedOldest, // buffered after discarding older requests (DropOldest policy)
    Dropped,             // the request has been discarded: the queue is full (DropNewest policy),
                         // the request alone exceeds max_bytes, or buffering is disabled
    NotWritable,         // the stream does not accept writes (not open, half-closed or ending)
};

// True when the request will be written into the stream (unless the call ends first).
[[nodiscard("check whether the request was accepted")]] constexpr bool isAccepted(SendResult result) noexcept
{
    using enum SendResult;
    return result == Written || result == Queued || result == QueuedDroppedOldest;
}

// States of a call, as reported by AsyncReadWriteStream::state()
enum class StreamState
{
    Closed,      // no call
    Open,        // reading and writing
    HalfClosing, // writesDone() requested, flushing the queued requests before WritesDone
    HalfClosed,  // WritesDone issued, still reading
    Finishing,   // server ended the stream or a write failed, waiting for the final status
};

// ---------------------------------------------------------------------------
// Implementation details
// ---------------------------------------------------------------------------

namespace detail
{

// Operation tags. Each stream owns its completion queue, so small integers are
// sufficient as unique tags and do not depend on member addresses. Declared
// outside the class template so that the enum is not a dependent type
// (required for `using enum`).
enum class StreamTag : std::uintptr_t
{
    Start = 1,
    Read,
    Write,
    WritesDone,
    Finish,
};

} // namespace detail

// ---------------------------------------------------------------------------
// Concepts
// ---------------------------------------------------------------------------

// A protobuf message (or a type with the same interface) usable as request or response
template<typename Message>
concept ProtobufMessage = std::default_initializable<Message> && std::movable<Message> &&
                          requires(Message& message, const Message& const_message) {
                              { const_message.ByteSizeLong() } -> std::convertible_to<std::size_t>;
                              message.Clear();
                          };

// The callable passed to AsyncReadWriteStream::open(): creates the call with an
// Async* stub method, e.g. [&](auto* ctx, auto* cq, void* tag) { return stub->AsyncChat(ctx, cq, tag); }
template<typename Factory, typename RequestProto, typename ResponseProto>
concept StreamFactoryFor =
    std::invocable<Factory, grpc::ClientContext*, grpc::CompletionQueue*, void*> &&
    std::convertible_to<std::invoke_result_t<Factory, grpc::ClientContext*, grpc::CompletionQueue*, void*>,
                        std::unique_ptr<grpc::ClientAsyncReaderWriterInterface<RequestProto, ResponseProto>>>;

// The callable receiving responses in AsyncReadWriteStream::poll() / pollFor()
template<typename Handler, typename ResponseProto>
concept ResponseHandlerFor = std::invocable<Handler&, ResponseProto&&>;

// A wrapper over the asynchronous bidirectional client stream
// (grpc::ClientAsyncReaderWriterInterface) that encapsulates the generic gRPC
// async stream machinery: the completion queue, the operation tags, the
// non-blocking event polling, write serialization with a bounded write queue,
// half-close and the final status retrieval (Finish). The protocol-specific
// parts (creating the stream via the stub method and processing the received
// responses) are provided by the caller.
//
// Threading: NOT thread-safe. open(), close(), sendRequest(), writesDone(),
// setWriteQueueLimits() and poll() must all be called from the same thread.
template<ProtobufMessage RequestProto, ProtobufMessage ResponseProto>
class AsyncReadWriteStream
{
public:
    using AsyncReaderWriterInterface = ::grpc::ClientAsyncReaderWriterInterface<RequestProto, ResponseProto>;
    using Request                    = RequestProto;
    using Response                   = ResponseProto;

    AsyncReadWriteStream() = default;
    explicit AsyncReadWriteStream(const WriteQueueLimits& limits)
        : m_limits(limits)
    {
    }
    ~AsyncReadWriteStream() { close(); }

    AsyncReadWriteStream(const AsyncReadWriteStream&)            = delete;
    AsyncReadWriteStream& operator=(const AsyncReadWriteStream&) = delete;
    AsyncReadWriteStream(AsyncReadWriteStream&&)                 = delete;
    AsyncReadWriteStream& operator=(AsyncReadWriteStream&&)      = delete;

    // Opens a new stream. A fresh grpc::ClientContext is created for every call,
    // because a ClientContext must never be reused for a second RPC. The factory
    // may configure the context (metadata, deadline, ...) and must create the
    // stream with the Async* stub flavour, which calls StartCall internally:
    //
    //   stream.open([&](grpc::ClientContext* ctx, grpc::CompletionQueue* cq, void* tag) {
    //       return stub->AsyncOpenRadarStream(ctx, cq, tag);
    //   });
    template<StreamFactoryFor<RequestProto, ResponseProto> StreamFactory>
    bool open(StreamFactory&& stream_factory)
    {
        close();

        m_context          = std::make_unique<grpc::ClientContext>();
        m_completion_queue = std::make_unique<grpc::CompletionQueue>();
        m_status           = grpc::Status::OK;
        m_stats            = {};

        m_stream = std::forward<StreamFactory>(stream_factory)(
            m_context.get(), m_completion_queue.get(), toTag(Tag::Start));
        if (!m_stream)
        {
            release();
            m_status = grpc::Status(grpc::StatusCode::INTERNAL, "Stream factory returned no stream");
            return false;
        }

        m_state = State::Open;

        // StartCall sends the initial metadata through the stream's internal write
        // operation set, which is shared with Write() and WritesDone(). Until the
        // start-call tag completes, that set is in flight, and issuing a Write
        // would submit SEND_INITIAL_METADATA a second time, which gRPC rejects with
        // GRPC_CALL_ERROR_TOO_MANY_OPERATIONS. The start-call is therefore tracked
        // as an outstanding write; requests sent meanwhile are queued.
        // NOTE: do not cork the initial metadata (set_initial_metadata_corked) on
        // the context, since then the start-call tag never completes.
        m_write_in_progress = true;

        // Exactly one read is kept outstanding for the whole lifetime of the call,
        // so server messages and the end of the stream are always reported.
        startRead();
        return true;
    }

    // Cancels the call (if still active) and releases all resources. Requests
    // still waiting in the write queue are discarded (see WriteQueueStats).
    void close()
    {
        if (m_state != State::Closed)
            m_status = grpc::Status(grpc::StatusCode::CANCELLED, "Stream closed by the client");
        release();
    }

    // True while the call is active (including half-closed / finishing states).
    [[nodiscard]] bool isOpen() const noexcept { return m_stream != nullptr; }

    // True while new requests may be sent.
    [[nodiscard]] bool canWrite() const noexcept { return m_state == State::Open; }

    // Final status of the last call: the server status after a regular end of the
    // stream, CANCELLED after a client-side close(), OK while the call is active.
    [[nodiscard]] const grpc::Status& status() const noexcept { return m_status; }

    [[nodiscard]] AsyncReaderWriterInterface* getReaderWriter() const noexcept { return m_stream.get(); }

    // Write queue limits; may be changed at any time. Tightening the limits
    // immediately applies the overflow policy to the already queued requests.
    [[nodiscard]] const WriteQueueLimits& writeQueueLimits() const noexcept { return m_limits; }
    void setWriteQueueLimits(const WriteQueueLimits& limits)
    {
        m_limits = limits;
        while (!m_pending_writes.empty() && (m_stats.queued_messages > m_limits.max_messages ||
                                             m_stats.queued_bytes > m_limits.max_bytes))
        {
            if (m_limits.overflow_policy == WriteOverflowPolicy::DropOldest)
                dropPendingFront();
            else
                dropPendingBack();
        }
    }

    [[nodiscard]] const WriteQueueStats& writeQueueStats() const noexcept { return m_stats; }

    // True while a write (or the start-call / half-close) is outstanding.
    [[nodiscard]] bool isWriteInProgress() const noexcept { return m_write_in_progress; }

    // Current state of the call, for diagnostics and logging (see StreamState).
    [[nodiscard]] StreamState state() const noexcept { return m_state; }

    // Enqueues the given request to be written into the stream. Only one write
    // may be outstanding at a time, and none before the start-call has
    // completed; the rest are queued in a bounded queue (see WriteQueueLimits)
    // and sent as the completions arrive in poll(). Requests not fitting into
    // the queue are discarded according to the overflow policy, so memory use
    // stays bounded however fast requests are produced.
    SendResult sendRequest(RequestProto request)
    {
        if (m_state != State::Open)
            return SendResult::NotWritable;

        ++m_stats.submitted_messages;
        if (!m_write_in_progress) [[likely]]
        {
            startWrite(std::move(request));
            return SendResult::Written;
        }

        return enqueue(std::move(request));
    }

    // Half-closes the stream from the client side: already queued requests are
    // still sent, then WritesDone is issued. Responses keep being delivered until
    // the server ends the call.
    bool writesDone()
    {
        if (m_state != State::Open)
            return false;

        m_state = State::HalfClosing;
        if (!m_write_in_progress)
            startWritesDone();
        return true;
    }

    // Non-blocking processing of up to max_events currently available stream
    // events. Received responses are passed one by one to the response handler,
    // which may call sendRequest(), writesDone() or close(). Returns false once
    // the call has ended (status() then holds the reason), true otherwise.
    template<ResponseHandlerFor<ResponseProto> ResponseHandler>
    bool poll(ResponseHandler&& response_handler, std::size_t max_events = 64)
    {
        // An already expired deadline makes AsyncNext return immediately with
        // GOT_EVENT if an event is available, or TIMEOUT otherwise
        return process(std::forward<ResponseHandler>(response_handler), max_events,
                       gpr_inf_past(GPR_CLOCK_MONOTONIC));
    }

    // Like poll(), but waits up to `timeout` for the first event, which avoids
    // busy polling in a dedicated stream thread. Further already available
    // events are processed without waiting.
    template<ResponseHandlerFor<ResponseProto> ResponseHandler, typename Rep, typename Period>
    bool pollFor(ResponseHandler&& response_handler, std::chrono::duration<Rep, Period> timeout,
                 std::size_t max_events = 64)
    {
        const auto deadline = std::chrono::system_clock::now() +
                              std::chrono::duration_cast<std::chrono::system_clock::duration>(timeout);
        return process(std::forward<ResponseHandler>(response_handler), max_events, deadline);
    }

private:
    using State = StreamState;
    using Tag   = detail::StreamTag;

    struct PendingWrite
    {
        RequestProto request;
        std::size_t  bytes;
    };

    static void* toTag(Tag tag) noexcept
    {
        return reinterpret_cast<void*>(static_cast<std::underlying_type_t<Tag>>(tag));
    }

    static Tag fromTag(void* tag) noexcept
    {
        const auto value = reinterpret_cast<std::uintptr_t>(tag);
        // Only this stream posts to its private completion queue: anything else is a bug
        assert(value >= static_cast<std::uintptr_t>(Tag::Start) && value <= static_cast<std::uintptr_t>(Tag::Finish) &&
               "unknown completion queue tag");
        return static_cast<Tag>(value);
    }

    template<typename ResponseHandler, typename Deadline>
    bool process(ResponseHandler&& response_handler, std::size_t max_events, const Deadline& first_deadline)
    {
        for (std::size_t i = 0; i < max_events && m_stream; ++i)
        {
            void* tag = nullptr;
            bool  ok  = false;

            const grpc::CompletionQueue::NextStatus next_status =
                i == 0 ? m_completion_queue->AsyncNext(&tag, &ok, first_deadline)
                       : m_completion_queue->AsyncNext(&tag, &ok, gpr_inf_past(GPR_CLOCK_MONOTONIC));
            if (next_status != grpc::CompletionQueue::GOT_EVENT)
                break;

            if (!handleEvent(fromTag(tag), ok, response_handler))
                return false;
        }

        return m_stream != nullptr;
    }

    // Returns false when the call has ended and the stream has been released.
    template<typename ResponseHandler>
    bool handleEvent(Tag tag, bool ok, ResponseHandler& response_handler)
    {
        switch (tag)
        {
            using enum detail::StreamTag;
        case Start:
            // The initial metadata has been sent and the write operation set is
            // free again, so the requests queued since open() can be sent now.
            m_write_in_progress = false;
            if (!ok) [[unlikely]]
            {
                // The call could not be started. The outstanding read fails as
                // well, after which Finish retrieves the actual error status.
                enterFinishing();
                break;
            }
            onWriteSlotFree();
            break;

        case Read:
            m_read_in_progress = false;
            if (!ok) [[unlikely]]
            {
                // No more messages from the server: stop writing and collect
                // the final status once all outstanding operations are done.
                enterFinishing();
                break;
            }
            {
                // Move the message out before re-arming the read: the read
                // buffer is overwritten by the next read, and the handler may
                // keep the message.
                ResponseProto response = std::move(m_pending_response);
                m_pending_response.Clear();
                startRead();
                response_handler(std::move(response));
            }
            break;

        case Write:
            m_write_in_progress     = false;
            m_stats.write_in_flight = false;
            if (!ok) [[unlikely]]
            {
                // The call is broken, further writes would fail as well
                ++m_stats.discarded_messages;
                enterFinishing();
                break;
            }
            ++m_stats.written_messages;
            // The written message is no longer needed by gRPC: free its memory now
            m_current_write.Clear();
            onWriteSlotFree();
            break;

        case WritesDone:
            m_write_in_progress = false;
            if (m_state == State::HalfClosing)
                m_state = State::HalfClosed;
            tryFinish();
            break;

        case Finish:
            // m_status now holds the final status reported by gRPC
            m_finish_in_progress = false;
            m_state              = State::Closed;
            release();
            return false;
        }
        return true;
    }

    SendResult enqueue(RequestProto request)
    {
        const std::size_t bytes = request.ByteSizeLong();

        // A request that cannot fit even into an empty queue is always dropped
        if (m_limits.max_messages == 0 || bytes > m_limits.max_bytes)
            return dropRequest(bytes);

        bool dropped_oldest = false;
        while (!fitsIntoQueue(bytes))
        {
            if (m_limits.overflow_policy == WriteOverflowPolicy::DropNewest || m_pending_writes.empty())
                return dropRequest(bytes);

            dropPendingFront();
            dropped_oldest = true;
        }

        m_pending_writes.push_back({.request = std::move(request), .bytes = bytes});
        ++m_stats.queued_messages;
        m_stats.queued_bytes += bytes;
        m_stats.peak_queued_messages = std::max(m_stats.peak_queued_messages, m_stats.queued_messages);
        m_stats.peak_queued_bytes    = std::max(m_stats.peak_queued_bytes, m_stats.queued_bytes);
        m_queue_peak_since_shrink    = std::max(m_queue_peak_since_shrink, m_stats.queued_messages);
        return dropped_oldest ? SendResult::QueuedDroppedOldest : SendResult::Queued;
    }

    [[nodiscard]] bool fitsIntoQueue(std::size_t bytes) const
    {
        // Written without additions that could overflow with "unlimited" limits
        return m_stats.queued_messages < m_limits.max_messages &&
               m_stats.queued_bytes <= m_limits.max_bytes && bytes <= m_limits.max_bytes - m_stats.queued_bytes;
    }

    SendResult dropRequest(std::size_t bytes)
    {
        ++m_stats.dropped_messages;
        m_stats.dropped_bytes += bytes;
        return SendResult::Dropped;
    }

    void dropPendingFront()
    {
        dropRequest(m_pending_writes.front().bytes);
        popPendingFront();
    }

    void dropPendingBack()
    {
        const std::size_t bytes = m_pending_writes.back().bytes;
        dropRequest(bytes);
        --m_stats.queued_messages;
        m_stats.queued_bytes -= bytes;
        m_pending_writes.pop_back();
        shrinkQueueIfEmpty();
    }

    RequestProto popPendingFront()
    {
        PendingWrite pending = std::move(m_pending_writes.front());
        m_pending_writes.pop_front();
        --m_stats.queued_messages;
        m_stats.queued_bytes -= pending.bytes;
        shrinkQueueIfEmpty();
        return std::move(pending.request);
    }

    // std::deque keeps its spare blocks after being drained; release them once
    // a load peak is over so that the memory footprint returns to the baseline.
    // Small fluctuations of the queue are ignored to avoid reallocation churn.
    void shrinkQueueIfEmpty()
    {
        static constexpr std::size_t shrink_threshold = 256;
        if (m_pending_writes.empty() && m_queue_peak_since_shrink > shrink_threshold)
        {
            std::deque<PendingWrite>().swap(m_pending_writes);
            m_queue_peak_since_shrink = 0;
        }
    }

    void discardPendingWrites()
    {
        m_stats.discarded_messages += m_pending_writes.size();
        m_stats.queued_messages   = 0;
        m_stats.queued_bytes      = 0;
        m_queue_peak_since_shrink = 0;
        std::deque<PendingWrite>().swap(m_pending_writes);
    }

    void startRead()
    {
        m_stream->Read(&m_pending_response, toTag(Tag::Read));
        m_read_in_progress = true;
    }

    void startWrite(RequestProto request)
    {
        // The message is kept alive in a member until the write completes
        m_current_write = std::move(request);
        m_stream->Write(m_current_write, toTag(Tag::Write));
        m_write_in_progress     = true;
        m_stats.write_in_flight = true;
    }

    void startWritesDone()
    {
        m_stream->WritesDone(toTag(Tag::WritesDone));
        m_write_in_progress = true;
    }

    // Called when the write operation set becomes free (start-call or a write
    // completed successfully): sends the next queued request, the pending
    // half-close, or the Finish once the stream is ending.
    void onWriteSlotFree()
    {
        if (!m_pending_writes.empty())
        {
            startWrite(popPendingFront());
        }
        else if (m_state == State::HalfClosing)
        {
            startWritesDone();
        }
        else
        {
            tryFinish();
        }
    }

    void enterFinishing()
    {
        m_state = State::Finishing;
        discardPendingWrites();
        tryFinish();
    }

    // Finish is issued exactly once, after the read side has ended and no write
    // operation is outstanding.
    void tryFinish()
    {
        if (m_state != State::Finishing || m_read_in_progress || m_write_in_progress || m_finish_in_progress)
            return;

        m_stream->Finish(&m_status, toTag(Tag::Finish));
        m_finish_in_progress = true;
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

        discardPendingWrites();
        if (m_stats.write_in_flight)
        {
            // Cancelled before the write was confirmed
            ++m_stats.discarded_messages;
            m_stats.write_in_flight = false;
        }
        m_current_write.Clear();
        m_pending_response.Clear();
        m_read_in_progress   = false;
        m_write_in_progress  = false;
        m_finish_in_progress = false;
        m_state              = State::Closed;
    }

    std::unique_ptr<grpc::ClientContext>        m_context;
    std::unique_ptr<grpc::CompletionQueue>      m_completion_queue;
    std::unique_ptr<AsyncReaderWriterInterface> m_stream;

    ResponseProto            m_pending_response;
    RequestProto             m_current_write;
    std::deque<PendingWrite> m_pending_writes;
    grpc::Status             m_status;

    WriteQueueLimits m_limits;
    WriteQueueStats  m_stats;
    std::size_t      m_queue_peak_since_shrink = 0;

    State m_state              = State::Closed;
    bool  m_read_in_progress   = false;
    bool  m_write_in_progress  = false;
    bool  m_finish_in_progress = false;
};

} // namespace grpc_utils
