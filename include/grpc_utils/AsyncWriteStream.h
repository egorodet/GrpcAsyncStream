#pragma once

// Requires C++20 (concepts, designated initializers, defaulted comparisons).
// Depends only on gRPC and the C++ standard library.

#include <grpc_utils/AsyncCallBase.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>

#include <algorithm>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

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

// Outcome of sendRequest().
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

namespace detail
{

// FIFO queue on a ring buffer whose capacity is a power of two and grows by
// doubling. Unlike std::deque it does not allocate per element (MSVC's deque
// allocates one block per element larger than 8 bytes), so pushing and popping
// in steady state allocate nothing. Popped slots are left in the moved-from
// state; release() frees the storage.
template<typename T>
    requires std::default_initializable<T> && std::movable<T>
class RingQueue
{
public:
    [[nodiscard]] bool        empty() const noexcept { return m_size == 0; }
    [[nodiscard]] std::size_t size() const noexcept { return m_size; }

    [[nodiscard]] T& front() noexcept { return m_slots[m_head]; }

    void pushBack(T&& value)
    {
        if (m_size == m_slots.size())
            grow();
        m_slots[slot(m_size)] = std::move(value);
        ++m_size;
    }

    T popFront()
    {
        T value = std::move(m_slots[m_head]);
        m_head  = slot(1);
        --m_size;
        return value;
    }

    T popBack()
    {
        --m_size;
        return std::move(m_slots[slot(m_size)]);
    }

    // Destroys all elements and frees the storage.
    void release() noexcept
    {
        std::vector<T>().swap(m_slots);
        m_head = 0;
        m_size = 0;
    }

private:
    static constexpr std::size_t initial_capacity = 16;

    [[nodiscard]] std::size_t slot(std::size_t offset) const noexcept
    {
        return (m_head + offset) & (m_slots.size() - 1);
    }

    void grow()
    {
        std::vector<T> slots(m_slots.empty() ? initial_capacity : m_slots.size() * 2);
        for (std::size_t i = 0; i < m_size; ++i)
            slots[i] = std::move(m_slots[slot(i)]);
        m_slots.swap(slots);
        m_head = 0;
    }

    std::vector<T> m_slots;
    std::size_t    m_head = 0;
    std::size_t    m_size = 0;
};

} // namespace detail

// The write side of an asynchronous client stream: satisfied by
// grpc::ClientAsyncWriterInterface<Request> (client-streaming calls) and
// grpc::ClientAsyncReaderWriterInterface<Request, Response> (bidirectional calls)
template<typename StreamInterface, typename RequestProto>
concept AsyncWriterInterfaceFor =
    requires(StreamInterface& stream, const RequestProto& request, grpc::Status* status, void* tag) {
        stream.Write(request, tag);
        stream.WritesDone(tag);
        stream.Finish(status, tag);
    };

// A wrapper over the asynchronous client-streaming call
// (grpc::ClientAsyncWriterInterface) that encapsulates the generic gRPC async
// write machinery: write serialization with a bounded write queue, half-close
// and the final status retrieval, on top of the call core (detail::AsyncCallBase:
// completion queue, operation tags, non-blocking event polling, cancellation).
// The protocol-specific part (creating the call via the stub method) is
// provided by the caller.
//
// The single response of a client-streaming call is written by gRPC into the
// message passed to the Async* stub method in the stream factory. That message
// must outlive the call (until poll() returns false, or close() / open() /
// the destructor returns) and holds the server response once the call has
// ended with an OK status().
//
// The server can only end a client-streaming call by returning its final
// status, which the client receives with Finish. Finish is requested after
// the half-close (writesDone()), or as soon as a write fails because the call
// has ended (the server returned early, the deadline expired, ...). An idle
// stream therefore notices that the call has ended with its next sendRequest()
// or writesDone().
//
// `Base` is the call core by default. AsyncReadWriteStream layers the read side
// (AsyncReadStream) over this class with the bidirectional stream interface;
// the write side always sits directly on the core.
//
// Threading: NOT thread-safe. open(), close(), sendRequest(), writesDone(),
// setWriteQueueLimits() and poll() must all be called from the same thread.
template<ProtobufMessage RequestProto,
         typename Base = detail::AsyncCallBase<grpc::ClientAsyncWriterInterface<RequestProto>>>
    requires std::same_as<Base, detail::AsyncCallBase<typename Base::AsyncStreamInterface>> &&
             AsyncWriterInterfaceFor<typename Base::AsyncStreamInterface, RequestProto>
class AsyncWriteStream : public Base
{
public:
    using Request = RequestProto;

    AsyncWriteStream() = default;
    explicit AsyncWriteStream(const WriteQueueLimits& limits)
        : m_limits(limits)
    {
    }
    ~AsyncWriteStream() { close(); }

    AsyncWriteStream(const AsyncWriteStream&)            = delete;
    AsyncWriteStream& operator=(const AsyncWriteStream&) = delete;
    AsyncWriteStream(AsyncWriteStream&&)                 = delete;
    AsyncWriteStream& operator=(AsyncWriteStream&&)      = delete;

    // Opens a new call (closing any previous one) and resets the statistics.
    // The factory creates the call with the Async* stub method, e.g.
    //
    //   stream.open([&](grpc::ClientContext* ctx, grpc::CompletionQueue* cq, void* tag) {
    //       return stub->AsyncUpload(ctx, &summary, cq, tag);
    //   });
    //
    // StartCall sends the initial metadata through the stream's internal write
    // operation set, which is shared with Write() and WritesDone(). Until the
    // start-call tag completes, that set is in flight, and issuing a Write would
    // submit SEND_INITIAL_METADATA a second time, which gRPC rejects with
    // GRPC_CALL_ERROR_TOO_MANY_OPERATIONS. Requests sent meanwhile are queued.
    // NOTE: do not cork the initial metadata (set_initial_metadata_corked) on
    // the context, since then the start-call tag never completes.
    template<AsyncStreamFactoryFor<typename Base::AsyncStreamInterface> StreamFactory>
    bool open(StreamFactory&& stream_factory)
    {
        close();
        m_stats = {};
        return Base::open(std::forward<StreamFactory>(stream_factory));
    }

    // Cancels the call (if still active) and releases all resources. Requests
    // still waiting in the write queue are discarded (see WriteQueueStats).
    void close()
    {
        Base::close();
        discardPendingWrites();
        if (m_stats.write_in_flight)
        {
            // Cancelled before the write was confirmed
            ++m_stats.discarded_messages;
            m_stats.write_in_flight = false;
        }
        m_current_write = RequestProto{};
    }

    // True while new requests may be sent.
    [[nodiscard]] bool canWrite() const noexcept { return this->state() == State::Open; }

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
    [[nodiscard]] bool isWriteInProgress() const noexcept
    {
        return this->isStartPending() || this->isWriteOperationPending();
    }

    // Enqueues the given request to be written into the stream. Only one write
    // may be outstanding at a time, and none before the start-call has
    // completed; the rest are queued in a bounded queue (see WriteQueueLimits)
    // and sent as the completions arrive in poll(). Requests not fitting into
    // the queue are discarded according to the overflow policy, so memory use
    // stays bounded however fast requests are produced.
    SendResult sendRequest(RequestProto request)
    {
        if (this->state() != State::Open)
            return SendResult::NotWritable;

        ++m_stats.submitted_messages;
        if (!isWriteInProgress()) [[likely]]
        {
            startWrite(std::move(request));
            return SendResult::Written;
        }

        return enqueue(std::move(request));
    }

    // Half-closes the stream from the client side: already queued requests are
    // still sent, then WritesDone is issued. A write-only stream then requests
    // the final status (and the response); a stream with a read side keeps
    // delivering responses until the server ends the call.
    bool writesDone()
    {
        if (this->state() != State::Open)
            return false;

        this->setState(State::HalfClosing);
        if (!isWriteInProgress())
            startWritesDone();
        return true;
    }

    // Non-blocking processing of up to max_events currently available stream
    // events. Returns false once the call has ended (status() then holds the
    // reason), true otherwise.
    bool poll(std::size_t max_events = 64)
    {
        return this->processEvents(eventHandler(), max_events, gpr_inf_past(GPR_CLOCK_MONOTONIC));
    }

    // Like poll(), but waits up to `timeout` for the first event, which avoids
    // busy polling in a dedicated stream thread. Further already available
    // events are processed without waiting.
    template<typename Rep, typename Period>
    bool pollFor(std::chrono::duration<Rep, Period> timeout, std::size_t max_events = 64)
    {
        return this->processEvents(eventHandler(), max_events, Base::deadlineAfter(timeout));
    }

protected:
    using State = StreamState;
    using Tag   = detail::StreamTag;

    // Handles the completions of the start-call, writes and the half-close, and
    // passes the others down to the call core.
    template<typename Context>
    void handleEvent(Tag tag, bool ok, Context& context)
    {
        switch (tag)
        {
            using enum detail::StreamTag;
        case Start:
            // The initial metadata has been sent and the write operation set is
            // free again, so the requests queued since open() can be sent now.
            if (!this->completeStart(ok)) [[unlikely]]
            {
                // The call could not be started. An outstanding read fails as
                // well, after which Finish retrieves the actual error status.
                enterFinishing();
                break;
            }
            onWriteSlotFree();
            break;

        case Write:
            this->setWriteOperationPending(false);
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
            // (Clear() would keep the capacity of its string and repeated fields)
            m_current_write = RequestProto{};
            onWriteSlotFree();
            break;

        case WritesDone:
            this->setWriteOperationPending(false);
            if (!this->hasReadSide())
            {
                // Nothing is left to wait for but the final status
                enterFinishing();
                break;
            }
            if (this->state() == State::HalfClosing)
                this->setState(State::HalfClosed);
            this->tryFinish();
            break;

        default:
            Base::handleEvent(tag, ok, context);
            break;
        }
    }

    // Stops writing: queued requests can no longer be delivered.
    void enterFinishing()
    {
        discardPendingWrites();
        Base::enterFinishing();
    }

private:
    struct PendingWrite
    {
        RequestProto request;
        std::size_t  bytes = 0;
    };

    auto eventHandler()
    {
        return [this](Tag tag, bool ok) {
            detail::NoEventContext context;
            handleEvent(tag, ok, context);
        };
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

        m_pending_writes.pushBack({.request = std::move(request), .bytes = bytes});
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
        const PendingWrite dropped = m_pending_writes.popBack();
        dropRequest(dropped.bytes);
        --m_stats.queued_messages;
        m_stats.queued_bytes -= dropped.bytes;
        shrinkQueueIfEmpty();
    }

    RequestProto popPendingFront()
    {
        PendingWrite pending = m_pending_writes.popFront();
        --m_stats.queued_messages;
        m_stats.queued_bytes -= pending.bytes;
        shrinkQueueIfEmpty();
        return std::move(pending.request);
    }

    // The ring buffer keeps its capacity after being drained; release it once
    // a load peak is over so that the memory footprint returns to the baseline.
    // Small fluctuations of the queue are ignored to avoid reallocation churn.
    void shrinkQueueIfEmpty()
    {
        static constexpr std::size_t shrink_threshold = 256;
        if (m_pending_writes.empty() && m_queue_peak_since_shrink > shrink_threshold)
        {
            m_pending_writes.release();
            m_queue_peak_since_shrink = 0;
        }
    }

    void discardPendingWrites()
    {
        m_stats.discarded_messages += m_pending_writes.size();
        m_stats.queued_messages   = 0;
        m_stats.queued_bytes      = 0;
        m_queue_peak_since_shrink = 0;
        m_pending_writes.release();
    }

    void startWrite(RequestProto request)
    {
        // The message is kept alive in a member until the write completes
        m_current_write = std::move(request);
        this->stream()->Write(m_current_write, Base::toTag(Tag::Write));
        this->setWriteOperationPending(true);
        m_stats.write_in_flight = true;
    }

    void startWritesDone()
    {
        this->stream()->WritesDone(Base::toTag(Tag::WritesDone));
        this->setWriteOperationPending(true);
    }

    // Called when the write operation set becomes free (start-call or a write
    // completed successfully): sends the next queued request, the pending
    // half-close, or the Finish once the stream is ending.
    void onWriteSlotFree()
    {
        if (!m_pending_writes.empty())
            startWrite(popPendingFront());
        else if (this->state() == State::HalfClosing)
            startWritesDone();
        else
            this->tryFinish();
    }

    RequestProto             m_current_write;
    detail::RingQueue<PendingWrite> m_pending_writes;
    WriteQueueLimits         m_limits;
    WriteQueueStats          m_stats;
    std::size_t              m_queue_peak_since_shrink = 0;
};

} // namespace grpc_utils
