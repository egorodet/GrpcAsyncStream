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
// triggers the overflow policy. The request currently being written is not
// part of the queue: gRPC serializes it within Write(), and the stream keeps
// no copy of it, so the stream holds at most `max_messages` requests.
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
// in steady state allocate nothing.
//
// The storage is raw memory: an element is constructed in its slot when pushed
// and destroyed when popped or dropped. Free slots therefore hold no objects
// (and no memory of the messages popped from them), growing moves only the
// live elements, and no empty elements are constructed. release() destroys the
// elements and frees the storage.
//
// The arguments of emplaceBack() / pushBack() must not refer to an element of
// the queue itself, since growing moves the elements.
template<typename T>
    requires std::move_constructible<T> && std::is_nothrow_destructible_v<T>
class RingQueue
{
public:
    RingQueue() = default;
    ~RingQueue() { release(); }

    RingQueue(const RingQueue&)            = delete;
    RingQueue& operator=(const RingQueue&) = delete;
    RingQueue(RingQueue&&)                 = delete;
    RingQueue& operator=(RingQueue&&)      = delete;

    [[nodiscard]] bool        empty() const noexcept { return m_size == 0; }
    [[nodiscard]] std::size_t size() const noexcept { return m_size; }
    [[nodiscard]] std::size_t capacity() const noexcept { return m_capacity; }

    // The oldest and the newest element; the queue must not be empty
    [[nodiscard]] T& front() noexcept { return m_slots[m_head]; }
    [[nodiscard]] T& back() noexcept { return m_slots[slot(m_size - 1)]; }

    // Constructs a new newest element in place. If the construction throws,
    // the queue keeps its elements (its capacity may have grown).
    template<typename... Args>
        requires std::constructible_from<T, Args...>
    T& emplaceBack(Args&&... args)
    {
        if (m_size == m_capacity)
            grow();
        T& element = *std::construct_at(m_slots + slot(m_size), std::forward<Args>(args)...);
        ++m_size;
        return element;
    }

    void pushBack(T&& value) { emplaceBack(std::move(value)); }

    // Remove the oldest / the newest element and return it
    T popFront()
    {
        T value = std::move(front());
        dropFront();
        return value;
    }

    T popBack()
    {
        T value = std::move(back());
        dropBack();
        return value;
    }

    // Destroy the oldest / the newest element in place, without moving it out
    void dropFront() noexcept
    {
        std::destroy_at(m_slots + m_head);
        m_head = slot(1);
        --m_size;
    }

    void dropBack() noexcept
    {
        std::destroy_at(m_slots + slot(m_size - 1));
        --m_size;
    }

    // Destroys all elements and frees the storage.
    void release() noexcept
    {
        destroyAll();
        if (m_slots)
            std::allocator<T>().deallocate(m_slots, m_capacity);
        m_slots    = nullptr;
        m_capacity = 0;
        m_head     = 0;
    }

private:
    static constexpr std::size_t initial_capacity = 16;

    [[nodiscard]] std::size_t slot(std::size_t offset) const noexcept { return (m_head + offset) & (m_capacity - 1); }

    void destroyAll() noexcept
    {
        while (m_size > 0)
            dropFront();
    }

    void grow()
    {
        // Frees the new storage if moving (or copying) an element throws, in
        // which case the queue keeps its elements in the old storage
        struct NewStorage
        {
            T*          slots       = nullptr;
            std::size_t capacity    = 0;
            std::size_t constructed = 0;

            ~NewStorage()
            {
                if (!slots)
                    return;
                std::destroy_n(slots, constructed);
                std::allocator<T>().deallocate(slots, capacity);
            }
        };

        const std::size_t new_capacity = m_capacity == 0 ? initial_capacity : m_capacity * 2;
        NewStorage        storage{.slots = std::allocator<T>().allocate(new_capacity), .capacity = new_capacity};
        for (; storage.constructed < m_size; ++storage.constructed)
            std::construct_at(storage.slots + storage.constructed,
                              std::move_if_noexcept(m_slots[slot(storage.constructed)]));
        T* const new_slots = std::exchange(storage.slots, nullptr);

        // The old slots hold moved-from elements, destroyed before freeing them
        const std::size_t count = m_size;
        release();
        m_slots    = new_slots;
        m_capacity = new_capacity;
        m_size     = count;
    }

    T*          m_slots    = nullptr;
    std::size_t m_capacity = 0;
    std::size_t m_head     = 0;
    std::size_t m_size     = 0;
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
    //
    // gRPC serializes a request within Write() and keeps no reference to it, so
    // a request written immediately is neither copied nor moved: the stream
    // only takes a request (copying an lvalue, moving an rvalue) when it has to
    // be queued. A request dropped by the overflow policy is never copied.
    SendResult sendRequest(const RequestProto& request) { return submit(request); }
    SendResult sendRequest(RequestProto&& request) { return submit(std::move(request)); }

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
    // reason), true otherwise. Returns at once while no operation is
    // outstanding (an idle stream).
    bool poll(std::size_t max_events = 64)
    {
        return this->processEvents(eventHandler(), max_events, gpr_inf_past(GPR_CLOCK_MONOTONIC));
    }

    // Waits up to `timeout` for the next event and processes it, which avoids
    // busy polling in a dedicated stream thread; returns like poll(). With
    // max_events > 1, further events already available are processed in the
    // same call, but finding out that none is left can delay the return (see
    // AsyncReadStream::pollFor()), so by default the loop calling pollFor()
    // takes them: the next call returns at once with an available event.
    template<typename Rep, typename Period>
    bool pollFor(std::chrono::duration<Rep, Period> timeout, std::size_t max_events = 1)
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
        template<typename Message>
        PendingWrite(Message&& message, std::size_t size)
            : request(std::forward<Message>(message))
            , bytes(size)
        {
        }

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

    template<typename Message>
    SendResult submit(Message&& request)
    {
        if (this->state() != State::Open)
            return SendResult::NotWritable;

        ++m_stats.submitted_messages;
        if (!isWriteInProgress()) [[likely]]
        {
            startWrite(request);
            return SendResult::Written;
        }

        return enqueue(std::forward<Message>(request));
    }

    // Queues a copy of an lvalue request, or moves an rvalue one, directly into
    // its slot of the queue; a dropped request is not copied.
    template<typename Message>
    SendResult enqueue(Message&& request)
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

        m_pending_writes.emplaceBack(std::forward<Message>(request), bytes);
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
        removePendingFront();
    }

    void dropPendingBack()
    {
        const std::size_t bytes = m_pending_writes.back().bytes;
        dropRequest(bytes);
        m_pending_writes.dropBack();
        --m_stats.queued_messages;
        m_stats.queued_bytes -= bytes;
        shrinkQueueIfEmpty();
    }

    // Destroys the oldest queued request in place, which frees its memory
    void removePendingFront()
    {
        const std::size_t bytes = m_pending_writes.front().bytes;
        m_pending_writes.dropFront();
        --m_stats.queued_messages;
        m_stats.queued_bytes -= bytes;
        shrinkQueueIfEmpty();
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

    // gRPC serializes the request within Write() and keeps no reference to it
    // ("safe to deallocate once Write returns", see AsyncWriterInterface), so
    // the request need not outlive this call.
    void startWrite(const RequestProto& request)
    {
        this->stream()->Write(request, Base::toTag(Tag::Write));
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
        {
            // Written straight from its slot, which is destroyed right after
            startWrite(m_pending_writes.front().request);
            removePendingFront();
        }
        else if (this->state() == State::HalfClosing)
            startWritesDone();
        else
            this->tryFinish();
    }

    detail::RingQueue<PendingWrite> m_pending_writes;
    WriteQueueLimits                m_limits;
    WriteQueueStats                 m_stats;
    std::size_t                     m_queue_peak_since_shrink = 0;
};

} // namespace grpc_utils
