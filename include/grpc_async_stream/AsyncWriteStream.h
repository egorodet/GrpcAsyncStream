// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

#pragma once

// Write side of asynchronous client streams with a bounded write queue.
// Requires C++20; depends only on gRPC and the C++ standard library.

#include <grpc_async_stream/AsyncCallBase.h>
#include <grpc_async_stream/RingQueue.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>

#include <algorithm>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>

namespace grpc_async_stream
{

// What happens to a request that does not fit into the write queue
enum class WriteOverflowPolicy
{
    DropNewest, // discard the new request (SendResult::Dropped), keeping the oldest data
    DropOldest, // discard the oldest queued requests (SendResult::QueuedDroppedOldest), suits telemetry
};

// Limits of the queue buffering requests while a write is in flight (gRPC allows one at a time).
// Whichever limit is hit first triggers the overflow policy; the in-flight request is not counted.
//   WriteQueueLimits{.max_messages = 256, .overflow_policy = WriteOverflowPolicy::DropNewest}
struct WriteQueueLimits
{
    static constexpr std::size_t unlimited = std::numeric_limits<std::size_t>::max();

    std::size_t max_messages = 1024;              // 0 disables buffering (only immediate writes)
    std::size_t max_bytes    = 64U * 1024U * 1024U; // total ByteSizeLong() of buffered requests
    WriteOverflowPolicy overflow_policy = WriteOverflowPolicy::DropOldest;

    [[nodiscard]] static constexpr WriteQueueLimits unbounded() noexcept
    {
        return {.max_messages = unlimited, .max_bytes = unlimited, .overflow_policy = WriteOverflowPolicy::DropNewest};
    }

    friend constexpr bool operator==(const WriteQueueLimits&, const WriteQueueLimits&) = default;
};

// Write queue counters of the current (or last) call, reset by open()
struct WriteQueueStats
{
    std::size_t   queued_messages      = 0; // requests currently waiting in the queue
    std::size_t   queued_bytes         = 0; // their total serialized size
    std::size_t   peak_queued_messages = 0; // high-water mark of queued_messages
    std::size_t   peak_queued_bytes    = 0; // high-water mark of queued_bytes
    std::uint64_t submitted_messages   = 0; // requests passed to sendRequest while writable
    std::uint64_t written_messages     = 0; // writes completed successfully
    std::uint64_t dropped_messages     = 0; // requests discarded by the overflow policy
    std::uint64_t dropped_bytes        = 0; // their total serialized size
    std::uint64_t discarded_messages   = 0; // queued or in-flight requests lost when the call ended
    bool          write_in_flight      = false; // a request write is outstanding

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
    Written,             // write started immediately
    Queued,              // buffered, written later
    QueuedDroppedOldest, // buffered after discarding older requests (DropOldest)
    Dropped,             // discarded: queue full (DropNewest), too large or buffering disabled
    NotWritable,         // stream not open, half-closed or ending
};

// True when the request will be written (unless the call ends first)
[[nodiscard("check whether the request was accepted")]] constexpr bool isAccepted(SendResult result) noexcept
{
    using enum SendResult;
    return result == Written || result == Queued || result == QueuedDroppedOldest;
}

// Write side of a client stream: ClientAsyncWriterInterface or ClientAsyncReaderWriterInterface
template<typename StreamInterface, typename RequestProto>
concept AsyncWriterInterfaceFor =
    requires(StreamInterface& stream, const RequestProto& request, grpc::Status* status, void* tag) {
        stream.Write(request, tag);
        stream.WritesDone(tag);
        stream.Finish(status, tag);
    };

// Client-streaming call wrapper: serialized writes with a bounded queue, half-close and final status.
// The single response goes to the message passed to the stub in the factory; it must outlive the call.
// An idle stream notices the end of the call with its next sendRequest() or writesDone().
// Not thread-safe: call all members from one thread.
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

    // Opens a new call (closing the previous one) and resets the statistics, e.g.
    //   stream.open([&](auto* ctx, auto* cq, void* tag) { return stub->AsyncUpload(ctx, &summary, cq, tag); });
    // Requests are queued until the start-call completes (gRPC allows no Write before it).
    // Do not cork the initial metadata on the context: the start-call would never complete.
    template<AsyncStreamFactoryFor<typename Base::AsyncStreamInterface> StreamFactory>
    bool open(StreamFactory&& stream_factory)
    {
        close();
        m_stats = {};
        return Base::open(std::forward<StreamFactory>(stream_factory));
    }

    // Cancels the active call and releases all resources, discarding queued requests.
    void close()
    {
        Base::close();
        discardPendingWrites();
        if (m_stats.write_in_flight)
        {
            // Cancelled before the write completed
            ++m_stats.discarded_messages;
            m_stats.write_in_flight = false;
        }
    }

    // True while new requests may be sent.
    [[nodiscard]] bool canWrite() const noexcept { return this->state() == State::Open; }

    // Write queue limits; tightening them applies the overflow policy to queued requests at once.
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

    // True while a write, the start-call or the half-close is outstanding.
    [[nodiscard]] bool isWriteInProgress() const noexcept
    {
        return this->isStartPending() || this->isWriteOperationPending();
    }

    // Writes the request now if no write is outstanding, otherwise queues it (see WriteQueueLimits).
    // The request is copied or moved only when queued; dropped requests are never copied.
    SendResult sendRequest(const RequestProto& request) { return submit(request); }
    SendResult sendRequest(RequestProto&& request) { return submit(std::move(request)); }

    // Half-closes the stream after sending the queued requests.
    // A stream with a read side keeps delivering responses until the server ends the call.
    bool writesDone()
    {
        if (this->state() != State::Open)
            return false;

        this->setState(State::HalfClosing);
        if (!isWriteInProgress())
            startWritesDone();
        return true;
    }

    // Processes up to max_events available events without blocking.
    // Returns false once the call has ended (see status()).
    bool poll(std::size_t max_events = 64)
    {
        return this->processEvents(eventHandler(), max_events, gpr_inf_past(GPR_CLOCK_MONOTONIC));
    }

    // Waits up to `timeout` for the next event and processes it; returns like poll().
    // Keep max_events = 1 for low latency (see AsyncReadStream::pollFor()).
    template<typename Rep, typename Period>
    bool pollFor(std::chrono::duration<Rep, Period> timeout, std::size_t max_events = 1)
    {
        return this->processEvents(eventHandler(), max_events, Base::deadlineAfter(timeout));
    }

protected:
    using State = StreamState;
    using Tag   = detail::StreamTag;

    // Handles start-call, write and half-close completions; passes the rest to the core.
    template<typename Context>
    void handleEvent(Tag tag, bool ok, Context& context)
    {
        switch (tag)
        {
            using enum detail::StreamTag;
        case Start:
            // Initial metadata sent: requests queued since open() can be written now
            if (!this->completeStart(ok)) [[unlikely]]
            {
                // Start failed: the pending read fails too, then Finish gets the error status
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
                // The call is broken: further writes would fail too
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
                // Only the final status is left to wait for
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

    // Stops writing and discards the queued requests.
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

    // Copies or moves the request into its queue slot; a dropped request is not copied.
    template<typename Message>
    SendResult enqueue(Message&& request)
    {
        const std::size_t bytes = request.ByteSizeLong();

        // A request not fitting even into an empty queue is dropped
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
        // No additions that could overflow with "unlimited" limits
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

    // Destroys the oldest queued request in place
    void removePendingFront()
    {
        const std::size_t bytes = m_pending_writes.front().bytes;
        m_pending_writes.dropFront();
        --m_stats.queued_messages;
        m_stats.queued_bytes -= bytes;
        shrinkQueueIfEmpty();
    }

    // Releases the drained queue storage after a large load peak (small peaks keep it to avoid churn).
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

    // gRPC serializes the request within Write(): it need not outlive this call.
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

    // Write slot is free: sends the next queued request, the pending half-close or Finish.
    void onWriteSlotFree()
    {
        if (!m_pending_writes.empty())
        {
            // Written straight from its slot, destroyed right after
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

} // namespace grpc_async_stream
