// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

// Tests of the bounded write queue: message / byte limits, overflow policies,
// statistics and memory bounds under a flood of requests.

#include "support/TestServer.h"

#include <grpc_async_stream_test/Catch2StringMakers.h>
#include <grpc_async_stream_test/EnumReflection.h>
#include <grpc_async_stream_test/FakeReaderWriter.h>
#include <grpc_async_stream/AsyncReadWriteStream.h>
#include <grpc_async_stream/StreamUtils.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/generators/catch_generators_range.hpp>

#include <array>
#include <functional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <vector>

namespace
{

using namespace TestSupport;
using namespace grpc_async_stream_test;
using grpc_async_stream::idsOf;
using grpc_async_stream::pollUntil;
using namespace Catch::Generators;
using grpc_async_stream::SendResult;
using grpc_async_stream::WriteOverflowPolicy;
using grpc_async_stream::WriteQueueLimits;
using grpc_async_stream::WriteQueueStats;

using Stream = grpc_async_stream::AsyncReadWriteStream<Request, Response>;
using Call   = FakeCall<Request, Response>;

constexpr std::size_t unlimited = WriteQueueLimits::unlimited;

constexpr WriteQueueLimits limits(std::size_t max_messages, std::size_t max_bytes, WriteOverflowPolicy policy)
{
    return {.max_messages = max_messages, .max_bytes = max_bytes, .overflow_policy = policy};
}

std::size_t sizeOf(const Request& request) { return request.ByteSizeLong(); }

void checkInvariant(const WriteQueueStats& stats)
{
    INFO("submitted=" << stats.submitted_messages << " written=" << stats.written_messages
                      << " dropped=" << stats.dropped_messages << " discarded=" << stats.discarded_messages
                      << " queued=" << stats.queued_messages << " in_flight=" << stats.write_in_flight);
    CHECK(stats.isConsistent());
}

// Stream opened over a fake call whose start-call is NOT completed, so every
// request goes through the write queue deterministically.
struct QueueFixture
{
    std::shared_ptr<Call>           call = std::make_shared<Call>();
    Stream                          stream;
    std::function<void(Response&&)> handler = [](Response&&) {};

    explicit QueueFixture(const WriteQueueLimits& queue_limits)
        : stream(queue_limits)
    {
        REQUIRE(stream.open(makeFakeStreamFactory(call)));
    }

    // Completes the start-call and every subsequent write; returns the written ids
    std::vector<std::uint64_t> drain()
    {
        if (call->startPending())
            call->completeStart();
        for (;;)
        {
            const bool more = pollUntil(stream, handler, [&] {
                return call->writePending() || (!stream.isWriteInProgress() && stream.writeQueueStats().queued_messages == 0);
            });
            REQUIRE(more);
            if (!call->writePending())
                break;
            const std::uint64_t written_before = stream.writeQueueStats().written_messages;
            call->completeWrite();
            REQUIRE(pollUntil(stream, handler, [&] { return stream.writeQueueStats().written_messages > written_before; }));
        }
        return idsOf(call->writes());
    }
};

// Element of the ring buffer tests counting its live instances
struct CountedElement
{
    static inline int live = 0;

    explicit CountedElement(int element_value)
        : value(element_value)
    {
        ++live;
    }
    CountedElement(CountedElement&& other) noexcept
        : value(other.value)
    {
        ++live;
    }
    CountedElement(const CountedElement&)            = delete;
    CountedElement& operator=(const CountedElement&) = delete;
    CountedElement& operator=(CountedElement&&)      = delete;
    ~CountedElement() { --live; }

    int value;
};

// Element whose move may throw, so the ring buffer copies it when growing;
// the copy throws after the configured number of successful copies
struct ThrowingCopyElement
{
    static inline int live                  = 0;
    static inline int copies_before_failure = -1; // negative: never fail

    explicit ThrowingCopyElement(int element_value)
        : value(element_value)
    {
        ++live;
    }
    ThrowingCopyElement(const ThrowingCopyElement& other)
        : value(other.value)
    {
        if (copies_before_failure == 0)
            throw std::runtime_error("copy failed");
        if (copies_before_failure > 0)
            --copies_before_failure;
        ++live;
    }
    ThrowingCopyElement(ThrowingCopyElement&& other) // deliberately not noexcept
        : value(other.value)
    {
        ++live;
    }
    ThrowingCopyElement& operator=(const ThrowingCopyElement&) = delete;
    ThrowingCopyElement& operator=(ThrowingCopyElement&&)      = delete;
    ~ThrowingCopyElement() { --live; }

    int value;
};

} // namespace

TEST_CASE("Default write queue limits bound both message count and bytes", "[write-queue]")
{
    constexpr WriteQueueLimits defaults;
    static_assert(WriteQueueLimits::unbounded() != defaults);
    static_assert(limits(1, 2, WriteOverflowPolicy::DropNewest) ==
                  WriteQueueLimits{.max_messages = 1, .max_bytes = 2, .overflow_policy = WriteOverflowPolicy::DropNewest});
    CHECK(defaults.max_messages == 1024);
    CHECK(defaults.max_bytes == 64U * 1024U * 1024U);
    CHECK(defaults.overflow_policy == WriteOverflowPolicy::DropOldest);

    Stream stream;
    CHECK(stream.writeQueueLimits() == defaults);

    const WriteQueueLimits unbounded = WriteQueueLimits::unbounded();
    CHECK(unbounded.max_messages == unlimited);
    CHECK(unbounded.max_bytes == unlimited);
}

TEST_CASE("isAccepted() classifies every send result", "[write-queue][enums]")
{
    static_assert(grpc_async_stream::isAccepted(SendResult::Written));

    const SendResult result = GENERATE(from_range(magic_enum::enum_values<SendResult>()));
    const bool accepted     = result == SendResult::Written || result == SendResult::Queued ||
                          result == SendResult::QueuedDroppedOldest;
    CHECK(grpc_async_stream::isAccepted(result) == accepted);
}

TEST_CASE("Library enums reflect with magic_enum: names, parsing, counts and indexes", "[write-queue][enums]")
{
    static_assert(magic_enum::enum_count<SendResult>() == 5);
    static_assert(magic_enum::enum_count<WriteOverflowPolicy>() == 2);
    static_assert(magic_enum::enum_values<WriteOverflowPolicy>()[0] == WriteOverflowPolicy::DropNewest);
    static_assert(magic_enum::enum_name(SendResult::QueuedDroppedOldest) == "QueuedDroppedOldest");
    static_assert(magic_enum::enum_name(WriteOverflowPolicy::DropNewest) == "DropNewest");
    static_assert(magic_enum::enum_cast<WriteOverflowPolicy>("DropOldest") == WriteOverflowPolicy::DropOldest);
    static_assert(enumIndex(SendResult::NotWritable) == magic_enum::enum_count<SendResult>() - 1);
    static_assert(magic_enum::enum_count<grpc_async_stream::StreamState>() == 5);
    static_assert(magic_enum::enum_name(grpc_async_stream::StreamState::HalfClosing) == "HalfClosing");

    SECTION("Every enumerator round-trips through its name and index")
    {
        const SendResult result = GENERATE(from_range(magic_enum::enum_values<SendResult>()));
        CHECK(magic_enum::enum_cast<SendResult>(magic_enum::enum_name(result)) == result);
        CHECK(magic_enum::enum_values<SendResult>()[enumIndex(result)] == result);
        CHECK(Catch::Detail::stringify(result) == std::string(magic_enum::enum_name(result)));
    }

    SECTION("Unknown names and values are rejected")
    {
        CHECK_FALSE(magic_enum::enum_cast<SendResult>("dropped").has_value()); // case-sensitive
        CHECK_FALSE(magic_enum::enum_cast<SendResult>("").has_value());
        CHECK_FALSE(magic_enum::enum_cast<WriteOverflowPolicy>("DropAll").has_value());
        CHECK(magic_enum::enum_name(static_cast<SendResult>(42)).empty());
        CHECK(Catch::Detail::stringify(static_cast<SendResult>(42)) == "<invalid enumerator>");
    }

    SECTION("A histogram can be indexed by enumerator")
    {
        std::array<std::uint64_t, magic_enum::enum_count<SendResult>()> histogram{};
        QueueFixture f(limits(2, unlimited, WriteOverflowPolicy::DropNewest));
        for (const std::uint64_t id : std::views::iota(std::uint64_t{1}, std::uint64_t{6}))
            ++histogram[enumIndex(f.stream.sendRequest(makeEcho(id)))];
        f.stream.close();
        ++histogram[enumIndex(f.stream.sendRequest(makeEcho(6)))];

        CHECK(histogram[enumIndex(SendResult::Queued)] == 2);
        CHECK(histogram[enumIndex(SendResult::Dropped)] == 3);
        CHECK(histogram[enumIndex(SendResult::NotWritable)] == 1);
        CHECK(histogram[enumIndex(SendResult::Written)] == 0);
    }
}

TEST_CASE("DropNewest rejects requests once the message limit is reached", "[write-queue]")
{
    QueueFixture f(limits(3, unlimited, WriteOverflowPolicy::DropNewest));

    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(3)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(4)) == SendResult::Dropped);
    CHECK(f.stream.sendRequest(makeEcho(5)) == SendResult::Dropped);

    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.queued_messages == 3);
    CHECK(stats.dropped_messages == 2);
    CHECK(stats.dropped_bytes == sizeOf(makeEcho(4)) + sizeOf(makeEcho(5)));
    checkInvariant(stats);

    CHECK(f.drain() == std::vector<std::uint64_t>{1, 2, 3});
    checkInvariant(f.stream.writeQueueStats());
    CHECK(f.call->violations().empty());
}

TEST_CASE("DropOldest evicts the oldest requests to keep the most recent ones", "[write-queue]")
{
    QueueFixture f(limits(3, unlimited, WriteOverflowPolicy::DropOldest));

    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(3)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(4)) == SendResult::QueuedDroppedOldest);
    CHECK(f.stream.sendRequest(makeEcho(5)) == SendResult::QueuedDroppedOldest);

    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.queued_messages == 3);
    CHECK(stats.peak_queued_messages == 3);
    CHECK(stats.dropped_messages == 2);
    checkInvariant(stats);

    CHECK(f.drain() == std::vector<std::uint64_t>{3, 4, 5});
    CHECK(f.call->violations().empty());
}

TEST_CASE("The byte limit bounds the total serialized size of queued requests", "[write-queue]")
{
    const std::string payload(100, 'p');
    const std::size_t message_size = sizeOf(makeEcho(1, payload));
    REQUIRE(sizeOf(makeEcho(9, payload)) == message_size);

    SECTION("DropNewest")
    {
        QueueFixture f(limits(unlimited, 3 * message_size, WriteOverflowPolicy::DropNewest));
        for (std::uint64_t id = 1; id <= 3; ++id)
            CHECK(f.stream.sendRequest(makeEcho(id, payload)) == SendResult::Queued);
        CHECK(f.stream.sendRequest(makeEcho(4, payload)) == SendResult::Dropped);
        CHECK(f.stream.writeQueueStats().queued_bytes == 3 * message_size);
        CHECK(f.stream.writeQueueStats().peak_queued_bytes == 3 * message_size);
        CHECK(f.drain() == std::vector<std::uint64_t>{1, 2, 3});
    }

    SECTION("DropOldest evicts as many small requests as needed for a big one")
    {
        QueueFixture f(limits(unlimited, 3 * message_size, WriteOverflowPolicy::DropOldest));
        for (std::uint64_t id = 1; id <= 3; ++id)
            CHECK(f.stream.sendRequest(makeEcho(id, payload)) == SendResult::Queued);

        // Needs roughly the space of two queued messages
        const Request big = makeEcho(10, std::string(2 * message_size - 20, 'b'));
        REQUIRE(sizeOf(big) > message_size);
        REQUIRE(sizeOf(big) <= 2 * message_size);
        CHECK(f.stream.sendRequest(big) == SendResult::QueuedDroppedOldest);

        const auto& stats = f.stream.writeQueueStats();
        CHECK(stats.dropped_messages == 2);
        CHECK(stats.queued_messages == 2);
        CHECK(stats.queued_bytes == message_size + sizeOf(big));
        CHECK(stats.queued_bytes <= 3 * message_size);
        CHECK(f.drain() == std::vector<std::uint64_t>{3, 10});
    }
}

TEST_CASE("A request larger than the byte limit is always dropped", "[write-queue]")
{
    const auto policy = GENERATE(from_range(magic_enum::enum_values<WriteOverflowPolicy>()));
    QueueFixture f(limits(10, 500, policy));

    CHECK(f.stream.sendRequest(makeEcho(1, "small")) == SendResult::Queued);
    const Request huge = makeEcho(2, std::string(1000, 'h'));
    CHECK(f.stream.sendRequest(huge) == SendResult::Dropped);

    // The already queued request is kept even with DropOldest
    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.queued_messages == 1);
    CHECK(stats.dropped_messages == 1);
    CHECK(stats.dropped_bytes == sizeOf(huge));
    CHECK(f.drain() == std::vector<std::uint64_t>{1});
}

TEST_CASE("A zero message limit disables buffering", "[write-queue]")
{
    const auto policy = GENERATE(from_range(magic_enum::enum_values<WriteOverflowPolicy>()));
    QueueFixture f(limits(0, unlimited, policy));

    // Before the start-call completes nothing can be written
    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Dropped);
    f.drain();

    // Accepted only while the write slot is free
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Written);
    CHECK(f.stream.sendRequest(makeEcho(3)) == SendResult::Dropped);
    CHECK(f.stream.writeQueueStats().peak_queued_messages == 0);
    checkInvariant(f.stream.writeQueueStats());
}

TEST_CASE("Tightening the limits applies the overflow policy to the queued requests", "[write-queue]")
{
    SECTION("DropOldest keeps the newest requests")
    {
        QueueFixture f(WriteQueueLimits::unbounded());
        for (std::uint64_t id = 1; id <= 10; ++id)
            CHECK(f.stream.sendRequest(makeEcho(id)) == SendResult::Queued);

        f.stream.setWriteQueueLimits(limits(4, unlimited, WriteOverflowPolicy::DropOldest));
        CHECK(f.stream.writeQueueStats().queued_messages == 4);
        CHECK(f.stream.writeQueueStats().dropped_messages == 6);
        checkInvariant(f.stream.writeQueueStats());
        CHECK(f.drain() == std::vector<std::uint64_t>{7, 8, 9, 10});
    }

    SECTION("DropNewest keeps the oldest requests")
    {
        QueueFixture f(WriteQueueLimits::unbounded());
        for (std::uint64_t id = 1; id <= 10; ++id)
            CHECK(f.stream.sendRequest(makeEcho(id)) == SendResult::Queued);

        f.stream.setWriteQueueLimits(limits(4, unlimited, WriteOverflowPolicy::DropNewest));
        CHECK(f.stream.writeQueueStats().queued_messages == 4);
        CHECK(f.drain() == std::vector<std::uint64_t>{1, 2, 3, 4});
    }

    SECTION("Byte limit")
    {
        QueueFixture f(WriteQueueLimits::unbounded());
        const std::string payload(50, 'z');
        for (std::uint64_t id = 1; id <= 10; ++id)
            f.stream.sendRequest(makeEcho(id, payload));

        const std::size_t message_size = sizeOf(makeEcho(1, payload));
        f.stream.setWriteQueueLimits(limits(unlimited, 2 * message_size + 1, WriteOverflowPolicy::DropOldest));
        CHECK(f.stream.writeQueueStats().queued_messages == 2);
        CHECK(f.stream.writeQueueStats().queued_bytes == 2 * message_size);
    }

    SECTION("Loosening the limits keeps everything")
    {
        QueueFixture f(limits(5, unlimited, WriteOverflowPolicy::DropNewest));
        for (std::uint64_t id = 1; id <= 5; ++id)
            f.stream.sendRequest(makeEcho(id));
        f.stream.setWriteQueueLimits(limits(50, unlimited, WriteOverflowPolicy::DropNewest));
        CHECK(f.stream.writeQueueStats().queued_messages == 5);
        CHECK(f.stream.sendRequest(makeEcho(6)) == SendResult::Queued);
    }
}

TEST_CASE("Unbounded limits accept every request", "[write-queue]")
{
    QueueFixture f(WriteQueueLimits::unbounded());
    for (const std::uint64_t id : std::views::iota(std::uint64_t{1}, std::uint64_t{10001}))
        REQUIRE(f.stream.sendRequest(makeEcho(id)) == SendResult::Queued);
    CHECK(f.stream.writeQueueStats().queued_messages == 10000);
    CHECK(f.stream.writeQueueStats().dropped_messages == 0);
}

TEST_CASE("A flood of requests never grows the queue beyond its limits", "[write-queue][load]")
{
    const auto policy = GENERATE(from_range(magic_enum::enum_values<WriteOverflowPolicy>()));
    const std::string payload(256, 'f');
    const std::size_t message_size = sizeOf(makeEcho(1, payload));

    // The byte limit is hit before the message limit (50 messages fit)
    const std::size_t max_bytes = 50 * message_size;
    QueueFixture      f(limits(100, max_bytes, policy));

    constexpr std::uint64_t total = 200000;
    for (std::uint64_t id = 1; id <= total; ++id)
    {
        const SendResult result = f.stream.sendRequest(makeEcho(id, payload));
        if (policy == WriteOverflowPolicy::DropNewest)
            REQUIRE((id <= 50 ? result == SendResult::Queued : result == SendResult::Dropped));
        else
            REQUIRE((id <= 50 ? result == SendResult::Queued : result == SendResult::QueuedDroppedOldest));
    }

    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.queued_messages == 50);
    CHECK(stats.peak_queued_messages == 50);
    CHECK(stats.peak_queued_bytes <= max_bytes);
    CHECK(stats.dropped_messages == total - 50);
    CHECK(stats.dropped_bytes == (total - 50) * message_size);
    checkInvariant(stats);

    const auto ids = f.drain();
    REQUIRE(ids.size() == 50);
    CHECK(ids.front() == (policy == WriteOverflowPolicy::DropNewest ? 1 : total - 49));
    CHECK(ids.back() == (policy == WriteOverflowPolicy::DropNewest ? 50 : total));
    checkInvariant(f.stream.writeQueueStats());
}

TEST_CASE("Requests sent while the queue drains are accepted again", "[write-queue]")
{
    QueueFixture f(limits(2, unlimited, WriteOverflowPolicy::DropNewest));
    f.drain();

    CHECK(f.stream.sendRequest(makeEcho(1)) == SendResult::Written);
    CHECK(f.stream.sendRequest(makeEcho(2)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(3)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(4)) == SendResult::Dropped);

    // Completing one write moves request 2 into flight and frees a queue slot
    f.call->completeWrite();
    REQUIRE(pollUntil(f.stream, f.handler, [&] { return f.call->writes().size() == 2; }));
    CHECK(f.stream.sendRequest(makeEcho(5)) == SendResult::Queued);
    CHECK(f.stream.sendRequest(makeEcho(6)) == SendResult::Dropped);

    f.call->completeWrite();
    CHECK(f.drain() == std::vector<std::uint64_t>{1, 2, 3, 5});
    checkInvariant(f.stream.writeQueueStats());
    CHECK(f.call->violations().empty());
}

TEST_CASE("The ring buffer of the write queue keeps FIFO order across wrap-around and growth", "[write-queue]")
{
    grpc_async_stream::detail::RingQueue<std::string> queue;
    std::uint64_t                              next_in  = 0;
    std::uint64_t                              next_out = 0;

    // Move the head forward so that the contents wrap around the end of the
    // storage, then grow while wrapped (16 -> 32 -> 64 slots)
    for (int i = 0; i < 10; ++i)
        queue.pushBack(std::to_string(next_in++));
    for (int i = 0; i < 7; ++i)
        CHECK(queue.popFront() == std::to_string(next_out++));
    for (int i = 0; i < 50; ++i)
        queue.pushBack(std::to_string(next_in++));
    CHECK(queue.size() == next_in - next_out);

    // popBack takes the newest element, front() is the oldest one
    CHECK(queue.popBack() == std::to_string(--next_in));
    CHECK(queue.front() == std::to_string(next_out));
    while (!queue.empty())
        CHECK(queue.popFront() == std::to_string(next_out++));
    CHECK(next_out == next_in);

    // Usable again after releasing the storage
    queue.pushBack("a");
    queue.release();
    CHECK(queue.empty());
    queue.pushBack("b");
    CHECK(queue.popFront() == "b");
}

TEST_CASE("Queued requests are written in order when the queue wraps around and grows", "[write-queue]")
{
    QueueFixture f(WriteQueueLimits::unbounded());
    std::uint64_t id = 0;
    for (; id < 10; ++id)
        REQUIRE(isAccepted(f.stream.sendRequest(makeEcho(id))));

    // Write a few, so that the queue's head moves forward
    f.call->completeStart();
    for (std::size_t written = 1; written <= 6; ++written)
    {
        REQUIRE(pollUntil(f.stream, f.handler, [&] { return f.call->writePending(); }));
        f.call->completeWrite();
        REQUIRE(pollUntil(f.stream, f.handler, [&] { return f.stream.writeQueueStats().written_messages == written; }));
    }

    // 3 requests are left in the queue: the next ones wrap around and grow it while wrapped
    for (; id < 40; ++id)
        REQUIRE(isAccepted(f.stream.sendRequest(makeEcho(id))));

    const auto ids = f.drain();
    REQUIRE(ids.size() == 40);
    for (std::uint64_t i = 0; i < ids.size(); ++i)
        CHECK(ids[i] == i);
    checkInvariant(f.stream.writeQueueStats());
    CHECK(f.call->violations().empty());
}

TEST_CASE("The ring buffer destroys elements as soon as they leave the queue", "[write-queue]")
{
    CountedElement::live = 0;
    {
        grpc_async_stream::detail::RingQueue<CountedElement> queue;
        for (int i = 0; i < 12; ++i)
            queue.emplaceBack(i);
        for (int i = 0; i < 9; ++i)
            queue.dropFront();
        CHECK(CountedElement::live == 3);

        // Wrap around the end of the storage and grow twice while wrapped:
        // growing leaves no moved-from elements behind
        for (int i = 12; i < 60; ++i)
            queue.emplaceBack(i);
        CHECK(queue.size() == 51);
        CHECK(queue.capacity() == 64);
        CHECK(CountedElement::live == 51);
        CHECK(queue.front().value == 9);
        CHECK(queue.back().value == 59);

        queue.dropBack();
        CHECK(queue.back().value == 58);
        CHECK(CountedElement::live == 50);

        const CountedElement oldest = queue.popFront();
        CHECK(oldest.value == 9);
        CHECK(CountedElement::live == 50);

        queue.release();
        CHECK(queue.empty());
        CHECK(queue.capacity() == 0);
        CHECK(CountedElement::live == 1);
    }
    CHECK(CountedElement::live == 0);
}

TEST_CASE("The ring buffer keeps its elements when copying them while growing throws", "[write-queue]")
{
    ThrowingCopyElement::live = 0;
    {
        grpc_async_stream::detail::RingQueue<ThrowingCopyElement> queue;
        for (int i = 0; i < 16; ++i)
            queue.emplaceBack(i);

        ThrowingCopyElement::copies_before_failure = 5;
        CHECK_THROWS_AS(queue.emplaceBack(16), std::runtime_error);
        ThrowingCopyElement::copies_before_failure = -1;
        CHECK(queue.size() == 16);
        CHECK(queue.capacity() == 16);
        CHECK(ThrowingCopyElement::live == 16);

        queue.emplaceBack(16);
        for (int i = 0; i <= 16; ++i)
            CHECK(queue.popFront().value == i);
    }
    CHECK(ThrowingCopyElement::live == 0);
}

TEST_CASE("sendRequest() copies an lvalue request only when it has to be queued", "[write-queue]")
{
    QueueFixture f(limits(1, unlimited, WriteOverflowPolicy::DropNewest));
    Request      request = makeEcho(1, "first");

    // Queued while the start-call is pending: the queue holds its own copy
    CHECK(f.stream.sendRequest(request) == SendResult::Queued);
    request.set_payload("changed after queueing");

    // Dropped, the queue being full: the caller's request is left as it is
    CHECK(f.stream.sendRequest(request) == SendResult::Dropped);
    CHECK(request.payload() == "changed after queueing");

    f.call->completeStart();
    REQUIRE(pollUntil(f.stream, f.handler, [&] { return f.call->writePending(); }));
    f.call->completeWrite();
    REQUIRE(pollUntil(f.stream, f.handler, [&] { return f.stream.writeQueueStats().written_messages == 1; }));

    // Written at once, straight from the caller's request, which is left as it is
    request.set_id(2);
    CHECK(f.stream.sendRequest(request) == SendResult::Written);
    CHECK(request.id() == 2);
    CHECK(request.payload() == "changed after queueing");

    REQUIRE(f.call->writes().size() == 2);
    CHECK(f.call->writes()[0].id() == 1);
    CHECK(f.call->writes()[0].payload() == "first");
    CHECK(f.call->writes()[1].id() == 2);
    CHECK(f.call->writes()[1].payload() == "changed after queueing");
    checkInvariant(f.stream.writeQueueStats());
    CHECK(f.call->violations().empty());
}

TEST_CASE("Closing the stream discards and accounts the queued requests", "[write-queue]")
{
    QueueFixture f(limits(10, unlimited, WriteOverflowPolicy::DropOldest));
    for (std::uint64_t id = 1; id <= 15; ++id)
        f.stream.sendRequest(makeEcho(id));

    f.stream.close();
    const auto& stats = f.stream.writeQueueStats();
    CHECK(stats.queued_messages == 0);
    CHECK(stats.queued_bytes == 0);
    CHECK(stats.discarded_messages == 10);
    CHECK(stats.dropped_messages == 5);
    CHECK(stats.peak_queued_messages == 10);
    checkInvariant(stats);
}
