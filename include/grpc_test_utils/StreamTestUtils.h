#pragma once

// Helpers for testing code built on grpc_utils::AsyncReadWriteStream:
// polling a stream until a condition holds or the call ends, and inspecting
// sequences of messages. Test-framework independent; requires only gRPC.

#include <grpc_utils/AsyncReadWriteStream.h>

#include <algorithm>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <iterator>
#include <ranges>
#include <vector>

namespace grpc_test_utils
{

using namespace std::chrono_literals;

// Anything that can be polled like grpc_utils::AsyncReadWriteStream
template<typename Stream>
concept PollableStream = requires(Stream& stream) {
    { stream.isOpen() } -> std::convertible_to<bool>;
};

// Polls the stream until the predicate becomes true, the call ends, or the
// timeout expires. Returns the final value of the predicate.
template<PollableStream Stream, typename Handler, std::predicate Predicate>
bool pollUntil(Stream& stream, Handler&& handler, Predicate&& predicate,
               std::chrono::milliseconds timeout = std::chrono::seconds(10))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return predicate();
        if (!stream.pollFor(handler, 1ms))
            return predicate();
    }
    return true;
}

// Polls the stream until the call ends or the timeout expires. Returns true if the call ended.
template<PollableStream Stream, typename Handler>
bool pollUntilClosed(Stream& stream, Handler&& handler, std::chrono::milliseconds timeout = std::chrono::seconds(10))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (stream.isOpen())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        stream.pollFor(handler, 1ms);
    }
    return true;
}

// Messages with an integral id() accessor, as used by idsOf() / idsStrictlyIncrease()
template<typename Message>
concept MessageWithId = requires(const Message& message) {
    { message.id() } -> std::convertible_to<std::uint64_t>;
};

// The ids of a range of messages, in order
template<std::ranges::input_range Messages>
    requires MessageWithId<std::ranges::range_value_t<Messages>>
std::vector<std::uint64_t> idsOf(const Messages& messages)
{
    std::vector<std::uint64_t> ids;
    if constexpr (std::ranges::sized_range<Messages>)
        ids.reserve(std::ranges::size(messages));
    std::ranges::transform(messages, std::back_inserter(ids),
                           [](const auto& message) { return static_cast<std::uint64_t>(message.id()); });
    return ids;
}

// True when the ids of the messages strictly increase
template<std::ranges::forward_range Messages>
    requires MessageWithId<std::ranges::range_value_t<Messages>>
bool idsStrictlyIncrease(const Messages& messages)
{
    return std::ranges::adjacent_find(messages, std::ranges::greater_equal{},
                                      [](const auto& message) { return static_cast<std::uint64_t>(message.id()); }) ==
           std::ranges::end(messages);
}

} // namespace grpc_test_utils
