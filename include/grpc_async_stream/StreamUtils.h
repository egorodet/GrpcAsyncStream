// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

#pragma once

// Helpers for polling streams until a condition holds and for checking message sequences.
// Requires C++20; depends only on the C++ standard library.

#include <algorithm>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <functional>
#include <iterator>
#include <ranges>
#include <vector>

namespace grpc_async_stream
{

// Anything that can be polled like AsyncReadWriteStream
template<typename Stream>
concept PollableStream = requires(Stream& stream) {
    { stream.isOpen() } -> std::convertible_to<bool>;
};

// Stream polled with a response handler, like AsyncReadWriteStream / AsyncReadStream
template<typename Stream, typename Handler>
concept PollableWithHandler = PollableStream<Stream> && requires(Stream& stream, Handler& handler) {
    { stream.pollFor(handler, std::chrono::milliseconds(1)) } -> std::convertible_to<bool>;
};

// Stream polled without a handler, like AsyncWriteStream
template<typename Stream>
concept PollableWithoutHandler = PollableStream<Stream> && requires(Stream& stream) {
    { stream.pollFor(std::chrono::milliseconds(1)) } -> std::convertible_to<bool>;
};

// Polls until the predicate holds, the call ends or the timeout expires; returns the predicate.
template<PollableStream Stream, typename Handler, std::predicate Predicate>
    requires PollableWithHandler<Stream, Handler>
bool pollUntil(Stream& stream, Handler&& handler, Predicate&& predicate,
               std::chrono::milliseconds timeout = std::chrono::seconds(10))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return predicate();
        if (!stream.pollFor(handler, std::chrono::milliseconds(1)))
            return predicate();
    }
    return true;
}

// Polls until the call ends or the timeout expires; returns true if the call ended.
template<PollableStream Stream, typename Handler>
    requires PollableWithHandler<Stream, Handler>
bool pollUntilClosed(Stream& stream, Handler&& handler, std::chrono::milliseconds timeout = std::chrono::seconds(10))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (stream.isOpen())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        stream.pollFor(handler, std::chrono::milliseconds(1));
    }
    return true;
}

// pollUntil() for streams without responses (AsyncWriteStream)
template<PollableWithoutHandler Stream, std::predicate Predicate>
bool pollUntil(Stream& stream, Predicate&& predicate, std::chrono::milliseconds timeout = std::chrono::seconds(10))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return predicate();
        if (!stream.pollFor(std::chrono::milliseconds(1)))
            return predicate();
    }
    return true;
}

// pollUntilClosed() for streams without responses (AsyncWriteStream)
template<PollableWithoutHandler Stream>
bool pollUntilClosed(Stream& stream, std::chrono::milliseconds timeout = std::chrono::seconds(10))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (stream.isOpen())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        stream.pollFor(std::chrono::milliseconds(1));
    }
    return true;
}

// Message with an integral id() accessor
template<typename Message>
concept MessageWithId = requires(const Message& message) {
    { message.id() } -> std::convertible_to<std::uint64_t>;
};

// Ids of a range of messages, in order
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

// True when the message ids strictly increase
template<std::ranges::forward_range Messages>
    requires MessageWithId<std::ranges::range_value_t<Messages>>
bool idsStrictlyIncrease(const Messages& messages)
{
    return std::ranges::adjacent_find(messages, std::ranges::greater_equal{},
                                      [](const auto& message) { return static_cast<std::uint64_t>(message.id()); }) ==
           std::ranges::end(messages);
}

} // namespace grpc_async_stream
