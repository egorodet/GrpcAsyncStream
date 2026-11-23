#pragma once

#include <AsyncReadWriteStream.h>

#include <catch2/catch_tostring.hpp>

#include <algorithm>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <iterator>
#include <ranges>
#include <string>
#include <vector>

namespace TestSupport
{

using namespace std::chrono_literals;

// Anything that can be polled like AsyncReadWriteStream
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

// The ids of a range of messages, in order
template<std::ranges::input_range Messages>
std::vector<std::uint64_t> idsOf(const Messages& messages)
{
    std::vector<std::uint64_t> ids;
    ids.reserve(std::ranges::size(messages));
    std::ranges::transform(messages, std::back_inserter(ids), [](const auto& message) { return message.id(); });
    return ids;
}

// True when the ids of the messages strictly increase
template<std::ranges::forward_range Messages>
bool idsStrictlyIncrease(const Messages& messages)
{
    return std::ranges::adjacent_find(messages, std::ranges::greater_equal{},
                                      [](const auto& message) { return message.id(); }) == std::ranges::end(messages);
}

} // namespace TestSupport

// Readable names of the library's enumerations in Catch2 failure messages
template<>
struct Catch::StringMaker<Network::SendResult>
{
    static std::string convert(Network::SendResult value) { return std::string(Network::toString(value)); }
};

template<>
struct Catch::StringMaker<Network::WriteOverflowPolicy>
{
    static std::string convert(Network::WriteOverflowPolicy value) { return std::string(Network::toString(value)); }
};
