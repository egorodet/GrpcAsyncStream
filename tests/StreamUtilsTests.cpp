// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

// Tests of the StreamUtils.h helpers: every exit of pollUntil() / pollUntilClosed() for streams
// with and without a response handler, and the message-sequence checks.

#include "support/TestServer.h"

#include <grpc_async_stream_test/FakeReaderWriter.h>
#include <grpc_async_stream/AsyncReadWriteStream.h>
#include <grpc_async_stream/AsyncWriteStream.h>
#include <grpc_async_stream/StreamUtils.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <forward_list>
#include <functional>
#include <limits>
#include <list>
#include <memory>
#include <initializer_list>
#include <ranges>
#include <vector>

namespace
{

using namespace TestSupport;
using namespace grpc_async_stream_test;
using grpc_async_stream::idsOf;
using grpc_async_stream::idsStrictlyIncrease;
using grpc_async_stream::pollUntil;
using grpc_async_stream::pollUntilClosed;

using Stream      = grpc_async_stream::AsyncReadWriteStream<Request, Response>;
using WriteStream = grpc_async_stream::AsyncWriteStream<Request>;

// Short enough to keep the timeout tests fast, long enough for several 1 ms polls
constexpr std::chrono::milliseconds short_timeout(20);
constexpr std::chrono::seconds      long_timeout(5);

Response makeResponse(std::uint64_t id)
{
    Response response;
    response.set_id(id);
    return response;
}

// Predicate counting its checks, true from check `true_from` on. Held in a std::function, it may throw
// like most real predicates, so GCC instruments the exception edges of the predicate calls as well.
std::function<bool()> countingPredicate(int& checks, int true_from = std::numeric_limits<int>::max())
{
    return [&checks, true_from] { return ++checks >= true_from; };
}

std::vector<Response> makeResponses(std::initializer_list<std::uint64_t> ids)
{
    std::vector<Response> responses;
    for (const std::uint64_t id : ids)
        responses.push_back(makeResponse(id));
    return responses;
}

// Bidirectional stream over a fake call whose start-call never completes: it stays open and idle
struct IdleStreamFixture
{
    std::shared_ptr<FakeCall<Request, Response>> call = std::make_shared<FakeCall<Request, Response>>();
    Stream                                       stream;
    std::function<void(Response&&)>              handler = [](Response&&) {};

    IdleStreamFixture() { REQUIRE(stream.open(makeFakeStreamFactory(call))); }
};

// Write stream over a fake call whose start-call never completes: it stays open and idle
struct IdleWriteStreamFixture
{
    std::shared_ptr<FakeCall<Request, UploadSummary>> call = std::make_shared<FakeCall<Request, UploadSummary>>();
    UploadSummary                                     summary; // must outlive the call
    WriteStream                                       stream;

    IdleWriteStreamFixture() { REQUIRE(stream.open(makeFakeWriterFactory(call, &summary))); }
};

} // namespace

TEST_CASE("pollUntil() with a handler returns the predicate when the timeout expires", "[stream-utils]")
{
    IdleStreamFixture f;
    int               checks = 0;
    CHECK_FALSE(pollUntil(f.stream, f.handler, countingPredicate(checks), short_timeout));
    CHECK(checks > 2); // checked before every poll and once more at the deadline
    CHECK(f.stream.isOpen());

    // A predicate turning true at the final check after the deadline still wins
    checks = 0;
    CHECK(pollUntil(f.stream, f.handler, countingPredicate(checks, 2), std::chrono::milliseconds(0)));
}

TEST_CASE("pollUntil() with a handler returns the predicate when the call ends", "[stream-utils]")
{
    Stream                          stream; // never opened: pollFor() reports the call has ended
    std::function<void(Response&&)> handler = [](Response&&) {};
    int                             checks  = 0;

    CHECK_FALSE(pollUntil(stream, handler, countingPredicate(checks), long_timeout));
    CHECK(checks == 2); // before the poll and after it

    checks = 0;
    CHECK(pollUntil(stream, handler, countingPredicate(checks, 2), long_timeout));
    CHECK(checks == 2);

    CHECK(pollUntil(stream, handler, [] { return true; }, long_timeout));
}

TEST_CASE("pollUntilClosed() with a handler reports whether the call ended in time", "[stream-utils]")
{
    IdleStreamFixture f;
    CHECK_FALSE(pollUntilClosed(f.stream, f.handler, short_timeout));
    CHECK(f.stream.isOpen());

    f.stream.close();
    CHECK(pollUntilClosed(f.stream, f.handler, short_timeout));
    CHECK(f.call->violations().empty());
}

TEST_CASE("pollUntil() without a handler returns the predicate when the timeout expires", "[stream-utils]")
{
    IdleWriteStreamFixture f;
    int                    checks = 0;
    CHECK_FALSE(pollUntil(f.stream, countingPredicate(checks), short_timeout));
    CHECK(checks > 2);
    CHECK(f.stream.isOpen());

    checks = 0;
    CHECK(pollUntil(f.stream, countingPredicate(checks, 2), std::chrono::milliseconds(0)));
}

TEST_CASE("pollUntil() without a handler returns the predicate when the call ends", "[stream-utils]")
{
    WriteStream stream; // never opened
    int         checks = 0;

    CHECK_FALSE(pollUntil(stream, countingPredicate(checks), long_timeout));
    CHECK(checks == 2);

    checks = 0;
    CHECK(pollUntil(stream, countingPredicate(checks, 2), long_timeout));
    CHECK(checks == 2);

    CHECK(pollUntil(stream, [] { return true; }, long_timeout));
}

TEST_CASE("pollUntilClosed() without a handler reports whether the call ended in time", "[stream-utils]")
{
    IdleWriteStreamFixture f;
    CHECK_FALSE(pollUntilClosed(f.stream, short_timeout));
    CHECK(f.stream.isOpen());

    f.stream.close();
    CHECK(pollUntilClosed(f.stream, short_timeout));
    CHECK(f.call->violations().empty());
}

TEST_CASE("idsOf() lists the message ids of any input range in order", "[stream-utils]")
{
    const std::vector<Response> responses = makeResponses({3, 1, 2});
    CHECK(idsOf(responses) == std::vector<std::uint64_t>{3, 1, 2});
    CHECK(idsOf(std::vector<Response>{}).empty());

    // Not a sized range: no reservation, same result
    const std::forward_list<Response> listed(responses.begin(), responses.end());
    static_assert(!std::ranges::sized_range<decltype(listed)>);
    CHECK(idsOf(listed) == std::vector<std::uint64_t>{3, 1, 2});
}

TEST_CASE("idsStrictlyIncrease() rejects repeated and decreasing ids", "[stream-utils]")
{
    CHECK(idsStrictlyIncrease(std::vector<Response>{}));
    CHECK(idsStrictlyIncrease(makeResponses({7})));
    CHECK(idsStrictlyIncrease(makeResponses({1, 2, 10})));
    CHECK_FALSE(idsStrictlyIncrease(makeResponses({1, 2, 2})));
    CHECK_FALSE(idsStrictlyIncrease(makeResponses({2, 1})));

    const std::vector<Response> responses = makeResponses({1, 5, 9});
    const std::list<Response>   listed(responses.begin(), responses.end());
    CHECK(idsStrictlyIncrease(listed));
}
