// Benchmarks of AsyncReadWriteStream:
//
//  [latency]     ping-pong round-trip time through a real in-process gRPC server
//  [throughput]  pipelined echo throughput for several message sizes
//  [queue]       cost of sendRequest() for queueing, dropping and evicting requests
//  [load]        heavy load against a slow server: memory consumption (C++ heap and
//                RSS), queue occupancy, drop counts, send / drain timings and
//                end-to-end latency for bounded and unbounded write queues
//
// All benchmarks are hidden test cases; run them explicitly, e.g.
//   GrpcAsyncStreamBenchmarks "[benchmark]"
//   GrpcAsyncStreamBenchmarks "[load]"

#include "MemoryTracker.h"

#include "support/FakeReaderWriter.h"
#include "support/StreamTestUtils.h"
#include "support/TestServer.h"

#include <AsyncReadWriteStream.h>

#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <string>
#include <vector>

namespace
{

using namespace TestSupport;
using Network::SendResult;
using Network::WriteOverflowPolicy;
using Network::WriteQueueLimits;

using Stream = Network::AsyncReadWriteStream<Request, Response>;
using Clock  = std::chrono::steady_clock;

std::uint64_t envOr(const char* name, std::uint64_t fallback)
{
    if (const char* value = std::getenv(name); value && *value)
        return std::strtoull(value, nullptr, 10);
    return fallback;
}

double toMiB(double bytes) { return bytes / (1024.0 * 1024.0); }

// Stream connected to an in-process test server
struct ServerStream
{
    TestServer                               server;
    std::unique_ptr<proto::TestStream::Stub> stub = server.makeStub();
    Stream                                   stream;

    explicit ServerStream(const WriteQueueLimits& limits = {})
        : stream(limits)
    {
        REQUIRE(stream.open([this](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
            return stub->AsyncChat(context, cq, tag);
        }));
        // Warm up: establish the call before measuring
        std::uint64_t received = 0;
        auto          handler  = [&](Response&&) { ++received; };
        stream.sendRequest(makeEcho(0));
        REQUIRE(pollUntil(stream, handler, [&] { return received == 1; }));
    }
};

// ---------------------------------------------------------------------------
// Heavy load scenario
// ---------------------------------------------------------------------------

struct LoadConfig
{
    std::string               name;
    WriteQueueLimits          limits;
    std::uint64_t             messages;
    std::size_t               payload_bytes;
    std::chrono::microseconds server_read_delay;
};

struct LoadResult
{
    std::string   name;
    std::uint64_t submitted          = 0;
    std::uint64_t written            = 0;
    std::uint64_t dropped            = 0;
    std::uint64_t received           = 0;
    std::size_t   peak_queued        = 0;
    std::size_t   peak_queued_bytes  = 0;
    std::int64_t  heap_peak_growth   = 0; // peak C++ heap above the baseline during the flood
    std::int64_t  rss_peak_growth    = 0; // peak sampled RSS above the baseline during the flood
    double        send_ns_per_msg    = 0; // mean sendRequest() cost incl. periodic polling
    double        produce_ms         = 0; // time to submit all requests
    double        drain_ms           = 0; // time from writesDone() to the end of the call
    double        latency_p50_ms     = 0; // send -> echo received, for delivered requests
    double        latency_p99_ms     = 0;
    double        latency_max_ms     = 0;
    bool          status_ok          = false;
};

LoadResult runHeavyLoad(const LoadConfig& config)
{
    ServerStream s(config.limits);
    s.server.service().setReadDelay(config.server_read_delay);

    // Preallocate everything the measurement itself needs, before the baseline
    std::vector<Clock::time_point> send_times(config.messages);
    std::vector<double>            latencies_ms;
    latencies_ms.reserve(config.messages);
    const std::string payload(config.payload_bytes, 'H');

    auto handler = [&](Response&& response) {
        if (response.id() < send_times.size())
            latencies_ms.push_back(std::chrono::duration<double, std::milli>(Clock::now() - send_times[response.id()]).count());
    };

    // The warm-up request sent by ServerStream is excluded from the results
    const Network::WriteQueueStats warm_up = s.stream.writeQueueStats();

    MemoryTracker::releaseFreeMemory();
    const std::int64_t heap_baseline = MemoryTracker::heap().live_bytes;
    const std::size_t  rss_baseline  = MemoryTracker::residentBytes();
    std::size_t        rss_peak      = rss_baseline;
    MemoryTracker::resetPeak();

    const auto produce_start = Clock::now();
    for (std::uint64_t id = 1; id < config.messages; ++id)
    {
        send_times[id] = Clock::now();
        const SendResult result = s.stream.sendRequest(makeEcho(id, payload));
        REQUIRE(result != SendResult::NotWritable);

        // A producer typically services the stream periodically
        if (id % 64 == 0)
            s.stream.poll(handler);
        if (id % 1024 == 0)
            rss_peak = std::max(rss_peak, MemoryTracker::residentBytes());
    }
    const auto produce_end = Clock::now();
    rss_peak               = std::max(rss_peak, MemoryTracker::residentBytes());
    const auto heap_peak   = MemoryTracker::heap().peak_bytes;

    REQUIRE(s.stream.writesDone());
    REQUIRE(pollUntilClosed(s.stream, handler, std::chrono::minutes(5)));
    const auto drain_end = Clock::now();

    const auto& stats = s.stream.writeQueueStats();
    LoadResult  result;
    result.name              = config.name;
    result.submitted         = stats.submitted_messages - warm_up.submitted_messages;
    result.written           = stats.written_messages - warm_up.written_messages;
    result.dropped           = stats.dropped_messages - warm_up.dropped_messages;
    result.received          = latencies_ms.size();
    result.peak_queued       = stats.peak_queued_messages;
    result.peak_queued_bytes = stats.peak_queued_bytes;
    result.heap_peak_growth  = heap_peak - heap_baseline;
    result.rss_peak_growth   = static_cast<std::int64_t>(rss_peak) - static_cast<std::int64_t>(rss_baseline);
    result.produce_ms        = std::chrono::duration<double, std::milli>(produce_end - produce_start).count();
    result.send_ns_per_msg   = result.produce_ms * 1e6 / static_cast<double>(config.messages - 1);
    result.drain_ms          = std::chrono::duration<double, std::milli>(drain_end - produce_end).count();
    result.status_ok         = s.stream.status().ok();

    if (!latencies_ms.empty())
    {
        std::sort(latencies_ms.begin(), latencies_ms.end());
        auto percentile = [&](double p) {
            const auto index = static_cast<std::size_t>(p * static_cast<double>(latencies_ms.size() - 1));
            return latencies_ms[index];
        };
        result.latency_p50_ms = percentile(0.50);
        result.latency_p99_ms = percentile(0.99);
        result.latency_max_ms = latencies_ms.back();
    }
    return result;
}

void printLoadResults(const LoadConfig& sample, const std::vector<LoadResult>& results)
{
    std::printf("\nHeavy load: %llu requests x %zu B payload, server consumes one request per >= %lld us\n",
                static_cast<unsigned long long>(sample.messages - 1), sample.payload_bytes,
                static_cast<long long>(sample.server_read_delay.count()));
    std::printf("%-34s %9s %9s %9s %8s %10s %10s %10s %9s %9s %9s %9s %9s\n", "write queue", "written", "dropped",
                "received", "peak q", "peak q MiB", "heap MiB", "RSS MiB", "send ns", "send ms", "drain ms",
                "p50 ms", "p99 ms");
    for (const LoadResult& r : results)
    {
        std::printf("%-34s %9llu %9llu %9llu %8zu %10.2f %10.2f %10.2f %9.0f %9.1f %9.1f %9.1f %9.1f\n",
                    r.name.c_str(), static_cast<unsigned long long>(r.written),
                    static_cast<unsigned long long>(r.dropped), static_cast<unsigned long long>(r.received),
                    r.peak_queued, toMiB(static_cast<double>(r.peak_queued_bytes)),
                    toMiB(static_cast<double>(r.heap_peak_growth)), toMiB(static_cast<double>(r.rss_peak_growth)),
                    r.send_ns_per_msg, r.produce_ms, r.drain_ms, r.latency_p50_ms, r.latency_p99_ms);
    }
    std::printf("\n");
    std::fflush(stdout);
}

WriteQueueLimits makeLimits(std::size_t max_messages, std::size_t max_bytes, WriteOverflowPolicy policy)
{
    WriteQueueLimits limits;
    limits.max_messages    = max_messages;
    limits.max_bytes       = max_bytes;
    limits.overflow_policy = policy;
    return limits;
}

} // namespace

// ---------------------------------------------------------------------------
// Catch2 benchmarks
// ---------------------------------------------------------------------------

TEST_CASE("Round-trip latency through a real gRPC call", "[!benchmark][benchmark][latency]")
{
    ServerStream  s;
    std::uint64_t received = 0;
    auto          handler  = [&](Response&&) { ++received; };

    const std::string small(64, 's');
    const std::string large(16 * 1024, 'l');

    BENCHMARK("ping-pong 64 B")
    {
        const std::uint64_t expected = received + 1;
        s.stream.sendRequest(makeEcho(expected, small));
        while (received < expected)
            s.stream.pollFor(handler, std::chrono::milliseconds(1));
        return received;
    };

    BENCHMARK("ping-pong 16 KiB")
    {
        const std::uint64_t expected = received + 1;
        s.stream.sendRequest(makeEcho(expected, large));
        while (received < expected)
            s.stream.pollFor(handler, std::chrono::milliseconds(1));
        return received;
    };

    CHECK(s.stream.writeQueueStats().dropped_messages == 0);
}

TEST_CASE("Pipelined echo throughput", "[!benchmark][benchmark][throughput]")
{
    constexpr std::uint64_t batch = 1000;

    for (const std::size_t payload_size : {std::size_t{64}, std::size_t{1024}, std::size_t{16 * 1024}})
    {
        ServerStream      s(WriteQueueLimits::unbounded());
        std::uint64_t     received = 0;
        auto              handler  = [&](Response&&) { ++received; };
        const std::string payload(payload_size, 't');

        BENCHMARK(std::to_string(batch) + " echoes x " + std::to_string(payload_size) + " B")
        {
            const std::uint64_t expected = received + batch;
            for (std::uint64_t i = 0; i < batch; ++i)
                s.stream.sendRequest(makeEcho(i, payload));
            while (received < expected)
                s.stream.pollFor(handler, std::chrono::milliseconds(1));
            return received;
        };

        CHECK(s.stream.writeQueueStats().dropped_messages == 0);
    }
}

TEST_CASE("Cost of sendRequest() on the write queue", "[!benchmark][benchmark][queue]")
{
    // A fake call whose start-call never completes keeps every request in the queue
    const Request request = makeEcho(1, std::string(256, 'q'));

    BENCHMARK_ADVANCED("enqueue")(Catch::Benchmark::Chronometer meter)
    {
        auto   call = std::make_shared<FakeCall<Request, Response>>();
        Stream stream(WriteQueueLimits::unbounded());
        REQUIRE(stream.open(makeFakeStreamFactory(call)));
        std::vector<Request> requests(static_cast<std::size_t>(meter.runs()), request);
        meter.measure([&](int i) { return stream.sendRequest(std::move(requests[static_cast<std::size_t>(i)])); });
    };

    BENCHMARK_ADVANCED("drop newest (queue full)")(Catch::Benchmark::Chronometer meter)
    {
        auto   call = std::make_shared<FakeCall<Request, Response>>();
        Stream stream(makeLimits(1024, WriteQueueLimits::unlimited, WriteOverflowPolicy::DropNewest));
        REQUIRE(stream.open(makeFakeStreamFactory(call)));
        for (int i = 0; i < 1024; ++i)
            stream.sendRequest(request);
        std::vector<Request> requests(static_cast<std::size_t>(meter.runs()), request);
        meter.measure([&](int i) { return stream.sendRequest(std::move(requests[static_cast<std::size_t>(i)])); });
    };

    BENCHMARK_ADVANCED("evict oldest + enqueue (queue full)")(Catch::Benchmark::Chronometer meter)
    {
        auto   call = std::make_shared<FakeCall<Request, Response>>();
        Stream stream(makeLimits(1024, WriteQueueLimits::unlimited, WriteOverflowPolicy::DropOldest));
        REQUIRE(stream.open(makeFakeStreamFactory(call)));
        for (int i = 0; i < 1024; ++i)
            stream.sendRequest(request);
        std::vector<Request> requests(static_cast<std::size_t>(meter.runs()), request);
        meter.measure([&](int i) { return stream.sendRequest(std::move(requests[static_cast<std::size_t>(i)])); });
    };
}

TEST_CASE("Heavy load: memory and timings with bounded and unbounded write queues", "[!benchmark][benchmark][load]")
{
    const std::uint64_t messages      = envOr("GRPC_ASYNC_STREAM_BENCH_MESSAGES", 50000) + 1;
    const std::size_t   payload_bytes = static_cast<std::size_t>(envOr("GRPC_ASYNC_STREAM_BENCH_PAYLOAD", 1024));
    const auto          server_delay  = std::chrono::microseconds(envOr("GRPC_ASYNC_STREAM_BENCH_SERVER_DELAY_US", 20));

    // Bounded configurations run first: RSS rarely shrinks after the unbounded
    // run, which would hide the RSS growth of the runs following it.
    const std::vector<LoadConfig> configs = {
        {"default: 1024 msgs/64 MiB, oldest", WriteQueueLimits{}, messages, payload_bytes, server_delay},
        {"256 msgs, drop oldest", makeLimits(256, WriteQueueLimits::unlimited, WriteOverflowPolicy::DropOldest),
         messages, payload_bytes, server_delay},
        {"256 msgs, drop newest", makeLimits(256, WriteQueueLimits::unlimited, WriteOverflowPolicy::DropNewest),
         messages, payload_bytes, server_delay},
        {"256 KiB, drop oldest", makeLimits(WriteQueueLimits::unlimited, 256 * 1024, WriteOverflowPolicy::DropOldest),
         messages, payload_bytes, server_delay},
        {"unbounded", WriteQueueLimits::unbounded(), messages, payload_bytes, server_delay},
    };

    std::vector<LoadResult> results;
    for (const LoadConfig& config : configs)
        results.push_back(runHeavyLoad(config));

    printLoadResults(configs.front(), results);

    const LoadResult& unbounded = results.back();
    CHECK(unbounded.status_ok);
    CHECK(unbounded.dropped == 0);
    CHECK(unbounded.written == unbounded.submitted);

    for (std::size_t i = 0; i + 1 < results.size(); ++i)
    {
        const LoadResult&       r      = results[i];
        const WriteQueueLimits& limits = configs[i].limits;
        INFO(r.name);
        CHECK(r.status_ok);
        CHECK(r.peak_queued <= limits.max_messages);
        CHECK(r.peak_queued_bytes <= limits.max_bytes);
        CHECK(r.written + r.dropped == r.submitted);
        CHECK(r.received == r.written);
        // The bounded queue must keep the heap far below the unbounded case
        CHECK(r.heap_peak_growth < unbounded.heap_peak_growth / 2);
    }
}
