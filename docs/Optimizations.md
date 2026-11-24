# Performance optimizations

This document records how the stream classes were profiled under heavy load, which bottlenecks were
found, what was changed, and what the changes measurably gained. It also lists what was deliberately
left alone and why. The [second round](#second-round-no-copies-on-the-write-path) removes the remaining
copies and moves of requests on the write path. The [third round](#third-round-polling-without-stalls)
removes a stall of about 50 µs from the end of every `pollFor()` call and from `poll()` of an idle write
stream, which made a round trip about nine times faster, and measures the second round with real gRPC.

## Method

**Benchmarks.** `GrpcAsyncStreamBenchmarks` (see [Benchmarks](../README.md#benchmarks)) was run in a
Release build (MSVC 14.52, gRPC 1.72.0 from Conan, Windows 11, x64) before and after every change.
Two benchmark tags were added for the classes that had none:

| Tag | Class | Workload |
|---|---|---|
| `[read]` | `AsyncReadStream` | Server-streaming call delivering 1000 responses of 64 B, 1 KiB and 16 KiB |
| `[write]` | `AsyncWriteStream` | Client-streaming call sending 1000 requests of 64 B, 1 KiB and 16 KiB, then the half-close and the final status |

**Profiling.** ETW-based profilers (WPR/xperf) need an elevated shell, so a small external sampling
profiler was used instead. It starts the benchmark executable, and about once per millisecond
suspends every thread whose CPU cycle counter (`QueryThreadCycleTime`) advanced since the previous
sample, walks its stack with `StackWalk64`, and resumes it. Samples whose top frame is a kernel wait
are discarded, and symbols are resolved with DbgHelp from the RelWithDebInfo PDBs. The profiles
covered the `[throughput]` benchmark and the `[load]` scenario scaled up to 1.5 million requests with
no server delay (`GRPC_ASYNC_STREAM_BENCH_MESSAGES=1500000 GRPC_ASYNC_STREAM_BENCH_SERVER_DELAY_US=0`),
so that the run is CPU-bound instead of waiting for the slow server.

**Noise.** On the development machine whole-run results drift by up to about 25 % between runs.
Changes were therefore judged by interleaved A/B runs (old build, new build, old build, new build)
and by medians of several repetitions, never by a single before/after pair.

## Findings

In the CPU-bound load profile, about 90 % of the samples are on the client thread, because the
in-process transport runs much of the server's work inline. Almost all of that time is spent in
gRPC (`grpc_call_start_batch`, `ExecCtx::Flush`, completion queue polling) and in protobuf
serialization and parsing. The library's own frames account for only a few percent. Within that
budget, three costs stood out, plus one in the benchmark harness itself:

1. **The benchmark's heap tracker was the top exclusive hotspot.** `RtlSizeHeap` took 12 % of the
   exclusive samples and 19 % inclusive. `MemoryTracker.cpp` replaces the global `operator new` /
   `delete` and called `_msize` (glibc `malloc_usable_size`, macOS `malloc_size`) on every
   allocation and deallocation. On Windows `_msize` takes the heap lock, so the benchmarks were
   partly measuring their own instrumentation.
2. **The write queue allocated once per queued request.** `AsyncWriteStream` buffered requests in a
   `std::deque<PendingWrite>`. MSVC's `std::deque` stores 16 bytes per block, which is one element
   for anything larger than 8 bytes, so every `sendRequest()` that queued a request allocated a
   block and every dequeue freed it, plus occasional growth of the block map.
3. **Every response was parsed into a freshly allocated message.** `AsyncReadStream` moves each
   received message out of its read buffer before handing it to the handler. The read buffer is
   then empty, so gRPC's parser had to allocate new storage for every `string`, `bytes` and
   repeated field of every response, and the handler's copy freed it again
   (`ArenaStringPtr::Set`, `MutableNoCopy`, `malloc` / `free` in the profile).
4. **`pollFor()` passed a `system_clock` deadline.** gRPC converts a realtime deadline to its
   monotonic clock on every `AsyncNext` call (`gpr_convert_clock_type` in the profile). This is a
   small cost, but the realtime deadline is also affected by changes of the wall clock.

## Changes

### 1. Ring buffer for the write queue

`include/grpc_utils/AsyncWriteStream.h`

The `std::deque` was replaced by `detail::RingQueue<T>`: a FIFO on a `std::vector` whose capacity is
a power of two (starting at 16) and doubles when full. Pushing moves the element into a free slot,
and popping moves it out and leaves the slot in the moved-from state. For protobuf messages that
state is an empty message that owns no heap memory. In steady state, enqueueing and dequeueing
allocate nothing, on every standard library implementation.

The existing memory policy is unchanged: once the queue drains after holding more than 256
requests, `release()` frees the storage, so the footprint returns to the baseline after a load
peak. `close()` and the end of a call release it as before.

Two tests were added to `tests/WriteQueueTests.cpp`. One checks `RingQueue` directly for FIFO order
when it grows while its contents wrap around the end of the storage. The other sends requests
through a stream so that the queue wraps around and grows, and checks that the requests are written
in order.

### 2. Reuse of the response buffer on reads

`include/grpc_utils/AsyncReadStream.h`

`AsyncReadStream` now keeps a second message, `m_spare_response`. When a read completes:

1. the received message is moved out of `m_pending_response` into a local, as before;
2. the spare message becomes the new read buffer, and the next `Read` is armed into it;
3. the handler receives the local message;
4. afterwards, unless the handler closed the stream, the local is `Clear()`ed and becomes the spare.

`Clear()` keeps the capacity of `string`, `bytes` and repeated fields, so a handler that only
inspects a message lets gRPC parse the following responses into already allocated storage. A
handler that takes the message (moves from it) leaves an empty message behind, which costs nothing
extra.

The handler contract is unchanged. The handler still owns its message: closing, reopening or
polling the stream from inside the handler does not affect it, because it is a local and never
aliases a buffer that the stream reads into.

The trade-off is memory: while a call is open the stream may hold one extra message's capacity, at
most the size of a recent response that the handler did not take. `close()` now resets both buffers
to empty messages, which releases that memory. Before, it only called `Clear()`, which kept it.

### 3. Monotonic deadline in `pollFor()`

`include/grpc_utils/AsyncCallBase.h`

`deadlineAfter()` now returns a `gpr_timespec` on `GPR_CLOCK_MONOTONIC` instead of a
`std::chrono::system_clock::time_point`. This saves gRPC a clock conversion on every `pollFor()`, and
the wait can no longer be stretched or cut short when the wall clock changes. Timeouts are rounded
up to whole microseconds. A timeout of zero or less becomes a non-blocking poll, and a timeout of a
year or more becomes an unbounded wait instead of overflowing the arithmetic.

### 4. Freeing the written request

*Superseded by the [second round](#second-round-no-copies-on-the-write-path): the stream no longer keeps
the request being written at all.*

`include/grpc_utils/AsyncWriteStream.h`

After a write completes, the code was commented "free its memory now" but called
`m_current_write.Clear()`, which keeps the capacity of the request's fields. An idle stream therefore
kept the memory of its last written request. It now assigns an empty message, which frees it. The
cost per write is the same: the old message used to be freed when the next request was moved in;
now it is freed one step earlier.

### 5. Heap tracking without allocator queries

`benchmarks/MemoryTracker.cpp`, `benchmarks/MemoryTracker.h`

The replacement `operator new` now allocates a small header in front of each block, holding the
requested size, and `operator delete` reads it back from there. The header is
`max(alignof(std::max_align_t), sizeof(std::size_t))` bytes, so the pointer returned to the caller
keeps the alignment that `malloc` guarantees. This removes the allocator queries from the measured
code, and heap tracking now works on every platform, not only on those with a usable-size API.

The figures now count the **requested** bytes instead of the allocator's usable size, so heap
numbers are slightly lower than before for the same workload (allocator rounding is no longer
counted). The aligned `operator new` overloads keep their default implementations, as before.

## Results

Release build, MSVC, same machine. "Before" is the code before these changes, measured with the
fixed heap tracker so that both sides carry the same instrumentation cost.

**Write queue** (median of 4 runs × 100 samples each):

| `[queue]` benchmark | Before | After |
|---|---:|---:|
| `sendRequest()` enqueue | ~78 ns | ~66 ns |
| `sendRequest()` evict oldest + enqueue, queue full | ~85 ns | ~65 ns |
| `sendRequest()` drop newest, queue full | ~33 ns | ~37 ns |

The drop-newest path does not touch the queue's storage, so the difference there is noise.

**Reads** (`[read]`, interleaved A/B pairs, 60 samples each):

| Payload | Change across three pairs |
|---|---|
| 64 B | within noise |
| 1 KiB | 3–17 % faster |
| 16 KiB | 5–25 % faster |

**Memory** (`[load]`, default limits of 1024 messages / 64 MiB): the peak heap growth of the bounded
queue dropped from 1.19 MiB to 1.11 MiB, because the deque's per-element blocks and block map are
gone.

**End-to-end throughput** (`[throughput]`, `[write]`) changed little. gRPC allows only one outstanding
write per call, and each message costs about 3–5 µs inside gRPC and protobuf, which dwarfs the
tens of nanoseconds saved in the library.

**Correctness.** All 85 tests pass in Release and in the AddressSanitizer build (which keeps
assertions enabled), and every benchmark runs cleanly under AddressSanitizer with the project's
usual `ASAN_OPTIONS=detect_container_overflow=0`. These changes were verified with MSVC only; the
GCC and Clang configurations are covered by CI.

## Not changed

- **`ByteSizeLong()` in `sendRequest()`.** The byte limit and the queue statistics need the size of
  every queued request. It did not show up in the profiles.
- **Write batching.** gRPC can coalesce consecutive small writes when `WriteOptions::set_buffer_hint()`
  is set on all but the last one. Writing queued requests with that hint is the most promising way to
  raise throughput beyond one message per write round trip. It was not attempted because the
  in-process channel used by the benchmarks does not batch, so the effect could not be measured; it
  needs a benchmark over a real TCP (HTTP/2) channel first.
- **Completion queue polling.** Each event costs one `AsyncNext` call, which is the gRPC API's
  minimum.
- **The heavy-load `send ns` figure for the unbounded queue** (about 440 ns compared with about 190 ns
  for bounded queues) comes from the benchmark itself: building a 1 KiB request per send and growing
  the process by about 50 MiB. It is not a cost of the library.

## Second round: no copies on the write path

### Findings

The first round left the write path with more copies and moves than gRPC requires:

1. **The stream kept its own copy of the request being written.** `startWrite()` moved every request
   into the member `m_current_write` and passed that to `Write()`, "kept alive until the write
   completes". gRPC does not need this: `ClientAsyncWriterInterface::Write()` serializes the message
   into its own byte buffer before returning, and the gRPC API documents that it "doesn't take
   ownership or a reference to msg, so it is safe to deallocate once Write returns"
   (`grpcpp/support/async_stream.h`, `AsyncWriterInterface::Write`). While a write was in flight the
   stream therefore held each request twice: once as a message object, once serialized inside gRPC.
2. **`sendRequest(RequestProto request)` took the request by value.** A caller passing an lvalue, as
   in the README examples (`stream.sendRequest(request)`), paid for a full copy (an allocation plus a
   copy of every field) on every call, including the common case where the request is written at
   once and the cases where it is dropped or the stream is not writable.
3. **A queued request was moved six times.** Into `enqueue()`'s by-value parameter, into a
   `PendingWrite`, into its slot, out of the slot, out of the `PendingWrite`, into `m_current_write`;
   one written at once was moved twice. For a protobuf message every move is a default construction, a
   field-by-field swap and a destruction.
4. **The ring buffer constructed empty messages.** It stored its slots in a `std::vector<T>`, so
   every growth default-constructed a message in every new slot before moving the live ones over, and
   popped slots kept moved-from messages until they were overwritten or released.

### Changes

All in `include/grpc_utils/AsyncWriteStream.h`.

**Write without a member copy.** `m_current_write` is gone. `startWrite()` takes a `const&` and passes
it straight to `Write()`. A queued request is written straight from its queue slot, and the slot is
destroyed right after, which frees the request's memory at once. While a write is in flight, the
request exists only in serialized form inside gRPC. The write statistics are unchanged: they never
needed the message itself.

**Two overloads of `sendRequest()`.** `sendRequest(const RequestProto&)` and
`sendRequest(RequestProto&&)` replace the by-value overload:

| Outcome | lvalue, before | lvalue, after | rvalue, before | rvalue, after |
|---|---|---|---|---|
| `Written` | 1 copy, 2 moves | nothing | 2–3 moves | nothing |
| `Queued`, then written | 1 copy, 6 moves | 1 copy (into the slot) | 6–7 moves | 1 move (into the slot) |
| `Dropped` | 1 copy, 1 move | nothing | 1–2 moves | nothing |
| `NotWritable` | 1 copy | nothing | 0–1 move | nothing |

The ranges for rvalues depend on whether the argument is a temporary, which initializes the old
by-value parameter without a move, or a `std::move`d object. These counts were taken with an
instrumented message type. Before, each completed write also move-assigned an empty message to
`m_current_write` to free it.

Every existing call compiles unchanged, including braced initializers such as
`sendRequest({.value = 1})`, which pick the rvalue overload. Only code that takes the address of
`sendRequest` must now select an overload.

**`RingQueue` on raw storage.** The queue now manages uninitialized storage from `std::allocator<T>`:
`emplaceBack()` constructs an element in its slot (from the caller's request, copied or moved exactly
once), `dropFront()` / `dropBack()` destroy one in place, and growing move-constructs only the live
elements into the new storage. Free slots hold no objects. `popFront()` / `popBack()` remain for code
that needs the element. Growing uses `std::move_if_noexcept`, and if copying an element throws, the
new storage is freed and the queue keeps its elements, so the queue still accepts any movable type, as
the `ProtobufMessage` concept allows. Protobuf messages have `noexcept` moves and are always moved.

### Results

gRPC and protobuf are not available in the environment this round was done in, so the gRPC benchmarks
could not be run. The library's own overhead was measured instead, A/B against the first round's
headers: both versions of the real headers were compiled (GCC 13.3, `-O2`, Linux x86-64) against a
minimal stand-in for the gRPC headers whose `Write()` copies the payload into a reused buffer (standing
in for serialization, without allocating) and completes at once. The message is a protobuf-like class
with a `uint64` id and a `std::string` payload. Medians of 15 repetitions of 20 000 requests, two
interleaved runs per version; the machine's run-to-run noise is up to about 25 %.

| Scenario, ns per request | 64 B | 1 KiB | 16 KiB |
|---|---|---|---|
| Written at once, lvalue | 37 → 11 | 54 → 21 | 311 → 127 |
| Written at once, rvalue (destroyed by the caller) | 34 → 19 | within noise | within noise |
| 20 000 rvalues queued, then written | 66–73 → 31–55 | 100–140 → 66–88 | within noise |
| Steady queue of 64: enqueue one, write one | 40–47 → 24–31 | 82–144 → 82–101 | 1540 → 1460 |
| Evict oldest + enqueue, queue of 1024 full | 20–28 → 13–16 | 39–57 → 28–29 | within noise |
| Dropped (`DropNewest`, full), lvalue | 16–20 → 1 | 27 → 1 | 113 → 1 |

Heap allocations per request fell from 1 to 0 for an lvalue that is written at once or dropped. With
1 KiB and larger payloads, the rvalue paths are dominated by the allocation and release of the payload
itself, which the library cannot avoid, so the saved moves disappear in the noise there.

**Memory.**

- While a write is in flight, the stream no longer holds the request: under a slow server that is one
  message object per stream fewer, besides gRPC's serialized copy.
- A request's memory is released as soon as it leaves the queue (written, evicted or dropped), never
  later.
- The stream object is one request message smaller (`sizeof` of the test stream: 352 → 304 bytes).
- Callers that keep reusing an lvalue request no longer cause an allocation per send.

**Correctness.** The stream headers and the new tests were compiled with the project's warning flags
(`-Wall -Wextra -Wpedantic -Wshadow -Wconversion -Werror`) by GCC 13.3 and Clang 18 against the
stand-in gRPC headers, and scripted scenarios (FIFO order, both overflow policies, byte limits,
tightening the limits, a failed write, half-close, a handler closing the stream, close with queued
requests) ran through the repository's `FakeCall` with AddressSanitizer and UndefinedBehaviorSanitizer,
with no leaked message. Three tests were added to `tests/WriteQueueTests.cpp`: that an lvalue request
is copied only when queued and otherwise left untouched, that the ring buffer destroys elements as soon
as they leave it (including across wrap-around and growth), and that a failing copy while growing
leaves the queue intact. **The full test suite and the gRPC benchmarks still need to be run** with real
gRPC (MSVC, GCC and Clang through CI).

### Measured with real gRPC

The effect was expected to be largest where requests are sent as lvalues, are large, or are dropped:

- `[queue]`: evict + enqueue should gain a little; drop newest passes rvalues and is unaffected.
- `[throughput]` / `[write]` with 16 KiB payloads: one move fewer per request is small next to
  serialization; expect at most a few percent.
- `[load]` with `GRPC_ASYNC_STREAM_BENCH_PAYLOAD=16384`: the heap figure should drop by about one
  payload per stream, and drops stay allocation-free.

The [third round](#third-round-polling-without-stalls) took these measurements on Linux with GCC 15.2 and
gRPC 1.72.0: r1 is built with the first round's headers, r2 with the second round's, both from the same
benchmark source and heap tracker (medians of 5 interleaved runs; see the third round for the setup).

| Benchmark | r1 | r2 | Change |
|---|---:|---:|---|
| `sendRequest()` enqueue | 23.9 ns | 13.0 ns | −46 % |
| `sendRequest()` evict oldest + enqueue, queue full | 43.8 ns | 33.6 ns | −23 % |
| `sendRequest()` drop newest, queue full | 33.8 ns | 2.8 ns | −92 % |
| `[throughput]` 1000 echoes × 16 KiB | 3.53 ms | 3.60 ms | within noise |
| `[write]` 1000 requests × 64 B / 1 KiB / 16 KiB | 1.99 / 1.91 / 2.47 ms | 2.08 / 2.04 / 2.62 ms | +4 / +7 / +6 % |
| `[load]` 16 KiB payloads, default queue: peak heap growth | 16.22 MiB | 16.20 MiB | −1 payload |
| `[load]` 16 KiB payloads, unbounded: peak heap growth | 307.46 MiB | 307.08 MiB | −0.4 MiB |

- **`[queue]` gained far more than expected.** The benchmark passes rvalues, so the stand-in measurement
  predicted little, but with real protobuf messages every saved move is a default construction, a swap
  of all fields and a destruction, and the drop-newest path no longer moves the request at all.
- **`[load]` with 16 KiB payloads:** the heap figure dropped by one payload per stream, as expected.
- **`[write]` measured 4 to 7 % slower** with the second round's headers. The cause is the order of allocation and release, not
  the code path: the second round destroys a queued request right after `Write()` has serialized it, so
  gRPC's serialization buffer for the next request is allocated before the previous payload is freed;
  the first round freed the written request when its write completed, just before the next
  serialization, which lets glibc's allocator hand the same, still cached block straight back. A
  diagnostic build that destroys the queue slot only when its write completes (writing it still
  straight from the slot) measured 1.97 / 1.94 / 2.38 ms against 1.94 / 1.95 / 2.45 ms for r1 and
  2.03 / 2.04 / 2.51 ms for r2 (8 interleaved runs). It was not adopted: it would hold one request per
  stream for the duration of every write again, which is the memory the second round removed, for a
  gain of 2 to 5 % in a benchmark that writes 1000 requests back to back. A stream that writes large
  requests at full speed and cares more about throughput than about that memory is the case where it
  would pay off.

With real gRPC, the full test suite (88 test cases) passed with the second round's headers in a Release
build at the start of the third round.

### Still not changed

- **Response buffers.** The read path still moves each response out of its read buffer and recycles a
  spare (round 1, change 2). Lending the read buffer to the handler without moving it would save two
  swaps per response, but a handler may poll, close or reopen the stream, and the message it holds
  would then alias a buffer that gRPC reads into. The few nanoseconds are not worth that hazard next to
  the microseconds gRPC spends parsing.
- **`WriteLast`.** Writing the last queued request with `WriteOptions::set_last_message()` would merge
  it with the half-close and save one operation per call; that is measurable only for short calls.
- **Write batching** (`set_buffer_hint()`) and the other items listed in [Not changed](#not-changed)
  still apply.

## Third round: polling without stalls

### Method

This round was done on Linux: AMD Ryzen 9 9950X3D (16 cores, SMT), Ubuntu with kernel 7.0, GCC 15.2,
glibc 2.43, and gRPC 1.72.0 with protobuf 5.27.0 from Conan, in the `conan-release` configuration
(`-O3`). It also takes the gRPC measurements that the second round could not.

**Profiling.** `perf` is not available to unprivileged users on that machine
(`kernel.perf_event_paranoid = 4`), so a small sampling profiler was preloaded into the benchmark
executable (`LD_PRELOAD`). It arms `ITIMER_PROF`, which sends `SIGPROF` every 250 µs of process CPU
time to the thread that consumed it, records the thread and its call stack with `backtrace()` in the
signal handler, and writes the samples and the memory map at exit; `llvm-symbolizer` resolves the
addresses, including the frames inlined into the RelWithDebInfo build. As in the first round, the
profiles covered the `[read]`, `[write]`, `[throughput]` and `[latency]` benchmarks and the `[load]`
scenario at 1.5 million requests without server delay. Samples of Catch2's own timing loops (clock
resolution estimates, the warm-up) were excluded.

**Timing.** Each variant was built from the same benchmark source and run in interleaved rounds
(r1, r2, r3lib, r3, then in reverse order, and so on), pinned to the eight cores of one CCD (`taskset -c 8-15`); the
tables give the median over the rounds of each benchmark's mean. Run-to-run noise on this machine is
about ±5 %, and up to ±10 % for the first benchmark of a run. Variants:

| Label | Library headers | Heap tracker |
|---|---|---|
| r1 | first round (`f06bf20`) | before this round |
| r2 | second round (`091cc68`) | before this round |
| r3lib | this round | before this round |
| r3 | this round | this round |

### Findings

The library's own frames are again a fraction of a percent of the samples: almost all the CPU time is
in gRPC and protobuf. The largest costs found are not CPU time at all:

1. **Every `pollFor()` that handled an event ended with a sleep of about 50 µs.** `[latency]` measured
   57 µs for a 64-byte round trip, but a client that handles one event per `pollFor()` call needs about
   6 µs. The difference was spent at the end of every `pollFor()`: after handling the response,
   `processEvents()` asked the completion queue for further events with an expired deadline, found
   none, and gRPC only reported that after about 50 µs. Where no further event came, as after the
   response of a request–response exchange, that was pure delay: the caller could not send its next
   request in the meantime.
2. **Why an expired deadline waits.** In gRPC's default Linux polling engine (epoll1) only one thread of
   the process at a time, the *designated poller*, waits in `epoll_wait`. Any other thread that calls
   `AsyncNext` becomes a worker that waits on its own condition variable until it is kicked or its
   deadline passes (`begin_worker()` in `ev_epoll1_linux.cc`). For an expired deadline, `gpr_cv_wait()`
   computes a timeout of zero and calls Abseil's `CondVar::WaitWithTimeout()`, which still makes a
   `futex` system call, with a zero relative timeout. Linux extends every timer of a thread by its
   *timer slack*, 50 µs by default (`/proc/self/timerslack_ns`), so the call returns about 50 µs later
   unless an event arrives first. The evidence:
   - `poll()` of an idle stream took 50.0 µs per call, whatever the stream type.
   - With the client thread's timer slack set to 1 ns (`prctl(PR_SET_TIMERSLACK, 1)`), it took 10.9 µs:
     the rest is the context switch and the core waking up.
   - Over TCP to a server in another process, `poll()` of an idle stream took 0.6 µs while the client
     thread was the only thread polling (it is then the designated poller and calls `epoll_wait` with a
     zero timeout), and 50 µs as soon as a second thread polled another stream. A 64-byte ping-pong then
     took 79 µs with the draining `pollFor()` and 26 µs with one event per call.
   - In the benchmarks and tests, the threads of the in-process synchronous server poll completion
     queues of the same process, so the client thread is never the designated poller.

   Any process with more than one thread polling gRPC completion queues is affected: two streams with a
   dedicated thread each (the pattern this library recommends), or a client next to a gRPC server.
3. **An idle `AsyncWriteStream` paid the same 50 µs per `poll()`**, although with no operation
   outstanding no event can arrive.
4. **The benchmark's heap tracker contended between threads.** In the CPU-bound load profile, the
   tracker's atomic updates took about 10 % of all samples: its three counters shared one cache line,
   updated by every allocation of the benchmark thread and of the gRPC threads alike. Without heap
   tracking, a `sendRequest()` of the producer loop cost 87 ns instead of 140 ns in that scenario
   (300 000 requests, no server delay).

Two things looked similar but are outside the library:

- A `poll()` of a stream with a read outstanding (every `AsyncReadStream` and `AsyncReadWriteStream`)
  has to ask the completion queue, so with nothing to deliver it still takes about 50 µs in such a
  process. That is the cost of a non-blocking check in gRPC, not of the library.
- A client-streaming call with one request took 66 µs. With the timer slack of the in-process server's
  threads set to 1 ns it took 27 µs, while lowering the client's changed nothing: the synchronous test
  server waits on its own completion queues the same way.

### Changes

**`pollFor()` processes one event per call by default.** `max_events` now defaults to 1 for `pollFor()`
of all three streams (it stays 64 for `poll()`). The first `AsyncNext` of a call waits for an event with
the caller's deadline, which is what `pollFor()` is for; no further `AsyncNext` follows, so no call ends
with a check that finds nothing. The loop around `pollFor()` takes the following events, and a further
call returns at once while one is already queued. Since every stream has its own completion queue with at
most one read and one write outstanding, no more than a few events can be waiting at the same time, and
nothing is lost by taking them one per call. `pollFor()` with `max_events > 1` keeps the previous
behaviour for callers that want it.

**No `AsyncNext` without an outstanding operation.** `processEvents()` stops, and `poll()` returns at
once, when no start-call, read, write, half-close or `Finish` is outstanding, since no event can arrive.
A `pollFor()` still waits until its deadline then, so that a loop around it keeps its pace instead of
spinning.

**Heap tracker.** Each counter of `MemoryTracker.cpp` has a cache line of its own (128-byte alignment,
which also covers the adjacent-line prefetch of x86 and the 128-byte lines of Apple silicon), and the
allocation count is kept per thread and added to the shared counter in batches of 64. The live and peak
bytes stay exact; the allocation count, which no benchmark reports, lags by less than 64 per thread.

**Tests and benchmarks.** `[lifecycle]` checks that `pollFor()` processes one of two available events per
call, and `[write-stream]` that 1000 `poll()` calls of an idle write stream next to the in-process server
take well under 25 ms (they took 50 ms before). Both tests fail with the previous headers. The new
`[poll]` benchmark measures `poll()` with nothing to process.

### Results

**Latency and polling** (medians of 5 interleaved runs of 30 samples):

| Benchmark | r1 | r2 | r3lib | r3 |
|---|---:|---:|---:|---:|
| ping-pong round trip, 64 B | 57.1 µs | 56.5 µs | 6.8 µs | **6.4 µs** |
| ping-pong round trip, 16 KiB | 62.1 µs | 61.4 µs | 8.4 µs | **8.8 µs** |
| `poll()` of an idle write stream | 50.6 µs | 50.5 µs | 2.5 ns | **2.5 ns** |
| `poll()` of an idle bidirectional stream (read outstanding) | 49.1 µs | 50.0 µs | 49.5 µs | 50.0 µs |

The round trip is about nine times faster: the client now returns from `pollFor()` as soon as it has
handled the response, instead of first waiting about 50 µs for a further event that never comes. The
idle bidirectional stream shows what remains: a `poll()` with a read outstanding has to ask gRPC, which
takes the timer slack in a process with other polling threads.

**Throughput** (medians of 5 interleaved runs of 30 samples; one call per batch for `[read]` and
`[write]`):

| Benchmark | r1 | r2 | r3lib | r3 |
|---|---:|---:|---:|---:|
| 1000 pipelined echoes × 64 B | 3.30 ms | 3.29 ms | 3.53 ms | 3.34 ms |
| 1000 pipelined echoes × 1 KiB | 3.24 ms | 3.24 ms | 3.63 ms | 3.11 ms |
| 1000 pipelined echoes × 16 KiB | 3.53 ms | 3.60 ms | 3.44 ms | 3.59 ms |
| `[read]` 1000 responses × 64 B | 2.48 ms | 2.50 ms | 2.58 ms | 2.43 ms |
| `[read]` 1000 responses × 1 KiB | 2.41 ms | 2.43 ms | 2.35 ms | 2.33 ms |
| `[read]` 1000 responses × 16 KiB | 3.13 ms | 2.99 ms | 2.96 ms | 2.97 ms |
| `[write]` 1000 requests × 64 B | 1.99 ms | 2.08 ms | 2.02 ms | 1.96 ms |
| `[write]` 1000 requests × 1 KiB | 1.91 ms | 2.04 ms | 2.01 ms | 2.00 ms |
| `[write]` 1000 requests × 16 KiB | 2.47 ms | 2.62 ms | 2.57 ms | 2.57 ms |

Streaming throughput did not change beyond the noise: while events keep coming, the final check of the
old `pollFor()` was mostly cut short by the next event, and handling one event per call costs one more
`AsyncNext` deadline computation per event. A focused run (8 interleaved runs of r2, r3lib and r3) put the cost of
taking one event per call at about 4 % for 64-byte and 1 KiB echoes, where an event arrives every
1.5 µs and each needs one more call with its deadline computation; 16 KiB echoes were not slower. The
first campaign's 7 to 12 % for r3lib was noise on top of that.

**Queue operations** (`[queue]`, medians of 5 interleaved runs of 100 samples): 13.0 / 2.9 / 34.1 ns for
enqueue / drop newest / evict + enqueue with r3, the same as r2: this round did not touch the queue.

**Heavy load** (`[load]`, default parameters: 50 000 × 1 KiB against a server taking one request per
≥ 20 µs; medians of 3 runs): written and dropped counts, peak queue, peak heap (1.10 MiB default queue,
0.28 MiB for the 256-message queues, 53.65 MiB unbounded), drain times and latencies are identical across
r1, r2, r3lib and r3 within a few tenths of a percent. The producer calls `poll()`, which still drains,
and the slow server sets the pace, so neither the polling changes nor the tracker show there.

**CPU-bound load** (300 000 × 1 KiB, no server delay; medians of 4 runs), mean `sendRequest()` cost:

| Write queue | r1 | r2 | r3lib | r3 |
|---|---:|---:|---:|---:|
| default (1024 msgs / 64 MiB, drop oldest) | 141.5 ns | 132.0 ns | 136.5 ns | **126.0 ns** |
| 256 msgs, drop oldest | 141.0 ns | 133.0 ns | 131.5 ns | **127.0 ns** |
| 256 msgs, drop newest | 137.5 ns | 130.5 ns | 130.0 ns | **124.0 ns** |
| 256 KiB, drop oldest | 140.5 ns | 133.0 ns | 132.5 ns | **127.5 ns** |
| unbounded | 348.5 ns | 336.0 ns | 337.0 ns | **330.0 ns** |

The second round's write path saves 6 to 9 ns per send here, and the tracker change (r3lib → r3) about
5 ns more; the rest of the gap to an untracked build (about 87 ns) is the exact live and peak counters.

**Memory** is unchanged by this round: the heap figures of every `[load]` configuration are the same for
r2 and r3, and the stream objects have the same size.

**Correctness.** All 90 tests pass with GCC 15.2 in Release, RelWithDebInfo, Debug and RelWithSanitizers
(ASan, UBSan and LSan, assertions enabled), and every benchmark runs cleanly in the sanitizer build. The
changed sources also compile with Clang 21 and the project's warning flags (`-Werror`). MSVC and Apple
Clang are covered by CI.

### Not changed

- **`poll()` still drains.** A loop that calls `poll()` once per frame relies on it processing everything
  that has arrived, so its final check, and its cost in a process with other polling threads, remain.
  [README.md](../README.md#polling) explains the trade-off and the timer-slack setting.
- **Lowering the timer slack inside the library.** Setting the polling thread's slack to 1 ns around the
  non-blocking `AsyncNext` calls (`prctl(PR_SET_TIMERSLACK)`) cut the empty check from 50 µs to about
  11 µs here. It was not done: it changes a thread attribute behind the caller's back, costs two system
  calls per poll, and is Linux-specific; an application that wants it can set it for its polling thread.
- **Stopping a drain after an event that was waited for.** Timing each `AsyncNext` and ending a `poll()`
  after one that took longer than a few microseconds would keep it from waiting for event after event
  while they trickle in, but the threshold is arbitrary: deserializing a large message takes longer than
  a short wait.
- **`WriteLast`.** Merging the last queued request with the half-close would save one operation per
  call; the one-request client-streaming call above spends most of its 66 µs in the test server, so the
  gain could not be shown with this test setup.
- **The synchronous test server** lingers in gRPC's completion queues in the same way. That affects how
  fast calls end in the benchmarks, not the library.

## Reproducing

```powershell
cmake --preset conan-release
cmake --build build-conan-release
.\build-conan-release\benchmarks\GrpcAsyncStreamBenchmarks.exe "[benchmark]" --benchmark-samples 30
.\build-conan-release\benchmarks\GrpcAsyncStreamBenchmarks.exe "[queue]" --benchmark-samples 100 --benchmark-warmup-time 300
```

For a CPU-bound profile of the load scenario:

```powershell
$env:GRPC_ASYNC_STREAM_BENCH_MESSAGES = 1500000
$env:GRPC_ASYNC_STREAM_BENCH_SERVER_DELAY_US = 0
.\build-conan-relwithdebinfo\benchmarks\GrpcAsyncStreamBenchmarks.exe "[load]"
```

On Linux (third round), pinned to the cores of one CCD:

```bash
cmake --workflow --preset conan-release
B=./build-conan-release/benchmarks/GrpcAsyncStreamBenchmarks
taskset -c 8-15 $B "[latency],[poll]" --benchmark-samples 30
taskset -c 8-15 $B "[throughput],[read],[write]" --benchmark-samples 30
taskset -c 8-15 $B "[queue]" --benchmark-samples 100 --benchmark-warmup-time 300
taskset -c 8-15 $B "[load]"
cat /proc/self/timerslack_ns   # the timer slack of a thread, 50000 ns by default
```
