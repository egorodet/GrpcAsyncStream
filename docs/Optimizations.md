# Performance optimizations

This document records how the stream classes were profiled under heavy load, which bottlenecks were
found, what was changed, and what the changes measurably gained. It also lists what was deliberately
left alone and why. The [second round](#second-round-no-copies-on-the-write-path) removes the remaining
copies and moves of requests on the write path.

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

### To measure with real gRPC

The effect is largest where requests are sent as lvalues, are large, or are dropped:

- `[queue]`: evict + enqueue should gain a little; drop newest passes rvalues and is unaffected.
- `[throughput]` / `[write]` with 16 KiB payloads: one move fewer per request is small next to
  serialization; expect at most a few percent.
- `[load]` with `GRPC_ASYNC_STREAM_BENCH_PAYLOAD=16384`: the heap figure should drop by about one
  payload per stream, and drops stay allocation-free.

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
