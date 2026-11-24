# Performance optimizations

This document records how the stream classes were profiled under heavy load, which bottlenecks were
found, what was changed, and what the changes measurably gained. It also lists what was deliberately
left alone and why.

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
