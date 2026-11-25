# Performance optimizations

How the streams were profiled under heavy load, what was found, changed and measurably gained, and what was
deliberately left alone. Three rounds:

1. [First round](#method): ring-buffer write queue, response buffer reuse, monotonic deadlines, cheaper heap tracking.
2. [Second round](#second-round-no-copies-on-the-write-path): no copies or moves of requests on the write path.
3. [Third round](#third-round-polling-without-stalls): no ~50 µs stall at the end of `pollFor()` and in an idle
   write stream's `poll()` — round trips about nine times faster; also measures round two with real gRPC.

## Method

**Benchmarks.** `GrpcAsyncStreamBenchmarks` ([Benchmarks](../README.md#benchmarks)), Release build, MSVC 14.52,
gRPC 1.72.0 from Conan, Windows 11 x64, before and after every change. Two tags were added:

| Tag | Class | Workload |
|---|---|---|
| `[read]` | `AsyncReadStream` | Server-streaming call delivering 1000 responses of 64 B, 1 KiB and 16 KiB |
| `[write]` | `AsyncWriteStream` | Client-streaming call sending 1000 requests of 64 B, 1 KiB and 16 KiB, then the half-close and the final status |

**Profiling.** A small external sampling profiler (ETW needs elevation): about once per millisecond it
suspends every thread whose cycle counter advanced, walks its stack with `StackWalk64` and resumes it; kernel
waits are discarded, symbols come from the RelWithDebInfo PDBs. Profiled: `[throughput]` and `[load]` at
1.5 million requests without server delay (`GRPC_ASYNC_STREAM_BENCH_MESSAGES=1500000
GRPC_ASYNC_STREAM_BENCH_SERVER_DELAY_US=0`), so the run is CPU-bound.

**Noise.** Whole runs drift by up to ~25 %, so changes were judged by interleaved A/B runs and medians.

## Findings

About 90 % of the samples are on the client thread (the in-process transport runs much server work inline),
almost all in gRPC (`grpc_call_start_batch`, `ExecCtx::Flush`, polling) and protobuf. The library's frames are
a few percent. Within that:

1. **The benchmark's heap tracker was the top hotspot**: `RtlSizeHeap` 12 % exclusive, 19 % inclusive. The
   tracker called `_msize` on every allocation, which takes the heap lock on Windows.
2. **The write queue allocated per queued request**: MSVC's `std::deque` stores one element > 8 bytes per block.
3. **Every response was parsed into a fresh message**: moving it out of the read buffer left gRPC's parser
   allocating every `string`, `bytes` and repeated field anew (`ArenaStringPtr::Set`, `malloc` / `free`).
4. **`pollFor()` passed a `system_clock` deadline**, converted by gRPC on every `AsyncNext`
   (`gpr_convert_clock_type`) and affected by wall clock changes.

## Changes

### 1. Ring buffer for the write queue

`AsyncWriteStream.h`: `std::deque` replaced by `detail::RingQueue<T>`, a power-of-two ring (from 16, doubling)
that allocates nothing in steady state on every standard library. The memory policy is unchanged: storage is
released after a peak of more than 256 requests, and on `close()` / end of call. Two tests in
`tests/WriteQueueTests.cpp` check FIFO order across wrap-around and growth.

### 2. Reuse of the response buffer on reads

`AsyncReadStream.h`: a second message, `m_spare_response`. On each read completion the message is moved out to
a local, the spare becomes the read buffer for the next `Read`, the handler gets the local, and afterwards
(unless the handler closed the stream) the local is `Clear()`ed and becomes the spare. `Clear()` keeps field
capacity, so gRPC parses into allocated storage. The handler contract is unchanged (it owns a local that never
aliases a read buffer). Cost: up to one extra message's capacity per open call; `close()` now frees both buffers.

### 3. Monotonic deadline in `pollFor()`

`AsyncCallBase.h`: `deadlineAfter()` returns a `gpr_timespec` on `GPR_CLOCK_MONOTONIC`: no conversion per call,
immune to wall clock changes. Timeouts round up to microseconds; ≤ 0 means non-blocking, ≥ 1 year means
unbounded.

### 4. Freeing the written request

*Superseded by the [second round](#second-round-no-copies-on-the-write-path).* The "free its memory" step used
`Clear()`, which kept the capacity; it now assigned an empty message.

### 5. Heap tracking without allocator queries

`benchmarks/MemoryTracker.{h,cpp}`: `operator new` stores the requested size in a header of
`max(alignof(std::max_align_t), sizeof(std::size_t))` bytes in front of each block, so `delete` needs no
allocator query and tracking works on every platform. Figures now count **requested** bytes (slightly lower
than the former usable sizes).

## Results

Release, MSVC, same machine; "Before" uses the fixed heap tracker too.

**Write queue** (median of 4 runs × 100 samples each):

| `[queue]` benchmark | Before | After |
|---|---:|---:|
| `sendRequest()` enqueue | ~78 ns | ~66 ns |
| `sendRequest()` evict oldest + enqueue, queue full | ~85 ns | ~65 ns |
| `sendRequest()` drop newest, queue full | ~33 ns | ~37 ns |

Drop newest does not touch the storage; its difference is noise.

**Reads** (`[read]`, interleaved A/B pairs, 60 samples each):

| Payload | Change across three pairs |
|---|---|
| 64 B | within noise |
| 1 KiB | 3–17 % faster |
| 16 KiB | 5–25 % faster |

**Memory** (`[load]`, default limits): peak heap growth 1.19 → 1.11 MiB without the deque's blocks and map.

**End-to-end throughput** (`[throughput]`, `[write]`) changed little: each message costs 3–5 µs in gRPC and
protobuf, dwarfing the nanoseconds saved.

**Correctness.** All 85 tests pass in Release and ASan builds; benchmarks run cleanly under ASan
(`detect_container_overflow=0`). Verified with MSVC; GCC and Clang via CI.

## Not changed

- **`ByteSizeLong()` in `sendRequest()`**: needed by the byte limit and statistics; not visible in profiles.
- **Write batching** (`WriteOptions::set_buffer_hint()` on all but the last queued write): the most promising
  throughput gain, but the in-process channel does not batch, so it needs a TCP benchmark first.
- **Completion queue polling**: one `AsyncNext` per event is the API's minimum.
- **Unbounded `[load]` send cost** (~440 ns vs ~190 ns bounded) comes from the benchmark building requests and
  growing the process by ~50 MiB, not from the library.

## Second round: no copies on the write path

### Findings

1. **The stream kept its own copy of the request being written** (`m_current_write`), although `Write()`
   serializes the message and "doesn't take ownership or a reference to msg"
   (`AsyncWriterInterface::Write`). In-flight requests were held twice.
2. **`sendRequest(RequestProto)` took the request by value**: a full copy per call for lvalues, even when
   written at once, dropped or not writable.
3. **A queued request was moved six times** (two when written at once); each protobuf move is a construction,
   a field swap and a destruction.
4. **The ring buffer constructed empty messages** in every new slot on growth (`std::vector<T>` storage) and
   kept moved-from ones in popped slots.

### Changes

All in `AsyncWriteStream.h`.

**Write without a member copy.** `m_current_write` is gone: `startWrite(const&)` passes the request straight to
`Write()`; a queued request is written from its slot, destroyed right after. In flight, a request exists only
serialized inside gRPC.

**Two overloads of `sendRequest()`**, `const RequestProto&` and `RequestProto&&`:

| Outcome | lvalue, before | lvalue, after | rvalue, before | rvalue, after |
|---|---|---|---|---|
| `Written` | 1 copy, 2 moves | nothing | 2–3 moves | nothing |
| `Queued`, then written | 1 copy, 6 moves | 1 copy (into the slot) | 6–7 moves | 1 move (into the slot) |
| `Dropped` | 1 copy, 1 move | nothing | 1–2 moves | nothing |
| `NotWritable` | 1 copy | nothing | 0–1 move | nothing |

Counted with an instrumented message type; rvalue ranges depend on temporary vs `std::move`d argument. Every
existing call compiles unchanged (braced initializers pick the rvalue overload); only taking the address of
`sendRequest` needs an explicit overload.

**`RingQueue` on raw storage** from `std::allocator<T>`: `emplaceBack()` constructs in the slot (one copy or
move), `dropFront()` / `dropBack()` destroy in place, growth moves only live elements with
`std::move_if_noexcept`, and a throwing copy leaves the queue intact. Free slots hold no objects.

### Results

Without gRPC in that environment, the library overhead was measured A/B against the first round: real headers,
GCC 13.3 `-O2`, Linux x86-64, a stand-in gRPC whose `Write()` copies the payload into a reused buffer and
completes at once, a protobuf-like message (`uint64` id + `std::string` payload). Medians of 15 repetitions of
20 000 requests, two interleaved runs per version; noise up to ~25 %.

| Scenario, ns per request | 64 B | 1 KiB | 16 KiB |
|---|---|---|---|
| Written at once, lvalue | 37 → 11 | 54 → 21 | 311 → 127 |
| Written at once, rvalue (destroyed by the caller) | 34 → 19 | within noise | within noise |
| 20 000 rvalues queued, then written | 66–73 → 31–55 | 100–140 → 66–88 | within noise |
| Steady queue of 64: enqueue one, write one | 40–47 → 24–31 | 82–144 → 82–101 | 1540 → 1460 |
| Evict oldest + enqueue, queue of 1024 full | 20–28 → 13–16 | 39–57 → 28–29 | within noise |
| Dropped (`DropNewest`, full), lvalue | 16–20 → 1 | 27 → 1 | 113 → 1 |

Allocations per request fell from 1 to 0 for lvalues written at once or dropped. From 1 KiB up, rvalue paths
are dominated by the payload's own allocation.

**Memory.** In flight, the stream no longer holds the request; a request is freed as soon as it leaves the
queue; the stream object is one message smaller (352 → 304 bytes); reused lvalues no longer allocate per send.

**Correctness.** Built with the project's warnings (`-Werror`) by GCC 13.3 and Clang 18 against the stand-in;
scripted scenarios ran through `FakeCall` under ASan and UBSan without leaks. Three tests were added to
`tests/WriteQueueTests.cpp` (lvalue copied only when queued, elements destroyed on leaving the ring, a failing
copy during growth leaves the queue intact).

### Measured with real gRPC

Taken in the [third round](#third-round-polling-without-stalls) (Linux, GCC 15.2, gRPC 1.72.0): r1 = first
round's headers, r2 = second round's, same benchmark source and tracker, medians of 5 interleaved runs.

| Benchmark | r1 | r2 | Change |
|---|---:|---:|---|
| `sendRequest()` enqueue | 23.9 ns | 13.0 ns | −46 % |
| `sendRequest()` evict oldest + enqueue, queue full | 43.8 ns | 33.6 ns | −23 % |
| `sendRequest()` drop newest, queue full | 33.8 ns | 2.8 ns | −92 % |
| `[throughput]` 1000 echoes × 16 KiB | 3.53 ms | 3.60 ms | within noise |
| `[write]` 1000 requests × 64 B / 1 KiB / 16 KiB | 1.99 / 1.91 / 2.47 ms | 2.08 / 2.04 / 2.62 ms | +4 / +7 / +6 % |
| `[load]` 16 KiB payloads, default queue: peak heap growth | 16.22 MiB | 16.20 MiB | −1 payload |
| `[load]` 16 KiB payloads, unbounded: peak heap growth | 307.46 MiB | 307.08 MiB | −0.4 MiB |

- **`[queue]` gained far more than predicted**: with real protobuf every saved move is costly, and drop newest
  no longer moves at all.
- **`[load]`**: one payload less per stream, as expected.
- **`[write]` is 4–7 % slower** because of allocation order: the queued request is freed after `Write()`
  serialized it, so the next serialization buffer is allocated before the old payload is freed and glibc
  cannot hand back the cached block. A diagnostic build freeing the slot only on write completion measured
  1.97 / 1.94 / 2.38 ms (r1: 1.94 / 1.95 / 2.45, r2: 2.03 / 2.04 / 2.51; 8 interleaved runs). Not adopted: it
  would hold a request per stream during every write again, for 2–5 % in a back-to-back benchmark.

All 88 tests passed with real gRPC in Release at the start of the third round.

### Still not changed

- **Response buffers**: lending the read buffer to the handler would save two swaps per response, but a handler
  that polls, closes or reopens the stream would then alias a buffer gRPC reads into.
- **`WriteLast`** (`set_last_message()` on the last queued request) would save one operation per call;
  measurable only for short calls.
- **Write batching** and the other [Not changed](#not-changed) items still apply.

## Third round: polling without stalls

### Method

Linux: AMD Ryzen 9 9950X3D (16 cores, SMT), Ubuntu, kernel 7.0, GCC 15.2, glibc 2.43, gRPC 1.72.0 and protobuf
5.27.0 from Conan, `conan-release` (`-O3`).

**Profiling.** `perf` is unavailable (`kernel.perf_event_paranoid = 4`), so a sampling profiler was preloaded
(`LD_PRELOAD`): `ITIMER_PROF` every 250 µs of CPU time, stacks with `backtrace()`, symbols with
`llvm-symbolizer` (inlined frames included). Profiled `[read]`, `[write]`, `[throughput]`, `[latency]` and
`[load]` at 1.5 million requests without delay, excluding Catch2's own timing loops.

**Timing.** Variants built from the same benchmark source, run in interleaved rounds (r1, r2, r3lib, r3, then
reversed, …) pinned to one CCD (`taskset -c 8-15`); tables give the median over rounds of each mean. Noise is
about ±5 % (±10 % for the first benchmark of a run).

| Label | Library headers | Heap tracker |
|---|---|---|
| r1 | first round (`f06bf20`) | before this round |
| r2 | second round (`091cc68`) | before this round |
| r3lib | this round | before this round |
| r3 | this round | this round |

### Findings

The library's frames are again a fraction of a percent. The largest costs are not CPU time:

1. **Every `pollFor()` that handled an event ended with a ~50 µs sleep.** A 64 B round trip took 57 µs instead
   of ~6 µs: after the response, `processEvents()` asked for further events with an expired deadline, and gRPC
   reported "none" only ~50 µs later, delaying the caller's next request.
2. **Why an expired deadline waits.** In gRPC's epoll1 engine only one *designated poller* thread waits in
   `epoll_wait`; other threads calling `AsyncNext` wait on a condition variable (`begin_worker()` in
   `ev_epoll1_linux.cc`). For an expired deadline Abseil's `CondVar::WaitWithTimeout()` still issues a `futex`
   call with zero timeout, which Linux extends by the thread's *timer slack*, 50 µs by default
   (`/proc/self/timerslack_ns`). Evidence:
   - `poll()` of an idle stream took 50.0 µs per call, whatever the stream type.
   - With `prctl(PR_SET_TIMERSLACK, 1)` on the client thread it took 10.9 µs (context switch and wake-up).
   - Over TCP to another process: 0.6 µs while the client was the only polling thread, 50 µs once a second
     thread polled another stream; a 64 B ping-pong took 79 µs draining vs 26 µs with one event per call.
   - In benchmarks and tests the in-process server's threads poll too, so the client is never the poller.

   Any process with more than one thread polling gRPC queues is affected: two streams with a thread each, or a
   client next to a server.
3. **An idle `AsyncWriteStream` paid the same 50 µs per `poll()`**, though no event can arrive.
4. **The benchmark's heap tracker contended between threads**: its three counters shared a cache line, ~10 % of
   CPU-bound samples. Without tracking, a producer `sendRequest()` cost 87 ns instead of 140 ns (300 000
   requests, no delay).

Outside the library:

- `poll()` of a stream with a read outstanding must ask the completion queue, so it still takes ~50 µs in such
  a process.
- A one-request client-streaming call took 66 µs, 27 µs with the in-process server threads' slack at 1 ns
  (the client's slack changed nothing): the synchronous test server waits the same way.

### Changes

**`pollFor()` processes one event per call by default** (`max_events = 1`; `poll()` keeps 64). The first
`AsyncNext` waits with the caller's deadline and no empty check follows; the loop takes further events, which
return at once. With at most one read and one write outstanding per stream, nothing is lost.
`max_events > 1` keeps the old behaviour.

**No `AsyncNext` without an outstanding operation**: `poll()` returns at once; `pollFor()` still waits until
its deadline so loops keep their pace.

**Heap tracker**: each counter on its own 128-byte-aligned cache line (x86 adjacent-line prefetch, Apple
silicon), allocation counts batched per thread by 64. Live and peak bytes stay exact.

**Tests and benchmarks**: `[lifecycle]` checks one of two available events per `pollFor()`; `[write-stream]`
checks 1000 idle `poll()`s next to the server take well under 25 ms (50 ms before); both fail with the old
headers. New `[poll]` benchmark.

### Results

**Latency and polling** (medians of 5 interleaved runs of 30 samples):

| Benchmark | r1 | r2 | r3lib | r3 |
|---|---:|---:|---:|---:|
| ping-pong round trip, 64 B | 57.1 µs | 56.5 µs | 6.8 µs | **6.4 µs** |
| ping-pong round trip, 16 KiB | 62.1 µs | 61.4 µs | 8.4 µs | **8.8 µs** |
| `poll()` of an idle write stream | 50.6 µs | 50.5 µs | 2.5 ns | **2.5 ns** |
| `poll()` of an idle bidirectional stream (read outstanding) | 49.1 µs | 50.0 µs | 49.5 µs | 50.0 µs |

Round trips are about nine times faster. The idle bidirectional stream shows what remains: a `poll()` with a
read outstanding has to ask gRPC.

**Throughput** (medians of 5 interleaved runs of 30 samples; one call per batch for `[read]` and `[write]`):

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

Streaming throughput is within noise: under a steady flow the old final check was cut short by the next event.
A focused run (8 interleaved runs) put the cost of one event per call at ~4 % for 64 B and 1 KiB echoes (an
event every 1.5 µs, one more deadline computation each); 16 KiB was not slower. The first campaign's 7–12 % for
r3lib was noise.

**Queue operations** (`[queue]`, medians of 5 runs of 100 samples): 13.0 / 2.9 / 34.1 ns for enqueue / drop
newest / evict + enqueue with r3, same as r2.

**Heavy load** (`[load]` defaults: 50 000 × 1 KiB, server ≥ 20 µs per request; medians of 3 runs): written and
dropped counts, peak queue, peak heap (1.10 MiB default, 0.28 MiB for 256-message queues, 53.65 MiB unbounded),
drain times and latencies identical across r1–r3 within tenths of a percent; the slow server sets the pace.

**CPU-bound load** (300 000 × 1 KiB, no server delay; medians of 4 runs), mean `sendRequest()` cost:

| Write queue | r1 | r2 | r3lib | r3 |
|---|---:|---:|---:|---:|
| default (1024 msgs / 64 MiB, drop oldest) | 141.5 ns | 132.0 ns | 136.5 ns | **126.0 ns** |
| 256 msgs, drop oldest | 141.0 ns | 133.0 ns | 131.5 ns | **127.0 ns** |
| 256 msgs, drop newest | 137.5 ns | 130.5 ns | 130.0 ns | **124.0 ns** |
| 256 KiB, drop oldest | 140.5 ns | 133.0 ns | 132.5 ns | **127.5 ns** |
| unbounded | 348.5 ns | 336.0 ns | 337.0 ns | **330.0 ns** |

The second round saves 6–9 ns per send, the tracker change ~5 ns more; the rest of the gap to an untracked
build (~87 ns) is the exact live and peak counters.

**Memory** is unchanged by this round (same `[load]` heap figures and object sizes for r2 and r3).

**Correctness.** All 90 tests pass with GCC 15.2 in Release, RelWithDebInfo, Debug and RelWithSanitizers (ASan,
UBSan, LSan); benchmarks run cleanly with sanitizers; changed sources compile with Clang 21 `-Werror`. MSVC and
Apple Clang via CI.

### Not changed

- **`poll()` still drains**: per-frame loops rely on it, so its final check remains;
  [README](../README.md#polling) explains the trade-off and the timer-slack setting.
- **Lowering the timer slack inside the library** (50 → ~11 µs here): it would change a thread attribute behind
  the caller's back, cost two system calls per poll and is Linux-only; applications can set it themselves.
- **Ending a drain after an event that was waited for**: the time threshold would be arbitrary.
- **`WriteLast`**: the one-request call spends most of its 66 µs in the test server, so no gain was measurable.
- **The synchronous test server** lingers in its completion queues the same way; that affects the benchmarks,
  not the library.

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
