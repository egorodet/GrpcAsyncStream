// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

#pragma once

// Process memory instrumentation for the benchmarks.
//
// Heap tracking: when enabled (GRPC_ASYNC_STREAM_HEAP_TRACKING=1, the default
// outside sanitizer builds), MemoryTracker.cpp replaces the global operator
// new / delete with versions that keep exact counters of the live C++ heap
// bytes (as requested, without allocator rounding) and their high-water mark.
// The size of each block is kept in a small header in front of it, so that
// tracking adds no allocator queries (such as _msize, which takes the heap lock
// on Windows) to the code being measured. Memory allocated directly with malloc (e.g. by the gRPC C
// core) is not included; it is covered by the resident set size (RSS).
//
// RSS: /proc/self/statm on Linux, task_info on macOS, GetProcessMemoryInfo on
// Windows. Elsewhere it is reported as unavailable.

#include <cstddef>
#include <cstdint>
#include <optional>

namespace MemoryTracker
{

struct HeapCounters
{
    std::int64_t  live_bytes  = 0; // C++ heap bytes currently allocated
    std::int64_t  peak_bytes  = 0; // high-water mark of live_bytes since the last resetPeak()
    std::uint64_t allocations = 0; // number of allocations since program start, counted per thread
                                   // in batches of 64: lags by less than 64 per thread
};

// True when operator new / delete are instrumented in this build
[[nodiscard]] bool isHeapTrackingAvailable() noexcept;

// Heap counters, or std::nullopt when heap tracking is not available
[[nodiscard]] std::optional<HeapCounters> heap() noexcept;

// Restarts the peak tracking from the current live bytes (no-op when unavailable)
void resetPeak() noexcept;

// Current resident set size of the process in bytes, or std::nullopt when unavailable
[[nodiscard]] std::optional<std::size_t> residentBytes() noexcept;

// Returns free heap pages to the operating system where the allocator supports
// it, so that a following RSS baseline is not inflated by earlier peaks.
void releaseFreeMemory() noexcept;

// Short description of the memory measurements available in this build
[[nodiscard]] const char* describeAvailability() noexcept;

} // namespace MemoryTracker
