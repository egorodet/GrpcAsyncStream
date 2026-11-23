#pragma once

// Process memory instrumentation for the benchmarks.
//
// Linking MemoryTracker.cpp into an executable replaces the global
// operator new / delete with versions that keep exact counters of the C++
// heap memory currently allocated (live bytes) and its high-water mark.
// Memory allocated directly with malloc (e.g. by the gRPC C core) is not
// included; it is covered by the resident set size (RSS) of the process.

#include <cstddef>
#include <cstdint>

namespace MemoryTracker
{

struct HeapCounters
{
    std::int64_t  live_bytes  = 0; // C++ heap bytes currently allocated
    std::int64_t  peak_bytes  = 0; // high-water mark of live_bytes since the last resetPeak()
    std::uint64_t allocations = 0; // number of allocations since program start
};

[[nodiscard]] HeapCounters heap();

// Restarts the peak tracking from the current live bytes
void resetPeak();

// Current resident set size of the process in bytes (0 if unavailable)
[[nodiscard]] std::size_t residentBytes();

// Returns free heap pages to the operating system where the allocator supports
// it, so that a following RSS baseline is not inflated by earlier peaks.
void releaseFreeMemory();

} // namespace MemoryTracker
