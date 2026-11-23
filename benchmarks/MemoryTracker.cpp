#include "MemoryTracker.h"

#include <atomic>
#include <cstdlib>
#include <new>

#if defined(__APPLE__)
#include <mach/mach.h>
#include <malloc/malloc.h>
#define GRPC_ASYNC_STREAM_USABLE_SIZE(ptr) malloc_size(ptr)
#elif defined(_WIN32)
#define NOMINMAX
#include <malloc.h>
#include <windows.h>
#include <psapi.h>
#define GRPC_ASYNC_STREAM_USABLE_SIZE(ptr) _msize(ptr)
#else
#include <malloc.h>
#include <unistd.h>
#include <cstdio>
#define GRPC_ASYNC_STREAM_USABLE_SIZE(ptr) malloc_usable_size(ptr)
#endif

namespace
{

std::atomic<std::int64_t>  g_live_bytes{0};
std::atomic<std::int64_t>  g_peak_bytes{0};
std::atomic<std::uint64_t> g_allocations{0};

void onAllocated(void* ptr)
{
    const auto size = static_cast<std::int64_t>(GRPC_ASYNC_STREAM_USABLE_SIZE(ptr));
    const auto live = g_live_bytes.fetch_add(size, std::memory_order_relaxed) + size;
    g_allocations.fetch_add(1, std::memory_order_relaxed);

    std::int64_t peak = g_peak_bytes.load(std::memory_order_relaxed);
    while (live > peak && !g_peak_bytes.compare_exchange_weak(peak, live, std::memory_order_relaxed))
    {
    }
}

void* allocate(std::size_t size)
{
    void* ptr = std::malloc(size == 0 ? 1 : size);
    if (!ptr)
        throw std::bad_alloc();
    onAllocated(ptr);
    return ptr;
}

void* allocateNoThrow(std::size_t size) noexcept
{
    void* ptr = std::malloc(size == 0 ? 1 : size);
    if (ptr)
        onAllocated(ptr);
    return ptr;
}

void deallocate(void* ptr) noexcept
{
    if (!ptr)
        return;
    g_live_bytes.fetch_sub(static_cast<std::int64_t>(GRPC_ASYNC_STREAM_USABLE_SIZE(ptr)), std::memory_order_relaxed);
    std::free(ptr);
}

} // namespace

// Replacements of the global (non-aligned) allocation functions. The aligned
// overloads keep their default implementations, which use a separate allocator.
void* operator new(std::size_t size) { return allocate(size); }
void* operator new[](std::size_t size) { return allocate(size); }
void* operator new(std::size_t size, const std::nothrow_t&) noexcept { return allocateNoThrow(size); }
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept { return allocateNoThrow(size); }
void  operator delete(void* ptr) noexcept { deallocate(ptr); }
void  operator delete[](void* ptr) noexcept { deallocate(ptr); }
void  operator delete(void* ptr, std::size_t) noexcept { deallocate(ptr); }
void  operator delete[](void* ptr, std::size_t) noexcept { deallocate(ptr); }
void  operator delete(void* ptr, const std::nothrow_t&) noexcept { deallocate(ptr); }
void  operator delete[](void* ptr, const std::nothrow_t&) noexcept { deallocate(ptr); }

namespace MemoryTracker
{

HeapCounters heap()
{
    HeapCounters counters;
    counters.live_bytes  = g_live_bytes.load(std::memory_order_relaxed);
    counters.peak_bytes  = g_peak_bytes.load(std::memory_order_relaxed);
    counters.allocations = g_allocations.load(std::memory_order_relaxed);
    return counters;
}

void resetPeak() { g_peak_bytes.store(g_live_bytes.load(std::memory_order_relaxed), std::memory_order_relaxed); }

std::size_t residentBytes()
{
#if defined(__APPLE__)
    mach_task_basic_info_data_t info{};
    mach_msg_type_number_t      count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS)
        return 0;
    return static_cast<std::size_t>(info.resident_size);
#elif defined(_WIN32)
    PROCESS_MEMORY_COUNTERS counters{};
    if (!GetProcessMemoryInfo(GetCurrentProcess(), &counters, sizeof(counters)))
        return 0;
    return static_cast<std::size_t>(counters.WorkingSetSize);
#else
    std::FILE* file = std::fopen("/proc/self/statm", "r");
    if (!file)
        return 0;
    unsigned long total_pages    = 0;
    unsigned long resident_pages = 0;
    const int     fields         = std::fscanf(file, "%lu %lu", &total_pages, &resident_pages);
    std::fclose(file);
    if (fields != 2)
        return 0;
    return static_cast<std::size_t>(resident_pages) * static_cast<std::size_t>(sysconf(_SC_PAGESIZE));
#endif
}

void releaseFreeMemory()
{
#if defined(__APPLE__)
    malloc_zone_pressure_relief(nullptr, 0);
#elif defined(__GLIBC__)
    malloc_trim(0);
#endif
}

} // namespace MemoryTracker
