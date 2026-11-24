#include "MemoryTracker.h"

#include <atomic>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <new>

// ---------------------------------------------------------------------------
// Platform detection
// ---------------------------------------------------------------------------

#if defined(__APPLE__)
#include <mach/mach.h>
#include <malloc/malloc.h>
#define GRPC_ASYNC_STREAM_HAS_RSS 1
#elif defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <malloc.h>
#include <windows.h>
// psapi.h must follow windows.h
#include <psapi.h>
#define GRPC_ASYNC_STREAM_HAS_RSS 1
#elif defined(__linux__) || defined(__ANDROID__)
#include <malloc.h>
#include <unistd.h>
#define GRPC_ASYNC_STREAM_HAS_RSS 1
#else
#define GRPC_ASYNC_STREAM_HAS_RSS 0
#endif

// Sanitizers replace the allocator: never instrument operator new on top of them,
// even if the build system did not disable tracking explicitly.
#if defined(__SANITIZE_ADDRESS__)
#define GRPC_ASYNC_STREAM_SANITIZED 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(memory_sanitizer) || __has_feature(thread_sanitizer)
#define GRPC_ASYNC_STREAM_SANITIZED 1
#endif
#endif
#ifndef GRPC_ASYNC_STREAM_SANITIZED
#define GRPC_ASYNC_STREAM_SANITIZED 0
#endif

#ifndef GRPC_ASYNC_STREAM_HEAP_TRACKING
#define GRPC_ASYNC_STREAM_HEAP_TRACKING 1
#endif

#if GRPC_ASYNC_STREAM_HEAP_TRACKING && !GRPC_ASYNC_STREAM_SANITIZED
#define GRPC_ASYNC_STREAM_TRACK_HEAP 1
#else
#define GRPC_ASYNC_STREAM_TRACK_HEAP 0
#endif

// ---------------------------------------------------------------------------
// Heap tracking
// ---------------------------------------------------------------------------

#if GRPC_ASYNC_STREAM_TRACK_HEAP

namespace
{

constinit std::atomic<std::int64_t>  g_live_bytes{0};
constinit std::atomic<std::int64_t>  g_peak_bytes{0};
constinit std::atomic<std::uint64_t> g_allocations{0};

// Each block is prefixed with a header holding its requested size, so that
// deallocation does not have to ask the allocator for it: the usable-size APIs
// are expensive on some platforms (_msize takes the heap lock on Windows) and
// would dominate profiles of allocation-heavy code. The header keeps the
// alignment guaranteed by malloc for the returned pointer.
constexpr std::size_t header_size = alignof(std::max_align_t) < sizeof(std::size_t) ? sizeof(std::size_t)
                                                                                    : alignof(std::max_align_t);

void onAllocated(std::size_t size) noexcept
{
    const auto bytes = static_cast<std::int64_t>(size);
    const auto live  = g_live_bytes.fetch_add(bytes, std::memory_order_relaxed) + bytes;
    g_allocations.fetch_add(1, std::memory_order_relaxed);

    std::int64_t peak = g_peak_bytes.load(std::memory_order_relaxed);
    while (live > peak && !g_peak_bytes.compare_exchange_weak(peak, live, std::memory_order_relaxed))
    {
    }
}

void* allocateNoThrow(std::size_t size) noexcept
{
    if (size > std::numeric_limits<std::size_t>::max() - header_size) [[unlikely]]
        return nullptr;
    void* block = std::malloc(header_size + size);
    if (!block) [[unlikely]]
        return nullptr;
    *static_cast<std::size_t*>(block) = size;
    onAllocated(size);
    return static_cast<std::byte*>(block) + header_size;
}

void* allocate(std::size_t size)
{
    void* ptr = allocateNoThrow(size);
    if (!ptr) [[unlikely]]
        throw std::bad_alloc();
    return ptr;
}

void deallocate(void* ptr) noexcept
{
    if (!ptr)
        return;
    void* block = static_cast<std::byte*>(ptr) - header_size;
    g_live_bytes.fetch_sub(static_cast<std::int64_t>(*static_cast<std::size_t*>(block)), std::memory_order_relaxed);
    std::free(block);
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

#endif // GRPC_ASYNC_STREAM_TRACK_HEAP

namespace MemoryTracker
{

bool isHeapTrackingAvailable() noexcept { return GRPC_ASYNC_STREAM_TRACK_HEAP != 0; }

std::optional<HeapCounters> heap() noexcept
{
#if GRPC_ASYNC_STREAM_TRACK_HEAP
    return HeapCounters{
        .live_bytes  = g_live_bytes.load(std::memory_order_relaxed),
        .peak_bytes  = g_peak_bytes.load(std::memory_order_relaxed),
        .allocations = g_allocations.load(std::memory_order_relaxed),
    };
#else
    return std::nullopt;
#endif
}

void resetPeak() noexcept
{
#if GRPC_ASYNC_STREAM_TRACK_HEAP
    g_peak_bytes.store(g_live_bytes.load(std::memory_order_relaxed), std::memory_order_relaxed);
#endif
}

std::optional<std::size_t> residentBytes() noexcept
{
#if defined(__APPLE__)
    mach_task_basic_info_data_t info{};
    mach_msg_type_number_t      count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS)
        return std::nullopt;
    return static_cast<std::size_t>(info.resident_size);
#elif defined(_WIN32)
    PROCESS_MEMORY_COUNTERS counters{};
    if (!GetProcessMemoryInfo(GetCurrentProcess(), &counters, sizeof(counters)))
        return std::nullopt;
    return static_cast<std::size_t>(counters.WorkingSetSize);
#elif GRPC_ASYNC_STREAM_HAS_RSS
    std::FILE* file = std::fopen("/proc/self/statm", "r");
    if (!file)
        return std::nullopt;
    unsigned long total_pages    = 0;
    unsigned long resident_pages = 0;
    const int     fields         = std::fscanf(file, "%lu %lu", &total_pages, &resident_pages);
    std::fclose(file);
    if (fields != 2)
        return std::nullopt;
    return static_cast<std::size_t>(resident_pages) * static_cast<std::size_t>(sysconf(_SC_PAGESIZE));
#else
    return std::nullopt;
#endif
}

void releaseFreeMemory() noexcept
{
#if defined(__APPLE__)
    malloc_zone_pressure_relief(nullptr, 0);
#elif defined(_WIN32)
    _heapmin();
#elif defined(__GLIBC__)
    malloc_trim(0);
#endif
}

#if GRPC_ASYNC_STREAM_TRACK_HEAP
#define GRPC_ASYNC_STREAM_HEAP_TEXT "C++ heap tracked"
#elif GRPC_ASYNC_STREAM_SANITIZED
#define GRPC_ASYNC_STREAM_HEAP_TEXT "C++ heap not tracked (sanitizer build)"
#else
#define GRPC_ASYNC_STREAM_HEAP_TEXT "C++ heap not tracked (GRPC_ASYNC_STREAM_HEAP_TRACKING=OFF)"
#endif

#if GRPC_ASYNC_STREAM_HAS_RSS
#define GRPC_ASYNC_STREAM_RSS_TEXT ", RSS measured"
#else
#define GRPC_ASYNC_STREAM_RSS_TEXT ", RSS not available on this platform"
#endif

const char* describeAvailability() noexcept { return GRPC_ASYNC_STREAM_HEAP_TEXT GRPC_ASYNC_STREAM_RSS_TEXT; }

} // namespace MemoryTracker
