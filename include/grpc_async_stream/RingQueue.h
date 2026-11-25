// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

#pragma once

// FIFO queue on a growable ring buffer.
// Requires C++20; depends only on the C++ standard library.

#include <concepts>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

namespace grpc_async_stream
{

namespace detail
{

// FIFO queue on raw ring storage with power-of-two capacity, doubled on growth.
// Unlike std::deque (MSVC allocates per element > 8 bytes), steady-state push/pop allocate nothing.
// Free slots hold no objects. Arguments of emplaceBack() / pushBack() must not refer to queue elements.
template<typename T>
    requires std::move_constructible<T> && std::is_nothrow_destructible_v<T>
class RingQueue
{
public:
    RingQueue() = default;
    ~RingQueue() { release(); }

    RingQueue(const RingQueue&)            = delete;
    RingQueue& operator=(const RingQueue&) = delete;
    RingQueue(RingQueue&&)                 = delete;
    RingQueue& operator=(RingQueue&&)      = delete;

    [[nodiscard]] bool        empty() const noexcept { return m_size == 0; }
    [[nodiscard]] std::size_t size() const noexcept { return m_size; }
    [[nodiscard]] std::size_t capacity() const noexcept { return m_capacity; }

    // Oldest and newest element; the queue must not be empty
    [[nodiscard]] T& front() noexcept { return m_slots[m_head]; }
    [[nodiscard]] T& back() noexcept { return m_slots[slot(m_size - 1)]; }

    // Constructs a new element at the back; on exception the elements are kept.
    template<typename... Args>
        requires std::constructible_from<T, Args...>
    T& emplaceBack(Args&&... args)
    {
        if (m_size == m_capacity)
            grow();
        T& element = *std::construct_at(m_slots + slot(m_size), std::forward<Args>(args)...);
        ++m_size;
        return element;
    }

    void pushBack(T&& value) { emplaceBack(std::move(value)); }

    // Remove and return the oldest / newest element
    T popFront()
    {
        T value = std::move(front());
        dropFront();
        return value;
    }

    T popBack()
    {
        T value = std::move(back());
        dropBack();
        return value;
    }

    // Destroy the oldest / newest element in place
    void dropFront() noexcept
    {
        std::destroy_at(m_slots + m_head);
        m_head = slot(1);
        --m_size;
    }

    void dropBack() noexcept
    {
        std::destroy_at(m_slots + slot(m_size - 1));
        --m_size;
    }

    // Destroys all elements and frees the storage.
    void release() noexcept
    {
        destroyAll();
        if (m_slots)
            std::allocator<T>().deallocate(m_slots, m_capacity);
        m_slots    = nullptr;
        m_capacity = 0;
        m_head     = 0;
    }

private:
    static constexpr std::size_t initial_capacity = 16;

    [[nodiscard]] std::size_t slot(std::size_t offset) const noexcept { return (m_head + offset) & (m_capacity - 1); }

    void destroyAll() noexcept
    {
        while (m_size > 0)
            dropFront();
    }

    void grow()
    {
        // Frees the new storage if moving an element throws; the old storage stays intact
        struct NewStorage
        {
            T*          slots       = nullptr;
            std::size_t capacity    = 0;
            std::size_t constructed = 0;

            ~NewStorage()
            {
                if (!slots)
                    return;
                std::destroy_n(slots, constructed);
                std::allocator<T>().deallocate(slots, capacity);
            }
        };

        const std::size_t new_capacity = m_capacity == 0 ? initial_capacity : m_capacity * 2;
        NewStorage        storage{.slots = std::allocator<T>().allocate(new_capacity), .capacity = new_capacity};
        for (; storage.constructed < m_size; ++storage.constructed)
            std::construct_at(storage.slots + storage.constructed,
                              std::move_if_noexcept(m_slots[slot(storage.constructed)]));
        T* const new_slots = std::exchange(storage.slots, nullptr);

        // Destroy the moved-from elements and free the old storage
        const std::size_t count = m_size;
        release();
        m_slots    = new_slots;
        m_capacity = new_capacity;
        m_size     = count;
    }

    T*          m_slots    = nullptr;
    std::size_t m_capacity = 0;
    std::size_t m_head     = 0;
    std::size_t m_size     = 0;
};

} // namespace detail

} // namespace grpc_async_stream
