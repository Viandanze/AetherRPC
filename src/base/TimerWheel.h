#ifndef AETHER_BASE_TIMERWHEEL_H
#define AETHER_BASE_TIMERWHEEL_H
#pragma once
#include <vector>
#include <memory>
#include <cstdint>
#include "base/noncopyable.h"
#include "base/TimerId.h"

class EventLoop;
class TcpConnection;

/// TimerWheel: O(1) idle-connection timeout management
///
/// Design:
/// - Single-level wheel, one tick per second, N slots (N = timeout seconds + 1)
/// - Each slot holds (weak_ptr<TcpConnection>, generation) pairs
/// - On activity, insert into slot (current + timeout) % capacity
/// - On tick, check the current slot: generation match -> idle timeout -> shutdown; mismatch -> stale entry -> ignore
/// - Refreshing is just ++generation then re-insert; old entries invalidate themselves
///
/// Compared to the old approach (one runAfter timer per connection):
/// - Old: N connections = N Timer objects + N timerfd_settime calls
/// - New: N connections = 1 one-second timer + O(k) tick (k = entries in current slot)
class TimerWheel : noncopyable {
public:
    TimerWheel(EventLoop* loop, int timeoutSeconds);
    ~TimerWheel();

    void start();
    void stop();

    /// Insert or refresh a connection's idle timer
    /// Must be called in the EventLoop thread
    void insert(const std::shared_ptr<TcpConnection>& conn);

private:
    void tick();

    struct SlotEntry {
        std::weak_ptr<TcpConnection> conn;
        uint64_t generation;  // used to tell stale entries from live ones
    };

    int timeoutSeconds_;
    size_t capacity_;
    size_t current_;
    EventLoop* loop_;
    TimerId tickTimerId_;
    std::vector<std::vector<SlotEntry>> wheel_;
};
#endif // AETHER_BASE_TIMERWHEEL_H
