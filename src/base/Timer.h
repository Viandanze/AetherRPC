#ifndef AETHER_BASE_TIMER_H
#define AETHER_BASE_TIMER_H
#pragma once
#include <functional>
#include <atomic>
#include "TimeStamp.h"

// Timer: timer abstraction
// Holds callback, expiration, repeat flag and interval
// Lifetime managed by TimerQueue
class Timer {
public:
    using TimerCallback = std::function<void()>;

    Timer(TimerCallback cb, TimeStamp when, double interval)
        : callback_(std::move(cb)),
          expiration_(when),
          interval_(interval),
          repeat_(interval > 0.0),
          sequence_(s_numCreated_++) {}

    void run() const { callback_(); }

    TimeStamp expiration() const { return expiration_; }
    bool repeat() const { return repeat_; }
    int64_t sequence() const { return sequence_; }

    // restart a repeating timer
    void restart(TimeStamp now) {
        if (repeat_) {
            expiration_ = TimeStamp::after(interval_ > 0 ? interval_ : 0.001);
        } else {
            expiration_ = TimeStamp();  // one-shot: invalidate
        }
    }

    static int64_t numCreated() { return s_numCreated_.load(); }

private:
    TimerCallback callback_;
    TimeStamp expiration_;
    double interval_;
    bool repeat_;
    int64_t sequence_;

    static std::atomic<int64_t> s_numCreated_;
};
#endif // AETHER_BASE_TIMER_H
