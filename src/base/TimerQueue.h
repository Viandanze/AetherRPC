#ifndef AETHER_BASE_TIMERQUEUE_H
#define AETHER_BASE_TIMERQUEUE_H
#pragma once
#include <set>
#include <vector>
#include <memory>
#include "base/TimeStamp.h"
#include "base/Timer.h"
#include "base/TimerId.h"
#include "base/noncopyable.h"

class EventLoop;
class Channel;

// TimerQueue: timer queue based on timerfd + epoll
// 
// Design notes:
// 1. Use timerfd to integrate timer events into the epoll loop
// 2. Manage timers in a std::set ordered by expiration
// 3. Always arm timerfd with the earliest expiration only
// 4. Support one-shot and repeating timers
class TimerQueue : noncopyable {
public:
    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    // run callback at the given time (one-shot)
    TimerId addTimer(Timer::TimerCallback cb, TimeStamp when, double interval = 0.0);

    // cancel a timer
    void cancel(TimerId timerId);

private:
    // pair<TimeStamp, Timer*> as the set key guarantees uniqueness
    using Entry = std::pair<TimeStamp, Timer*>;
    using TimerList = std::set<Entry>;
    using ActiveTimer = std::pair<Timer*, int64_t>;  // Timer* + sequence
    using ActiveTimerSet = std::set<ActiveTimer>;

    void addTimerInLoop(Timer* timer);
    void cancelInLoop(TimerId timerId);

    // timerfd readable callback
    void handleRead();

    // get all expired timers
    std::vector<Entry> getExpired(TimeStamp now);

    // reset repeating timers and update timerfd
    void reset(const std::vector<Entry>& expired, TimeStamp now);

    bool insert(Timer* timer);

    EventLoop* loop_;
    const int timerFd_;
    std::unique_ptr<Channel> timerFdChannel_;

    // Timer list sorted by expiration
    TimerList timers_;

    // Active timer set for cancel
    ActiveTimerSet activeTimers_;
    bool callingExpiredTimers_;
    ActiveTimerSet cancelingTimers_;  // timers pending cancellation while callbacks are running
};
#endif // AETHER_BASE_TIMERQUEUE_H
