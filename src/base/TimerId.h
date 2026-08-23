#ifndef AETHER_BASE_TIMERID_H
#define AETHER_BASE_TIMERID_H
#pragma once
#include "Timer.h"

// TimerId: timer identifier used for cancelling timers
// Holds a Timer pointer plus a sequence number, so we always cancel the right timer
class TimerId {
public:
    TimerId() : timer_(nullptr), sequence_(0) {}
    TimerId(Timer* timer, int64_t seq) : timer_(timer), sequence_(seq) {}

    // friend: TimerQueue needs access to private members
    friend class TimerQueue;

private:
    Timer* timer_;
    int64_t sequence_;
};
#endif // AETHER_BASE_TIMERID_H
