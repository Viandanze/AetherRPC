#ifndef AETHER_BASE_TIMESTAMP_H
#define AETHER_BASE_TIMESTAMP_H
#pragma once
#include <chrono>
#include <string>
#include <cstdio>

// TimeStamp: timestamp wrapper based on steady_clock, used by timers
// Also provides system_clock conversions for log timestamps
class TimeStamp {
public:
    // microsecond precision
    using MicroSeconds = std::chrono::microseconds;
    using SteadyTimePoint = std::chrono::steady_clock::time_point;
    using SystemTimePoint = std::chrono::system_clock::time_point;

    TimeStamp() : steadyTime_() {}

    explicit TimeStamp(SteadyTimePoint t) : steadyTime_(t) {}

    // a time point `delay` seconds from now
    static TimeStamp now() {
        return TimeStamp(std::chrono::steady_clock::now());
    }

    static TimeStamp after(double seconds) {
        return TimeStamp(std::chrono::steady_clock::now() +
                         std::chrono::duration_cast<MicroSeconds>(
                             std::chrono::duration<double>(seconds)));
    }

    // seconds elapsed since this timestamp
    double secondsFromNow() const {
        auto diff = steadyTime_ - std::chrono::steady_clock::now();
        return std::chrono::duration<double>(diff).count();
    }

    // seconds between this and another timestamp
    double diffSeconds(const TimeStamp& other) const {
        auto diff = steadyTime_ - other.steadyTime_;
        return std::chrono::duration<double>(diff).count();
    }

    bool operator<(const TimeStamp& rhs) const { return steadyTime_ < rhs.steadyTime_; }
    bool operator<=(const TimeStamp& rhs) const { return steadyTime_ <= rhs.steadyTime_; }
    bool operator>(const TimeStamp& rhs) const { return steadyTime_ > rhs.steadyTime_; }
    bool operator>=(const TimeStamp& rhs) const { return steadyTime_ >= rhs.steadyTime_; }
    bool operator==(const TimeStamp& rhs) const { return steadyTime_ == rhs.steadyTime_; }

    SteadyTimePoint steadyTime() const { return steadyTime_; }

    // convert to timespec (for timerfd_settime)
    struct timespec toTimeSpec() const {
        auto dur = steadyTime_.time_since_epoch();
        auto secs = std::chrono::duration_cast<std::chrono::seconds>(dur);
        auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(dur - secs);
        return {secs.count(), nsecs.count()};
    }

    // human-readable time string (for logging)
    static std::string nowString() {
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        char buf[64];
        std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&time_t_now));
        return buf;
    }

private:
    SteadyTimePoint steadyTime_;
};
#endif // AETHER_BASE_TIMESTAMP_H
