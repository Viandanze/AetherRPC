#include "TimerQueue.h"
#include "net/EventLoop.h"
#include "net/Channel.h"
#include "base/Logger.h"
#include <sys/timerfd.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>

// create timerfd
static int createTimerFd() {
    int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (fd < 0) {
        LOG_FATAL("timerfd_create failed: %s", strerror(errno));
    }
    return fd;
}

// set timerfd expiration
static void resetTimerFd(int fd, TimeStamp expiration) {
    struct itimerspec newValue;
    struct itimerspec oldValue;
    memset(&newValue, 0, sizeof(newValue));
    memset(&oldValue, 0, sizeof(oldValue));

    // when it should fire first
    newValue.it_value = expiration.toTimeSpec();
    // no interval (TimerQueue manages repeating timers itself)

    int ret = ::timerfd_settime(fd, TFD_TIMER_ABSTIME, &newValue, &oldValue);
    if (ret < 0) {
        LOG_ERROR("timerfd_settime failed: %s", strerror(errno));
    }
}

// read timerfd (mandatory, otherwise epoll keeps triggering)
static void readTimerFd(int fd) {
    uint64_t howmany;
    ssize_t n = ::read(fd, &howmany, sizeof(howmany));
    if (n != sizeof(howmany)) {
        LOG_ERROR("TimerQueue::handleRead reads %zd bytes instead of 8", n);
    }
}

TimerQueue::TimerQueue(EventLoop* loop)
    : loop_(loop),
      timerFd_(createTimerFd()),
      timerFdChannel_(new Channel(loop, timerFd_)),
      timers_(),
      activeTimers_(),
      callingExpiredTimers_(false) {
    timerFdChannel_->setReadCallback([this]() { handleRead(); });
    timerFdChannel_->enableReading();
}

TimerQueue::~TimerQueue() {
    timerFdChannel_->disableAll();
    timerFdChannel_->remove();
    ::close(timerFd_);

    // delete all timers
    for (auto& entry : timers_) {
        delete entry.second;
    }
}

TimerId TimerQueue::addTimer(Timer::TimerCallback cb, TimeStamp when, double interval) {
    auto* timer = new Timer(std::move(cb), when, interval);
    loop_->runInLoop([this, timer]() { addTimerInLoop(timer); });
    return TimerId(timer, timer->sequence());
}

void TimerQueue::addTimerInLoop(Timer* timer) {
    bool earliestChanged = insert(timer);

    if (earliestChanged) {
        // earliest timer changed, reset timerfd
        resetTimerFd(timerFd_, timer->expiration());
    }
}

void TimerQueue::cancel(TimerId timerId) {
    loop_->runInLoop([this, timerId]() { cancelInLoop(timerId); });
}

void TimerQueue::cancelInLoop(TimerId timerId) {
    ActiveTimer timer(timerId.timer_, timerId.sequence_);
    auto it = activeTimers_.find(timer);

    if (it != activeTimers_.end()) {
        size_t n = timers_.erase(Entry(it->first->expiration(), it->first));
        delete it->first;
        activeTimers_.erase(it);
    } else if (callingExpiredTimers_) {
        // timer whose callback is running: add to the canceling list
        cancelingTimers_.insert(timer);
    }
}

void TimerQueue::handleRead() {
    readTimerFd(timerFd_);

    TimeStamp now = TimeStamp::now();
    std::vector<Entry> expired = getExpired(now);

    callingExpiredTimers_ = true;
    cancelingTimers_.clear();

    for (auto& entry : expired) {
        entry.second->run();  // run timer callback
    }

    callingExpiredTimers_ = false;
    reset(expired, now);
}

std::vector<TimerQueue::Entry> TimerQueue::getExpired(TimeStamp now) {
    std::vector<Entry> expired;

    // sentinel: first entry greater than now
    Entry sentry(now, reinterpret_cast<Timer*>(UINTPTR_MAX));
    auto end = timers_.lower_bound(sentry);

    // [begin, end) are all expired
    expired.assign(timers_.begin(), end);
    timers_.erase(timers_.begin(), end);

    // remove from activeTimers_ in sync
    for (auto& entry : expired) {
        ActiveTimer timer(entry.second, entry.second->sequence());
        size_t n = activeTimers_.erase(timer);
        (void)n;
    }

    return expired;
}

void TimerQueue::reset(const std::vector<Entry>& expired, TimeStamp now) {
    TimeStamp nextExpire;

    for (auto& entry : expired) {
        ActiveTimer timer(entry.second, entry.second->sequence());

        // if not cancelled during callback execution and repeating, restart
        if (entry.second->repeat() &&
            cancelingTimers_.find(timer) == cancelingTimers_.end()) {
            entry.second->restart(now);
            insert(entry.second);
        } else {
            // one-shot or cancelled: delete the Timer object
            delete entry.second;
        }
    }

    // arm the next timerfd
    if (!timers_.empty()) {
        nextExpire = timers_.begin()->second->expiration();
        if (nextExpire.secondsFromNow() <= 0) {
            // already expired (e.g. tiny interval / clock skew): clamp to +1ms
            // so the timerfd still fires instead of being silently dropped
            nextExpire = TimeStamp::after(0.001);
        }
        resetTimerFd(timerFd_, nextExpire);
    }
}

bool TimerQueue::insert(Timer* timer) {
    bool earliestChanged = false;
    TimeStamp when = timer->expiration();
    auto it = timers_.begin();

    if (it == timers_.end() || when < it->first) {
        earliestChanged = true;
    }

    timers_.insert(Entry(when, timer));
    activeTimers_.insert(ActiveTimer(timer, timer->sequence()));

    return earliestChanged;
}
