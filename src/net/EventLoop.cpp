#include "EventLoop.h"
#include "Epoller.h"
#include "Channel.h"
#include "base/Logger.h"
#include "base/TimerQueue.h"
#include "base/TimerWheel.h"
#include <sys/eventfd.h>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <cassert>

__thread EventLoop* t_loopInThisThread = nullptr;

EventLoop::EventLoop()
    : looping_(false),
      quit_(false),
      threadId_(std::this_thread::get_id()),
      poller_(new Epoller()),
      timerQueue_(new TimerQueue(this)),
      wakeupFd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      wakeupChannel_(new Channel(this, wakeupFd_)),
      callingPendingFunctors_(false) {
    if (t_loopInThisThread) {
        LOG_FATAL("Another EventLoop %p exists in this thread", t_loopInThisThread);
    } else {
        t_loopInThisThread = this;
    }
    wakeupChannel_->setReadCallback([this]() { handleRead(); });
    wakeupChannel_->enableReading();
}

EventLoop::~EventLoop() {
    wakeupChannel_->disableAll();
    wakeupChannel_->remove();
    ::close(wakeupFd_);
    t_loopInThisThread = nullptr;
}

void EventLoop::loop() {
    looping_ = true;
    quit_ = false;
    LOG_INFO("EventLoop %p start looping", this);

    while (!quit_) {
        ChannelList activeChannels;
        poller_->poll(10000, &activeChannels);

        for (auto* channel : activeChannels) {
            channel->handleEvent();
        }
        doPendingFunctors();
    }

    looping_ = false;
}

void EventLoop::quit() {
    quit_ = true;
    if (!isInLoopThread()) {
        wakeup();
    }
}

void EventLoop::updateChannel(Channel* channel) {
    poller_->updateChannel(channel);
}

void EventLoop::removeChannel(Channel* channel) {
    poller_->removeChannel(channel);
}

void EventLoop::runInLoop(Functor cb) {
    if (isInLoopThread()) {
        cb();
    } else {
        queueInLoop(std::move(cb));
    }
}

void EventLoop::queueInLoop(Functor cb) {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        pendingFunctors_.push_back(std::move(cb));
    }
    if (!isInLoopThread() || callingPendingFunctors_) {
        wakeup();
    }
}

void EventLoop::wakeup() {
    uint64_t one = 1;
    ssize_t n = ::write(wakeupFd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        LOG_ERROR("EventLoop::wakeup writes %zd bytes instead of 8", n);
    }
}

void EventLoop::handleRead() {
    uint64_t one = 1;
    ssize_t n = ::read(wakeupFd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        LOG_ERROR("EventLoop::handleRead reads %zd bytes instead of 8", n);
    }
}

void EventLoop::doPendingFunctors() {
    std::vector<Functor> functors;
    callingPendingFunctors_ = true;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        functors.swap(pendingFunctors_);
    }
    for (auto& functor : functors) {
        functor();
    }
    callingPendingFunctors_ = false;
}

bool EventLoop::isInLoopThread() const {
    return threadId_ == std::this_thread::get_id();
}

// --- timer interface ---

TimerId EventLoop::runAt(TimeStamp time, Timer::TimerCallback cb) {
    return timerQueue_->addTimer(std::move(cb), time, 0.0);
}

TimerId EventLoop::runAfter(double delay, Timer::TimerCallback cb) {
    TimeStamp time = TimeStamp::after(delay);
    return runAt(time, std::move(cb));
}

TimerId EventLoop::runEvery(double interval, Timer::TimerCallback cb) {
    TimeStamp time = TimeStamp::after(interval);
    return timerQueue_->addTimer(std::move(cb), time, interval);
}

void EventLoop::cancel(TimerId timerId) {
    timerQueue_->cancel(timerId);
}

// ─── TimerWheel ───

void EventLoop::setIdleTimeout(int seconds) {
    if (!timerWheel_ && seconds > 0) {
        timerWheel_ = std::make_unique<TimerWheel>(this, seconds);
        timerWheel_->start();
    }
}

void EventLoop::insertToWheel(const std::shared_ptr<TcpConnection>& conn) {
    if (timerWheel_) {
        timerWheel_->insert(conn);
    }
}
