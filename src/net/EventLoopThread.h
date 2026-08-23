#ifndef AETHER_NET_EVENTLOOPTHREAD_H
#define AETHER_NET_EVENTLOOPTHREAD_H
#pragma once
#include <functional>
#include <mutex>
#include <condition_variable>
#include <thread>
#include "base/noncopyable.h"

class EventLoop;

// EventLoopThread: an IO thread plus the EventLoop it owns
// startLoop() waits until the EventLoop is created, making it thread-safe
class EventLoopThread : noncopyable {
public:
    using ThreadInitCallback = std::function<void(EventLoop*)>;

    EventLoopThread(const ThreadInitCallback& cb = ThreadInitCallback(),
                    const std::string& name = std::string());
    ~EventLoopThread();

    EventLoop* startLoop();  // start the thread and return its EventLoop

private:
    void threadFunc();  // thread entry function

    EventLoop* loop_;  // the loop created in this thread (raw pointer; lifetime owned by the thread)
    bool exiting_;
    std::string name_;
    std::thread thread_;
    std::mutex mtx_;
    std::condition_variable cond_;
    ThreadInitCallback callback_;  // thread init callback
};
#endif // AETHER_NET_EVENTLOOPTHREAD_H
