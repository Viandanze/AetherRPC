#include "EventLoopThreadPool.h"
#include "EventLoopThread.h"
#include "EventLoop.h"
#include "base/Logger.h"

EventLoopThreadPool::EventLoopThreadPool(EventLoop* baseLoop, const std::string& name)
    : baseLoop_(baseLoop),
      name_(name),
      started_(false),
      numThreads_(0),
      next_(0) {}

EventLoopThreadPool::~EventLoopThreadPool() {
    // unique_ptrs in threads_ destruct automatically; EventLoopThread's dtor quits + joins
}

void EventLoopThreadPool::start(const ThreadInitCallback& cb) {
    started_ = true;

    for (int i = 0; i < numThreads_; ++i) {
        char buf[32];
        snprintf(buf, sizeof(buf), "%s-IO-%d", name_.c_str(), i);
        auto t = std::make_unique<EventLoopThread>(cb, std::string(buf));
        EventLoop* loop = t->startLoop();
        loops_.push_back(loop);
        threads_.push_back(std::move(t));
    }

    if (numThreads_ == 0 && cb) {
        // single-reactor mode: run the init callback directly on baseLoop
        cb(baseLoop_);
    }

    LOG_INFO("EventLoopThreadPool [%s] started with %d IO threads",
             name_.c_str(), numThreads_);
}

EventLoop* EventLoopThreadPool::getNextLoop() {
    // single-reactor mode: the main thread handles all connections
    if (loops_.empty()) {
        return baseLoop_;
    }

    // round-robin: atomic increment + modulo, thread-safe
    // wrap the counter so it never overflows
    int expected = next_.load();
    if (expected > 1000000) {
        next_.store(0);
        expected = 0;
    }
    int idx = next_.fetch_add(1) % static_cast<int>(loops_.size());
    return loops_[idx];
}

std::vector<EventLoop*> EventLoopThreadPool::getAllLoops() {
    if (loops_.empty()) {
        return std::vector<EventLoop*>(1, baseLoop_);
    }
    return loops_;
}
