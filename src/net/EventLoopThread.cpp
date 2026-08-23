
#include "EventLoopThread.h"
#include "EventLoop.h"
#include "base/Logger.h"

EventLoopThread::EventLoopThread(const ThreadInitCallback& cb,
                                 const std::string& name)
    : loop_(nullptr),
      exiting_(false),
      name_(name),
      callback_(cb) {}

EventLoopThread::~EventLoopThread() {
    exiting_ = true;
    if (loop_ != nullptr) {
        loop_->quit();
    }
    // Always join: threadFunc() clears loop_ before returning, so a fast
    // IO thread could otherwise leave thread_ joinable here and terminate
    // the process in ~thread().
    if (thread_.joinable()) {
        thread_.join();
    }
}

EventLoop* EventLoopThread::startLoop() {
    // start the thread; the thread function creates the EventLoop
    thread_ = std::thread([this]() { threadFunc(); });

    // wait until the EventLoop exists
    {
        std::unique_lock<std::mutex> lock(mtx_);
        cond_.wait(lock, [this]() { return loop_ != nullptr; });
    }

    LOG_INFO("EventLoopThread [%s] started, loop=%p", name_.c_str(), loop_);
    return loop_;
}

void EventLoopThread::threadFunc() {
    // create the EventLoop in the IO thread (one loop per thread)
    EventLoop loop;

    if (callback_) {
        callback_(&loop);
    }

    {
        std::lock_guard<std::mutex> lock(mtx_);
        loop_ = &loop;
        cond_.notify_one();  // tell the main thread the loop exists
    }

    loop.loop();  // enter the event loop (blocks)

    // after the loop exits
    loop_ = nullptr;
}
