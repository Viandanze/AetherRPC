#include "AsyncLogger.h"
#include "base/Logger.h"
#include <iostream>
#include <chrono>
#include <ctime>
#include <cstdio>
#include <cstring>
#include <algorithm>
#include <filesystem>
#include <unistd.h>

namespace fs = std::filesystem;

AsyncLogger& AsyncLogger::instance() {
    static AsyncLogger logger;
    return logger;
}

AsyncLogger::AsyncLogger()
    : logFile_("./aether.log"),
      flushInterval_(3),
      rollSize_(100 * 1024 * 1024),
      keepFiles_(10),
      writtenBytes_(0),
      lastDay_(-1),
      frontBuf_(new Buffer),
      backBuf_(new Buffer),
      running_(false) {
    frontBuf_->reset();
    backBuf_->reset();
}

AsyncLogger::~AsyncLogger() {
    stop();
}

void AsyncLogger::start() {
    running_ = true;
    rollFile();  // create the log file at startup
    thread_ = std::thread([this]() { threadFunc(); });
}

void AsyncLogger::stop() {
    running_ = false;
    cond_.notify_one();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void AsyncLogger::append(const char* logline, int len) {
    std::lock_guard<std::mutex> lock(mtx_);

    if (frontBuf_->avail() > len) {
        frontBuf_->append(logline, len);
    } else {
        buffersToWrite_.push_back(std::move(frontBuf_));

        if (backBuf_) {
            frontBuf_ = std::move(backBuf_);
        } else {
            frontBuf_.reset(new Buffer);
        }
        frontBuf_->reset();
        frontBuf_->append(logline, len);

        cond_.notify_one();
    }
}

void AsyncLogger::threadFunc() {
    while (running_) {
        {
            std::unique_lock<std::mutex> lock(mtx_);
            if (buffersToWrite_.empty()) {
                cond_.wait_for(lock, std::chrono::seconds(flushInterval_));
            }

            buffersToWrite_.push_back(std::move(frontBuf_));

            frontBuf_.reset(new Buffer);
            frontBuf_->reset();

            if (!backBuf_) {
                backBuf_.reset(new Buffer);
                backBuf_->reset();
            }
        }

        // check for day rollover
        time_t now = time(nullptr);
        struct tm tm_now;
        localtime_r(&now, &tm_now);

        if (lastDay_ != -1 && tm_now.tm_mday != lastDay_) {
            rollFile();
        }

        if (!fileStream_.is_open()) {
            rollFile();
        }

        // write to file (no lock needed)
        for (auto& buf : buffersToWrite_) {
            fileStream_.write(buf->data(), buf->length());
            writtenBytes_ += buf->length();
        }

        // check size-based rollover
        if (writtenBytes_ >= rollSize_) {
            rollFile();
        }

        fileStream_.flush();

        // recycle buffers for reuse
        if (buffersToWrite_.size() > 1) {
            buffersToWrite_.erase(buffersToWrite_.begin() + 1, buffersToWrite_.end());
        }
        if (!buffersToWrite_.empty()) {
            buffersToWrite_[0]->reset();
            backBuf_ = std::move(buffersToWrite_[0]);
        }
        buffersToWrite_.clear();
    }

    // flush before exit
    if (fileStream_.is_open()) {
        fileStream_.flush();
        fileStream_.close();
    }
}

std::string AsyncLogger::getLogFileName(const struct tm& tm_now) {
    char timeStr[32];
    strftime(timeStr, sizeof(timeStr), "%Y%m%d-%H%M%S", &tm_now);

    // format: basename.20260622-180530.pid12345.log
    return logFile_ + "." + timeStr + ".pid" + std::to_string(::getpid()) + ".log";
}

void AsyncLogger::rollFile() {
    time_t now = time(nullptr);
    struct tm tm_now;
    localtime_r(&now, &tm_now);

    if (fileStream_.is_open()) {
        fileStream_.close();
    }

    std::string filename = getLogFileName(tm_now);
    fileStream_.open(filename, std::ios::app);
    if (!fileStream_.is_open()) {
        std::cerr << "Failed to open log file: " << filename << std::endl;
    }

    writtenBytes_ = 0;
    lastDay_ = tm_now.tm_mday;

    // clean up old log files
    cleanOldFiles();
}

void AsyncLogger::cleanOldFiles() {
    if (keepFiles_ <= 0) return;  // 0 = no cleanup

    try {
        // collect all matching log files
        fs::path dir = fs::path(logFile_).parent_path();
        if (dir.empty()) dir = ".";

        std::string baseName = fs::path(logFile_).filename().string();

        std::vector<fs::path> logFiles;
        for (const auto& entry : fs::directory_iterator(dir)) {
            if (entry.is_regular_file()) {
                std::string name = entry.path().filename().string();
                // match basename.*.pid*.log
                if (name.find(baseName) == 0 && name.size() > baseName.size()) {
                    logFiles.push_back(entry.path());
                }
            }
        }

        // sort by modification time (oldest first)
        std::sort(logFiles.begin(), logFiles.end(),
            [](const fs::path& a, const fs::path& b) {
                return fs::last_write_time(a) < fs::last_write_time(b);
            });

        // keep the newest keepFiles_ files, delete the rest
        if (static_cast<int>(logFiles.size()) > keepFiles_) {
            int toDelete = static_cast<int>(logFiles.size()) - keepFiles_;
            for (int i = 0; i < toDelete; ++i) {
                fs::remove(logFiles[i]);
                LOG_INFO("AsyncLogger: removed old log file: %s", logFiles[i].c_str());
            }
        }
    } catch (const fs::filesystem_error& e) {
        // filesystem errors must not break log writing
        LOG_ERROR("AsyncLogger: cleanOldFiles error: %s", e.what());
    }
}

bool AsyncLogger::isSameDay(const struct tm& a, const struct tm& b) {
    return a.tm_year == b.tm_year && a.tm_yday == b.tm_yday;
}
