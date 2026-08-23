#include "Logger.h"
#include "AsyncLogger.h"
#include <ctime>
#include <cstring>

Logger& Logger::instance() {
    static Logger logger;
    return logger;
}

Logger::Logger() : level_(LogLevel::INFO), asyncMode_(false) {}

void Logger::setLevel(LogLevel level) { level_ = level; }

void Logger::enableAsync(const std::string& logFile) {
    asyncMode_ = true;
    AsyncLogger::instance().setLogFile(logFile);
    AsyncLogger::instance().start();
}

const char* Logger::levelStr(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO";
        case LogLevel::WARN:  return "WARN";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::FATAL: return "FATAL";
    }
    return "UNKNOWN";
}

void Logger::log(LogLevel level, const char* file, int line, const char* fmt, ...) {
    if (level < level_) return;

    // format user message
    char msgBuf[4096];
    va_list args;
    va_start(args, fmt);
    vsnprintf(msgBuf, sizeof(msgBuf), fmt, args);
    va_end(args);

    // format full log line
    time_t now = time(nullptr);
    struct tm tm_buf;
    localtime_r(&now, &tm_buf);
    char timeStr[32];
    strftime(timeStr, sizeof(timeStr), "%Y-%m-%d %H:%M:%S", &tm_buf);

    const char* filename = strrchr(file, '/');
    filename = filename ? filename + 1 : file;

    char lineBuf[8192];
    int len = snprintf(lineBuf, sizeof(lineBuf), "[%s] [%s] %s:%d - %s\n",
                       timeStr, levelStr(level), filename, line, msgBuf);

    if (asyncMode_) {
        // async mode: write into buffer
        AsyncLogger::instance().append(lineBuf, len);
    } else {
        // sync mode: write directly to stderr
        std::lock_guard<std::mutex> lock(mtx_);
        fwrite(lineBuf, 1, len, stderr);
        fflush(stderr);
    }

    if (level == LogLevel::FATAL) {
        if (asyncMode_) {
            AsyncLogger::instance().stop();  // make sure logs are flushed
        }
        abort();
    }
}
