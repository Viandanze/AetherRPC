#ifndef AETHER_NET_BUFFER_H
#define AETHER_NET_BUFFER_H
#pragma once
#include <vector>
#include <string>
#include <algorithm>
#include <cstring>
#include <cassert>
#include <sys/uio.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>

/// Buffer: auto-growing byte buffer
///
/// Memory layout:
/// +-------------------+------------------+------------------+
/// | prependable bytes |  readable bytes  |  writable bytes  |
/// |                   |     (CONTENT)    |                  |
/// +-------------------+------------------+------------------+
/// 0      <=      readerIndex   <=   writerIndex    <=     size
///
/// Design follows muduo::net::Buffer:
/// - readv with an on-stack extrabuf reads as much as possible per syscall
/// - Grows automatically; shrink() reclaims idle space
/// - Supports prepend, e.g. for protocol headers
class Buffer {
public:
    static const size_t kCheapPrepend = 8;
    static const size_t kInitialSize = 1024;

    explicit Buffer(size_t initialSize = kInitialSize)
        : buffer_(kCheapPrepend + initialSize),
          readerIndex_(kCheapPrepend),
          writerIndex_(kCheapPrepend) {
    }

    void swap(Buffer& rhs) {
        buffer_.swap(rhs.buffer_);
        std::swap(readerIndex_, rhs.readerIndex_);
        std::swap(writerIndex_, rhs.writerIndex_);
    }

    // --- capacity queries ---
    size_t readableBytes()  const { return writerIndex_ - readerIndex_; }
    size_t writableBytes()  const { return buffer_.size() - writerIndex_; }
    size_t prependableBytes() const { return readerIndex_; }
    size_t internalCapacity() const { return buffer_.capacity(); }

    // --- read side ---
    const char* peek() const { return begin() + readerIndex_; }
    char* peek() { return begin() + readerIndex_; }

    const char* findCRLF() const {
        const char* crlf = std::search(peek(), beginWrite(), kCRLF, kCRLF + 2);
        return crlf == beginWrite() ? nullptr : crlf;
    }

    const char* findCRLF(const char* start) const {
        assert(peek() <= start);
        assert(start <= beginWrite());
        const char* crlf = std::search(start, beginWrite(), kCRLF, kCRLF + 2);
        return crlf == beginWrite() ? nullptr : crlf;
    }

    void retrieve(size_t len) {
        assert(len <= readableBytes());
        if (len < readableBytes()) {
            readerIndex_ += len;
        } else {
            retrieveAll();
        }
    }

    void retrieveUntil(const char* end) {
        assert(peek() <= end);
        assert(end <= beginWrite());
        retrieve(static_cast<size_t>(end - peek()));
    }

    void retrieveAll() {
        readerIndex_ = kCheapPrepend;
        writerIndex_ = kCheapPrepend;
    }

    std::string retrieveAsString(size_t len) {
        assert(len <= readableBytes());
        std::string result(peek(), len);
        retrieve(len);
        return result;
    }

    std::string retrieveAllAsString() {
        return retrieveAsString(readableBytes());
    }

    // --- write side ---
    void append(const char* data, size_t len) {
        ensureWritableBytes(len);
        std::copy(data, data + len, beginWrite());
        hasWritten(len);
    }

    void append(const void* data, size_t len) {
        append(static_cast<const char*>(data), len);
    }

    void append(const std::string& str) {
        append(str.data(), str.size());
    }

    void ensureWritableBytes(size_t len) {
        if (writableBytes() < len) {
            makeSpace(len);
        }
        assert(writableBytes() >= len);
    }

    char* beginWrite() { return begin() + writerIndex_; }
    const char* beginWrite() const { return begin() + writerIndex_; }

    void hasWritten(size_t len) {
        assert(len <= writableBytes());
        writerIndex_ += len;
    }

    void unwrite(size_t len) {
        assert(len <= readableBytes());
        writerIndex_ -= len;
    }

    // --- prepend ---
    void prepend(const void* data, size_t len) {
        assert(len <= prependableBytes());
        readerIndex_ -= len;
        const char* d = static_cast<const char*>(data);
        std::copy(d, d + len, begin() + readerIndex_);
    }

    void prependInt32(int32_t x) {
        int32_t be32 = htonl(x);
        prepend(&be32, sizeof(be32));
    }

    // --- read from fd (readv + on-stack extrabuf) ---
    ssize_t readFd(int fd, int* savedErrno) {
        // on-stack buffer: when writable space runs out, extrabuf catches the overflow
        char extrabuf[65536];
        struct iovec vec[2];
        const size_t writable = writableBytes();

        vec[0].iov_base = beginWrite();
        vec[0].iov_len = writable;
        vec[1].iov_base = extrabuf;
        vec[1].iov_len = sizeof(extrabuf);

        // if writable space is enough, use vec[0] only
        const int iovcnt = (writable < sizeof(extrabuf)) ? 2 : 1;
        const ssize_t n = ::readv(fd, vec, iovcnt);

        if (n < 0) {
            *savedErrno = errno;
        } else if (static_cast<size_t>(n) <= writable) {
            // all data landed in the buffer's writable area
            writerIndex_ += n;
        } else {
            // writable area was short; extrabuf caught some data too
            writerIndex_ = buffer_.size();
            append(extrabuf, static_cast<size_t>(n) - writable);
        }

        return n;
    }

    // --- shrink idle space ---
    void shrink(size_t reserve = 0) {
        Buffer other;
        other.ensureWritableBytes(readableBytes() + reserve);
        other.append(peek(), readableBytes());
        swap(other);
    }

private:
    char* begin() { return buffer_.data(); }
    const char* begin() const { return buffer_.data(); }

    void makeSpace(size_t len) {
        if (writableBytes() + prependableBytes() < len + kCheapPrepend) {
            // not enough total space, grow
            buffer_.resize(writerIndex_ + len);
        } else {
            // enough total space, move readable data forward
            size_t readable = readableBytes();
            std::copy(begin() + readerIndex_,
                      begin() + writerIndex_,
                      begin() + kCheapPrepend);
            readerIndex_ = kCheapPrepend;
            writerIndex_ = readerIndex_ + readable;
        }
    }

    std::vector<char> buffer_;
    size_t readerIndex_;
    size_t writerIndex_;

    static const char kCRLF[];
};

inline const char Buffer::kCRLF[] = "\r\n";
#endif // AETHER_NET_BUFFER_H
