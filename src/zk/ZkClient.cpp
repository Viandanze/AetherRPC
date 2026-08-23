#include "zk/ZkClient.h"

#include <chrono>

#include <zookeeper/zookeeper.h>

namespace aether {
namespace zk {

namespace {

// The C client logs its whole handshake at INFO level; keep the noise
// out of our test output unless something actually breaks.
struct ZkLogQuiet {
    ZkLogQuiet() { zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR); }
};
const ZkLogQuiet g_zkLogQuiet;

// Watch a node for ~forever by re-fetching. Not used directly here;
// documented for readers: watchers in ZK 3.4 are one-shot.

std::vector<std::string> toStringVector(const struct String_vector* v) {
    std::vector<std::string> out;
    if (v != nullptr) {
        out.reserve(static_cast<size_t>(v->count));
        for (int i = 0; i < v->count; ++i) out.emplace_back(v->data[i]);
    }
    return out;
}

}  // namespace

ZkClient::ZkClient(const std::string& servers, int recvTimeoutMs)
    : servers_(servers), recvTimeoutMs_(recvTimeoutMs) {}

ZkClient::~ZkClient() { shutdown(); }

bool ZkClient::connect(int timeoutMs) {
    {
        std::lock_guard<std::mutex> lock(handleMtx_);
        if (zh_ != nullptr) return true;  // already connected

        zh_ = zookeeper_init(servers_.c_str(), &ZkClient::watcherTrampoline,
                             recvTimeoutMs_, nullptr, this, 0);
        if (zh_ == nullptr) return false;
    }

    // The healing thread outlives any single handle.
    if (!healingThread_.joinable()) {
        healingThread_ = std::thread([this] { healingLoop(); });
    }

    if (!waitUntilConnected(timeoutMs)) {
        std::lock_guard<std::mutex> lock(handleMtx_);
        if (zh_ != nullptr) {
            zookeeper_close(zh_);
            zh_ = nullptr;
        }
        return false;
    }
    {
        std::lock_guard<std::mutex> cbLock(cbMtx_);
        for (auto& cb : sessionCbs_) cb(SessionEvent::kConnected);
    }
    return true;
}

void ZkClient::shutdown() {
    if (shuttingDown_.exchange(true)) return;
    {
        std::lock_guard<std::mutex> lock(healMtx_);
        expired_ = false;  // nothing left to heal
    }
    healCv_.notify_all();
    if (healingThread_.joinable()) healingThread_.join();
    std::lock_guard<std::mutex> lock(handleMtx_);
    if (zh_ != nullptr) {
        zookeeper_close(zh_);
        zh_ = nullptr;
    }
}

bool ZkClient::connected() const {
    std::lock_guard<std::mutex> lock(handleMtx_);
    return zh_ != nullptr && zoo_state(zh_) == ZOO_CONNECTED_STATE;
}

std::string ZkClient::parentPath(const std::string& path) {
    auto pos = path.find_last_of('/');
    if (pos == std::string::npos || pos == 0) return "";
    return path.substr(0, pos);
}

/// Create every prefix directory of `path` as a persistent node.
/// Callers must hold handleMtx_ and have verified zh_ != nullptr.
bool ZkClient::ensureParentsLocked(const std::string& path) {
    const std::string parent = parentPath(path);
    std::string built;
    size_t pos = 1;  // skip the leading '/'
    while (pos < parent.size()) {
        size_t next = parent.find('/', pos);
        if (next == std::string::npos) next = parent.size();
        built += '/';
        built += parent.substr(pos, next - pos);
        int rc = zoo_create(zh_, built.c_str(), nullptr, -1,
                            &ZOO_OPEN_ACL_UNSAFE, 0, nullptr, 0);
        if (rc != ZOK && rc != ZNODEEXISTS) return false;
        pos = next + 1;
    }
    return true;
}

bool ZkClient::createPersistent(const std::string& path,
                                const std::string& value) {
    std::lock_guard<std::mutex> lock(handleMtx_);
    if (zh_ == nullptr) return false;

    // Ensure parent chain (ZK has no recursive create).
    if (!parentPath(path).empty() && !ensureParentsLocked(path)) return false;

    int rc = zoo_create(zh_, path.c_str(), value.data(),
                        static_cast<int>(value.size()), &ZOO_OPEN_ACL_UNSAFE,
                        0, nullptr, 0);
    return rc == ZOK || rc == ZNODEEXISTS;
}

bool ZkClient::createEphemeral(const std::string& path,
                               const std::string& value) {
    std::lock_guard<std::mutex> lock(handleMtx_);
    if (zh_ == nullptr) return false;

    // Parent chain as persistent nodes (idempotent).
    if (!parentPath(path).empty() && !ensureParentsLocked(path)) return false;

    int rc = zoo_create(zh_, path.c_str(), value.data(),
                        static_cast<int>(value.size()), &ZOO_OPEN_ACL_UNSAFE,
                        ZOO_EPHEMERAL, nullptr, 0);
    if (rc == ZOK) return true;
    if (rc != ZNODEEXISTS) return false;

    // Stale node from an earlier session of ours: remove and retry once.
    if (zoo_delete(zh_, path.c_str(), -1) != ZOK) return false;
    rc = zoo_create(zh_, path.c_str(), value.data(),
                    static_cast<int>(value.size()), &ZOO_OPEN_ACL_UNSAFE,
                    ZOO_EPHEMERAL, nullptr, 0);
    return rc == ZOK;
}

bool ZkClient::exists(const std::string& path) {
    std::lock_guard<std::mutex> lock(handleMtx_);
    if (zh_ == nullptr) return false;
    return zoo_exists(zh_, path.c_str(), 0, nullptr) == ZOK;
}

bool ZkClient::remove(const std::string& path) {
    std::lock_guard<std::mutex> lock(handleMtx_);
    if (zh_ == nullptr) return false;
    int rc = zoo_delete(zh_, path.c_str(), -1);
    return rc == ZOK || rc == ZNONODE;
}

bool ZkClient::getChildren(const std::string& path,
                           std::vector<std::string>* out) {
    std::lock_guard<std::mutex> lock(handleMtx_);
    if (zh_ == nullptr) return false;
    struct String_vector v;
    int rc = zoo_get_children(zh_, path.c_str(), 0, &v);
    if (rc != ZOK) return false;
    *out = toStringVector(&v);
    deallocate_String_vector(&v);
    return true;
}

bool ZkClient::getChildrenAndWatch(const std::string& path,
                                   std::vector<std::string>* out) {
    std::lock_guard<std::mutex> lock(handleMtx_);
    if (zh_ == nullptr) return false;
    struct String_vector v;
    int rc = zoo_wget_children(zh_, path.c_str(), &ZkClient::watcherTrampoline,
                               this, &v);
    if (rc != ZOK) return false;
    *out = toStringVector(&v);
    deallocate_String_vector(&v);
    return true;
}

void ZkClient::addChildCallback(ChildWatchCallback cb) {
    std::lock_guard<std::mutex> lock(cbMtx_);
    childCbs_.push_back(std::move(cb));
}

void ZkClient::addSessionCallback(SessionCallback cb) {
    std::lock_guard<std::mutex> lock(cbMtx_);
    sessionCbs_.push_back(std::move(cb));
}

// ---- watcher plumbing (ZK IO thread) --------------------------------------

void ZkClient::watcherTrampoline(zhandle_t* /*zh*/, int type, int state,
                                 const char* path, void* ctx) {
    static_cast<ZkClient*>(ctx)->handleWatcher(type, state, path);
}

void ZkClient::handleWatcher(int type, int state, const char* path) {
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_EXPIRED_SESSION_STATE) {
            {
                std::lock_guard<std::mutex> lock(cbMtx_);
                for (auto& cb : sessionCbs_) cb(SessionEvent::kExpired);
            }
            std::lock_guard<std::mutex> lock(healMtx_);
            expired_ = true;
            healCv_.notify_one();
        } else if (state == ZOO_CONNECTING_STATE ||
                   state == ZOO_ASSOCIATING_STATE) {
            std::lock_guard<std::mutex> lock(cbMtx_);
            for (auto& cb : sessionCbs_) cb(SessionEvent::kReconnecting);
        }
        return;
    }

    if (type == ZOO_CHILD_EVENT && path != nullptr) {
        std::vector<ChildWatchCallback> cbs;
        {
            std::lock_guard<std::mutex> lock(cbMtx_);
            cbs = childCbs_;
        }
        for (auto& cb : cbs) cb(path);  // keep it light!
    }
}

// ---- session healing thread ------------------------------------------------

zhandle_t* ZkClient::acquireHandle() {
    std::lock_guard<std::mutex> lock(handleMtx_);
    return zh_;
}

bool ZkClient::waitUntilConnected(int timeoutMs) {
    zhandle_t* zh = acquireHandle();
    if (zh == nullptr) return false;
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (zoo_state(zh) == ZOO_CONNECTED_STATE) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return zoo_state(zh) == ZOO_CONNECTED_STATE;
}

void ZkClient::healingLoop() {
    while (true) {
        std::unique_lock<std::mutex> lock(healMtx_);
        healCv_.wait(lock, [this] { return expired_ || shuttingDown_.load(); });
        if (shuttingDown_.load()) return;
        lock.unlock();

        // Phase 1 of healing: dispose of the dead handle. The watcher
        // that reported EXPIRED has already returned (it only notified
        // this thread), so closing here cannot deadlock the IO thread.
        {
            std::lock_guard<std::mutex> hLock(handleMtx_);
            if (zh_ != nullptr) {
                zookeeper_close(zh_);
                zh_ = nullptr;
            }
        }

        // Phase 2: re-init and wait for a fresh session, retrying.
        while (!shuttingDown_.load()) {
            {
                std::lock_guard<std::mutex> hLock(handleMtx_);
                if (zh_ == nullptr) {
                    zh_ = zookeeper_init(servers_.c_str(),
                                         &ZkClient::watcherTrampoline,
                                         recvTimeoutMs_, nullptr, this, 0);
                }
                if (zh_ == nullptr) break;  // init failure; retry below
            }
            if (waitUntilConnected(5000)) {
                {
                    std::lock_guard<std::mutex> lock2(healMtx_);
                    expired_ = false;
                }
                {
                    std::lock_guard<std::mutex> cbLock(cbMtx_);
                    for (auto& cb : sessionCbs_)
                        cb(SessionEvent::kReconnected);
                }
                break;  // healed; go back to waiting
            }
            // Not connected within the window; keep retrying with the
            // same handle (the lib reconnects in the background).
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }
}

}  // namespace zk
}  // namespace aether
