#ifndef AETHER_ZK_CLIENT_H
#define AETHER_ZK_CLIENT_H
#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "base/noncopyable.h"

struct _zhandle;              // zookeeper handle, kept opaque here
typedef _zhandle zhandle_t;   // match zookeeper.h's typedef

namespace aether {
namespace zk {

enum class SessionEvent {
    kConnected,     // session reached CONNECTED (from connect())
    kReconnecting,  // transport lost, the ZK client lib is retrying
    kExpired,       // session expired: the old handle is unusable,
                    // ephemeral nodes are gone, watchers are cleared
    kReconnected,   // healed with a NEW session (re-register / re-watch!)
};

/// ZkClient: a thin, thread-safe wrapper around the multi-threaded
/// ZooKeeper C client (libzookeeper_mt).
///
/// Design notes:
///  * All node operations are the *synchronous* flavor of the C API and
///    serialize on one mutex together with the handle itself, so a
///    healing close/re-init never races an in-flight call.
///  * Watchers fire on ZK's own IO thread. We only forward them to
///    user callbacks there - subscribers must keep their callbacks
///    cheap (wake a thread, set a flag). Doing blocking work (or
///    calling zookeeper_close()) from a watcher would deadlock the
///    IO thread.
///  * Session healing: on EXPIRED a dedicated thread closes the dead
///    handle, re-initializes and waits for CONNECTED (retrying), then
///    notifies subscribers with kReconnected. Ephemeral nodes and
///    watchers do NOT survive a session loss, so subscribers are
///    expected to re-register / re-watch on kReconnected.
class ZkClient : noncopyable {
public:
    using ChildWatchCallback = std::function<void(const std::string& path)>;
    using SessionCallback = std::function<void(SessionEvent event)>;

    /// `servers`: comma separated "host:port" list, e.g. "127.0.0.1:2181".
    ZkClient(const std::string& servers, int recvTimeoutMs = 8000);
    ~ZkClient();

    /// Blocking connect: waits until the session is CONNECTED or the
    /// timeout elapses. Call once at startup, before any node op.
    bool connect(int timeoutMs = 5000);

    /// Stop the healing thread and close the handle. Called by the
    /// destructor; call manually for deterministic teardown order.
    void shutdown();

    bool connected() const;

    // ---- node operations (synchronous, thread safe) ---------------------

    /// Create a persistent node (parents are created as needed).
    /// Succeeds if the node already exists (idempotent).
    bool createPersistent(const std::string& path, const std::string& value = "");

    /// Create an ephemeral node (parents are created persistent as
    /// needed). If a same-path node lingers from a previous session,
    /// it is removed first. The node vanishes when this session ends.
    bool createEphemeral(const std::string& path, const std::string& value = "");

    bool exists(const std::string& path);

    bool remove(const std::string& path);

    /// List children of `path` without installing a watcher.
    /// Returns false if the node does not exist.
    bool getChildren(const std::string& path, std::vector<std::string>* out);

    /// List children AND install a one-shot child watcher on `path`.
    /// Later child events are delivered via child callbacks (on ZK's
    /// IO thread). Re-install after every event to keep watching.
    /// Returns false if the node does not exist.
    bool getChildrenAndWatch(const std::string& path,
                             std::vector<std::string>* out);

    // ---- subscriptions ----------------------------------------------------

    /// Child events for any path watched via getChildrenAndWatch().
    void addChildCallback(ChildWatchCallback cb);

    /// Session lifecycle events (see SessionEvent).
    void addSessionCallback(SessionCallback cb);

    /// "/a/b/c" -> "/a/b"; returns "" for single-segment paths.
    static std::string parentPath(const std::string& path);

private:
    static void watcherTrampoline(zhandle_t* zh, int type, int state,
                                  const char* path, void* ctx);
    void handleWatcher(int type, int state, const char* path);

    void healingLoop();          // runs on healingThread_
    bool waitUntilConnected(int timeoutMs);
    bool ensureParentsLocked(const std::string& path);
    zhandle_t* acquireHandle();  // current handle or nullptr

    const std::string servers_;
    const int recvTimeoutMs_;

    // Guards zh_ AND serializes every C API call (see class comment).
    mutable std::mutex handleMtx_;
    zhandle_t* zh_ = nullptr;

    std::mutex cbMtx_;
    std::vector<ChildWatchCallback> childCbs_;
    std::vector<SessionCallback> sessionCbs_;

    // Session healing machinery.
    std::thread healingThread_;
    std::mutex healMtx_;
    std::condition_variable healCv_;
    bool expired_ = false;  // guarded by healMtx_
    std::atomic<bool> shuttingDown_{false};
};

}  // namespace zk
}  // namespace aether

#endif  // AETHER_ZK_CLIENT_H
