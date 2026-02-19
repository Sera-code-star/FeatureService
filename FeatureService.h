#ifndef FEATURE_SERVICE_H
#define FEATURE_SERVICE_H

#include <cstdint>
#include <atomic>
#include <unordered_map>
#include <string>
#include <vector>
#include <deque>
#include <functional>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace feat {

    // ---------- Status / ServiceState ----------
    enum class Status : int {
        Ok = 0,
        Invalid = 1,
        BadState = 2,
        Internal = 3,
        NotFound = 4
    };

    enum class ServiceState : int {
        NotRunning   = 0,
        Initializing = 1,  // start() called; libs being inited in worker thread
        Running      = 2
    };

    // ---------- Payload (no header) ----------
    struct FeaturePayload {
        char  tag[8];  // 8-byte ASCII tag (e.g., "FEATRUN1")
        void* data;    // tag-specific pointer; freed by tag-specific deleter
    };

    // ---------- Lifecycle payload ----------
    enum class FeatureRunningState : std::uint8_t {
        NotRunning = 0,
        Running = 1
    };

    struct FeatureRunningData {
        FeatureRunningState state;
    };

    // ---------- Result payload ----------
    typedef std::uint64_t FeatureTicket;  // ticket/ID for submitted tasks

    struct FeatureResultData {
        FeatureTicket ticket;
        void* data; // malloc'ed buffer (may be NULL for empty)
        size_t        size; // 0 for empty
    };

    // ---------- Tags (exactly 8 bytes) ----------
    static const char TAG_FEAT_RUNNING_V1[8] = { 'F','E','A','T','R','U','N','1' };
    static const char TAG_FEAT_RESULT_V1[8] = { 'F','E','A','T','R','E','S','1' };

    // ---------- Callback ----------
    typedef void(*FeatureCallback)(void* user, FeaturePayload* payload);

    // ---------- Init Options (C++11: string->string) ----------
    typedef std::unordered_map<std::string, std::string> FeatureOptions;

    // ---------- Class-scoped deleter type ----------
    typedef void(*FeaturePayloadDeleter)(FeaturePayload* payload);

    // ---------- Lower-lib interface ----------
    // Each lower-level library implements this. The service does NOT own instances.
    class IFeatureLib {
    public:
        virtual ~IFeatureLib() {}
        // Called on the worker thread during the init task. Returns false if init fails.
        virtual bool init(const FeatureOptions& opts) = 0;
        // Called after init() returns true to collect the keywords this lib exposes.
        virtual std::vector<std::string> getKeywords() const = 0;
    };

    // ---------- Authorization verifier interface ----------
    // A single verifier is held by the service. It does NOT own the instance.
    class IFeatureVerifier {
    public:
        virtual ~IFeatureVerifier() {}
        // Returns true if the combined keyword set permits a submit().
        virtual bool isAuthorized(const std::vector<std::string>& keywords) const = 0;
    };

    // ---------- FeatureService ----------
    class FeatureService {
    public:
        // Contract:
        //  - start(): set Initializing, spawn worker thread; worker inits libs in background
        //             and transitions to Running, emitting the "Running" callback when ready
        //  - stop():  stop accepting work, join worker, notify "NotRunning", clear keywords
        // libs and verifier are NOT owned by the service; caller manages their lifetime.
        explicit FeatureService(FeatureCallback cb = 0,
            void* user = 0,
            const FeatureOptions& opts = FeatureOptions(),
            const std::vector<IFeatureLib*>& libs = std::vector<IFeatureLib*>(),
            IFeatureVerifier* verifier = 0);
        ~FeatureService();

        FeatureService(const FeatureService&) = delete;
        FeatureService& operator=(const FeatureService&) = delete;

        // Lifecycle
        //   start()    — async: spawns worker, returns immediately; Running callback fires when ready.
        //   stop()     — async: signals worker, drains queue, fires NotRunning callback, returns
        //                immediately (worker may still be finishing its current task).
        //   stopSync() — same as stop() but also blocks until the worker thread exits.
        //                Use stopSync() (or let the destructor do it) before re-starting or
        //                destroying the service.
        Status start();
        Status stop();
        Status stopSync();

        // Submit (always cancellable):
        // - fn(out) must fill 'out' with result bytes (may be empty).
        // Returns a non-zero ticket on success; 0 on failure (e.g., not Running).
        FeatureTicket submit(const std::function<void(std::vector<uint8_t>& out)>& fn);

        // Cancel a specific task by ticket (best-effort).
        // - Pending: removed from queue; immediately emits FEATRES1 with empty data for that ticket.
        // - Running: native cancel handled by lower libs; DOES NOT EMIT. Worker will emit the single terminal result from fn(out).
        // - Unknown/completed: NotFound
        Status cancel(FeatureTicket ticket);

        // State snapshot
        bool         isRunning()      const { return state_.load() == ServiceState::Running; }
        bool         isInitializing() const { return state_.load() == ServiceState::Initializing; }
        ServiceState state()          const { return state_.load(); }

        // Payload management
        void   releasePayload(FeaturePayload* payload) { DeletePayload(payload); }

        // ---------- Class-scoped (static) deleter registry ----------
        static Status RegisterPayloadDeleter(const char tag8[8], FeaturePayloadDeleter d);
        static void   DeletePayload(FeaturePayload* payload);

        // Options accessors
        bool        hasOption(const std::string& key) const;
        std::string getOption(const std::string& key) const;
        std::string getOptionOr(const std::string& key, const std::string& fallback) const;
        bool        getBoolOr(const std::string& key, bool fallback) const;
        int         getIntOr(const std::string& key, int fallback) const;
        long long   getLongLongOr(const std::string& key, long long fallback) const;
        double      getDoubleOr(const std::string& key, double fallback) const;

    private:
        // Emit lifecycle (Running/NotRunning)
        void                emitRunningEvent(bool running);
        static FeaturePayload* makeRunningPayload(bool running);

        // Emit result (ticket + data); data_ptr may be NULL/size=0 (empty).
        void emitResult(FeatureTicket ticket, const void* data_ptr, size_t size);

        // Built-in deleters
        static void DefaultFree(FeaturePayload* payload); // for FEATRUN1
        static void ResultFree(FeaturePayload* payload);  // for FEATRES1

        // Parsing helpers (C++11)
        static bool       parseBool(const std::string& s, bool* ok);
        static int        parseInt(const std::string& s, bool* ok);
        static long long  parseLongLong(const std::string& s, bool* ok);
        static double     parseDouble(const std::string& s, bool* ok);

        // Thread worker
        struct Task;
        void workerLoop();

        // Shared stop logic: drain queue, clear state, emit NotRunning callback.
        // If sync=true, also joins the worker thread before cleanup.
        Status doStop(bool sync);

    private:
        // callback
        FeatureCallback cb_;
        void* cb_user_;

        // lifecycle state
        std::atomic<ServiceState> state_;

        // options
        const FeatureOptions opts_;

        // lower-lib instances (not owned); initialized during start()
        std::vector<IFeatureLib*> libs_;

        // authorization verifier (not owned); consulted in submit()
        IFeatureVerifier* verifier_;

        // combined keywords collected from all libs after start()
        std::vector<std::string> authKeywords_;

        // single worker & tasks
        std::thread                           worker_;
        std::deque<Task*>                     queue_;
        std::unordered_map<FeatureTicket, Task*> tasks_;
        std::atomic<FeatureTicket>            next_ticket_;
        mutable std::mutex                    mtx_;
        std::condition_variable               cv_;
        std::atomic<bool>                     stop_flag_;

        // Current running task (only one)
        Task* current_;

        // ---------- static deleter registry (class-scoped) ----------
        static std::mutex                                          s_deleter_mtx_;
        static std::unordered_map<std::string, FeaturePayloadDeleter> s_deleters_;
    };

    // ---- Internal Task ----
    struct FeatureService::Task {
        FeatureTicket id;
        std::function<void(std::vector<uint8_t>&)> fn;
        bool internal = false; // true: lib-init task; no result emitted, not tracked in tasks_
    };

} // namespace feat

#endif // FEATURE_SERVICE_H