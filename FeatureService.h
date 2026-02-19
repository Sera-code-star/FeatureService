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
        Initializing = 1,
        Running      = 2
    };

    // ---------- Ticket ----------
    typedef std::uint64_t FeatureTicket;

    // ---------- Params ----------
    // Heap wrapper for caller-supplied input data passed to submit().
    // Build with packFeatureParams(); the service takes ownership on a successful submit()
    // and frees via the free_params deleter after the callback fires (or on cancel/stop).
    struct FeatureParams {
        void*  data;  // malloc'd copy of the caller's bytes; null if size == 0
        size_t size;
    };

    // Allocate a FeatureParams on the heap containing a malloc'd copy of data[0..size).
    // Returns null on allocation failure.
    // Typical usage: pass the result (as void*) to submit() with freeFeatureParams.
    FeatureParams* packFeatureParams(const void* data, size_t size);

    // Free a FeatureParams* allocated by packFeatureParams().
    // Compatible with the free_params parameter of submit().
    void freeFeatureParams(void* p);

    // ---------- Event ----------
    // Heap-allocated event packet handed to the universal FeatureCallback.
    // The callback takes ownership; call deleteFeatureEvent(ev) when done.
    struct FeatureEvent {
        const char*   tag;               // one of TAG_* below
        FeatureTicket ticket;            // 0 for service-level events (start/stop)
        void*         result;            // output of the tag handler (biz_func); null if none
        void        (*free_result)(void*);   // how to free result; null if result is null
        void*         user_input;        // original params passed to submit(); null for service events
        void        (*free_user_input)(void*); // how to free user_input; null if not owned
    };

    // Free a heap FeatureEvent delivered to a FeatureCallback.
    // Calls free_result(result) and free_user_input(user_input), then deletes the event.
    void deleteFeatureEvent(void* ev);

    // ---------- Tags ----------
    static const char* const TAG_START  = "start";   // service -> Running
    static const char* const TAG_STOP   = "stop";    // service -> NotRunning
    static const char* const TAG_CANCEL = "cancel";  // pending task cancelled; user_input echoed back
    static const char* const TAG_RESULT = "result";  // task completed; result = FeatureResult* if handler set

    // ---------- Callbacks / Handlers ----------
    // FeatureCallback: universal listener fired for every event.
    typedef void(*FeatureCallback)(void* user, const FeatureEvent* ev);

    // Raw C function pointers registered per tag via on().
    typedef void* (*FeatureHandlerFn) (void*);   // biz_func: user_input -> heap result
    typedef void  (*FeatureHandlerDel)(void*);   // delete_void: frees that result

    // Heap wrapper placed in FeatureEvent::result when a handler is registered for the tag.
    // Call deleteResult(ev->result) to free both the result data and this wrapper.
    struct FeatureResult {
        void*             data;  // return value of biz_func; null if biz_func returned null
        FeatureHandlerDel del;   // delete_void supplied to on()
    };

    // Universal free for a FeatureResult* delivered via FeatureEvent::result.
    void deleteResult(void* r);

    // ---------- Init Options ----------
    typedef std::unordered_map<std::string, std::string> FeatureOptions;

    // ---------- Lower-lib interface ----------
    // Each lower-level library implements this. The service does NOT own instances.
    class IFeatureLib {
    public:
        virtual ~IFeatureLib() {}
        virtual bool init(const FeatureOptions& opts) = 0;
        virtual std::vector<std::string> getKeywords() const = 0;
    };

    // ---------- Authorization verifier interface ----------
    class IFeatureVerifier {
    public:
        virtual ~IFeatureVerifier() {}
        virtual bool isAuthorized(const std::vector<std::string>& keywords) const = 0;
    };

    // ---------- FeatureService ----------
    class FeatureService {
    public:
        explicit FeatureService(FeatureCallback cb              = nullptr,
                                void* cb_user                  = nullptr,
                                const FeatureOptions& opts     = FeatureOptions(),
                                const std::vector<IFeatureLib*>& libs = std::vector<IFeatureLib*>(),
                                IFeatureVerifier* verifier     = nullptr);
        ~FeatureService();

        FeatureService(const FeatureService&) = delete;
        FeatureService& operator=(const FeatureService&) = delete;

        // Lifecycle
        Status start();
        Status stop(bool join = false);

        // Submit a task identified by tag, with heap params (typically from packFeatureParams()).
        // On success (non-zero ticket): service takes ownership of params and calls free_params
        //   on it after the callback fires (or on cancel/stop).
        // On failure (returns 0): params is NOT consumed; caller must free it.
        FeatureTicket submit(const char* tag, void* params, void(*free_params)(void*));

        // Cancel
        Status cancel(FeatureTicket ticket);

        // Link tag to a biz_func + delete_void pair. Thread-safe; call before start().
        // biz_func(user_input) -> heap result placed in FeatureEvent::result as FeatureResult*.
        // delete_void frees that result; call deleteResult(ev->result) when done.
        void on(const char* tag, FeatureHandlerFn biz_func, FeatureHandlerDel delete_void);

        // State snapshot
        bool         isRunning()      const { return state_.load() == ServiceState::Running; }
        bool         isInitializing() const { return state_.load() == ServiceState::Initializing; }
        ServiceState state()          const { return state_.load(); }

        // Options accessors
        bool        hasOption(const std::string& key) const;
        std::string getOption(const std::string& key) const;
        std::string getOptionOr(const std::string& key, const std::string& fallback) const;
        bool        getBoolOr(const std::string& key, bool fallback) const;
        int         getIntOr(const std::string& key, int fallback) const;
        long long   getLongLongOr(const std::string& key, long long fallback) const;
        double      getDoubleOr(const std::string& key, double fallback) const;

    private:
        // Call biz_func(user_input) if a handler is registered for tag, then fire the callback
        // with an event carrying both the result and the echoed user_input.
        // For service events (start/stop), pass null for user_input and free_user_input.
        void dispatch(const char* tag, FeatureTicket ticket,
                      void* user_input, void(*free_user_input)(void*));

        // Parsing helpers
        static bool       parseBool(const std::string& s, bool* ok);
        static int        parseInt(const std::string& s, bool* ok);
        static long long  parseLongLong(const std::string& s, bool* ok);
        static double     parseDouble(const std::string& s, bool* ok);

        struct Task;
        void workerLoop();

    private:
        // universal catch-all callback (fires for every event)
        FeatureCallback cb_;
        void*           cb_user_;

        // per-tag handler registry (guarded by handlers_mtx_)
        struct HandlerEntry { FeatureHandlerFn fn; FeatureHandlerDel del; };
        std::unordered_map<std::string, HandlerEntry> handlers_;
        std::mutex handlers_mtx_;

        // read-only snapshot built once at start(); used by dispatch() with no lock
        std::unordered_map<std::string, HandlerEntry> dispatch_table_;

        std::atomic<ServiceState>                state_;
        const FeatureOptions                     opts_;
        std::vector<IFeatureLib*>                libs_;
        IFeatureVerifier*                        verifier_;
        std::vector<std::string>                 authKeywords_;

        std::thread                              worker_;
        std::deque<Task*>                        queue_;
        std::unordered_map<FeatureTicket, Task*> tasks_;
        std::atomic<FeatureTicket>               next_ticket_;
        mutable std::mutex                       mtx_;
        std::condition_variable                  cv_;
        std::atomic<bool>                        stop_flag_;
        Task*                                    current_;
    };

    struct FeatureService::Task {
        FeatureTicket id;
        const char*   tag;               // user tasks only; null for internal
        void*         user_input;        // user tasks only; null for internal
        void        (*free_user_input)(void*); // user tasks only; null for internal
        std::function<void(std::vector<uint8_t>&)> fn; // internal tasks only
        bool internal = false;
    };

} // namespace feat

#endif // FEATURE_SERVICE_H
