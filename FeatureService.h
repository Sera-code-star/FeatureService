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

    // ---------- Event ----------
    // Heap-allocated event packet handed to the universal FeatureCallback.
    // The callback takes ownership; call deleteFeatureEvent(ev) when done.
    // Per-tag handlers (on()) receive info directly and do NOT own the event.
    struct FeatureEvent {
        const char*   tag;              // static string literal; one of TAG_* below
        FeatureTicket ticket;           // 0 for service-level events
        void*         info;             // tag-specific heap data; null if none
        void        (*free_info)(void*);// used by deleteFeatureEvent to free info
    };

    // Free a heap FeatureEvent delivered to a FeatureCallback.
    // Releases ev->info via ev->free_info, then deletes the event itself.
    void deleteFeatureEvent(void* ev);

    // ---------- Tags ----------
    static const char* const TAG_START  = "start";   // service → Running
    static const char* const TAG_STOP   = "stop";    // service → NotRunning
    static const char* const TAG_CANCEL = "cancel";  // pending task cancelled (no data)
    static const char* const TAG_RESULT = "result";  // task completed; info = FeatureResultInfo*

    // ---------- Result info (carried by TAG_RESULT events) ----------
    struct FeatureResultInfo {
        void*  data;  // malloc'd result buffer; null if empty
        size_t size;  // 0 if empty
    };

    // ---------- Callbacks / Handlers ----------
    // FeatureCallback: single C-style universal listener (fires for every tag).
    typedef void(*FeatureCallback)(void* user, const FeatureEvent* ev);

    // Raw C function pointers registered per tag via on().
    typedef void* (*FeatureHandlerFn) (void*);   // biz_func: transforms info -> heap result
    typedef void  (*FeatureHandlerDel)(void*);   // delete_void: frees that result

    // Heap wrapper placed in FeatureEvent::info when a handler is registered for the tag.
    // Call deleteResult(ev->info) to free both the result data and this wrapper.
    struct FeatureResult {
        void*             data;  // return value of biz_func; null if biz_func returned null
        FeatureHandlerDel del;   // delete_void supplied to on()
    };

    // Universal free for a FeatureResult* delivered via FeatureEvent::info.
    // Calls del(data) then deletes the FeatureResult wrapper.
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

        // Submit
        FeatureTicket submit(const std::function<void(std::vector<uint8_t>& out)>& fn);

        // Cancel
        Status cancel(FeatureTicket ticket);

        // Link tag to a biz_func + delete_void pair. Thread-safe; call before start().
        // biz_func(info) -> heap result placed in FeatureEvent::info as FeatureResult*.
        // delete_void frees that result; call deleteResult(ev->info) when done.
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
        // Look up handlers for `tag`, call each with the event, then free info.
        void dispatch(const char* tag, FeatureTicket ticket,
                      void* info, void(*free_info)(void*));

        // Parsing helpers
        static bool       parseBool(const std::string& s, bool* ok);
        static int        parseInt(const std::string& s, bool* ok);
        static long long  parseLongLong(const std::string& s, bool* ok);
        static double     parseDouble(const std::string& s, bool* ok);

        struct Task;
        void workerLoop();

    private:
        // universal catch-all callback (fires for every tag before per-tag handlers)
        FeatureCallback cb_;
        void*           cb_user_;

        // per-tag handler registry (registration, guarded by handlers_mtx_)
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
        std::function<void(std::vector<uint8_t>&)> fn;
        bool internal = false;
    };

} // namespace feat

#endif // FEATURE_SERVICE_H
