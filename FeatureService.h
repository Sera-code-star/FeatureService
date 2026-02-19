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
        Ok        = 0,
        Invalid   = 1,
        BadState  = 2,
        Internal  = 3,
        NotFound  = 4,
        Cancelled = 5   // task was cancelled before biz_func ran
    };

    enum class ServiceState : int {
        NotRunning   = 0,
        Initializing = 1,
        Running      = 2
    };

    // ---------- Ticket ----------
    typedef std::uint64_t FeatureTicket;

    // ---------- Handler function types ----------
    // biz_func : actual_input* -> actual_output*  (heap-allocated; freed by free_output)
    // free_input : frees the actual_input* after biz_func has run
    // free_output: frees the actual_output* returned by biz_func
    typedef void* (*FeatureHandlerFn) (void*);
    typedef void  (*FeatureHandlerDel)(void*);

    // ---------- FeatureInput ----------
    // Heap wrapper around the caller's typed input struct.
    //
    // Build with svc.makeInputEvt(tag, actual_input*) and pass to submit().
    //
    // Ownership:
    //   submit() success (non-zero ticket) -> service holds it; handed back via callback.
    //   submit() failure (returns 0)       -> caller must call svc.deleteInput(p).
    //   Callback receives it as the 4th arg; caller must call svc.deleteInput(p) when done.
    struct FeatureInput {
        const char* tag;   // agreed tag identifying the (input, output) type pair
        void*       data;  // heap-allocated actual typed input struct
    };

    // ---------- FeatureOutput ----------
    // Heap-allocated result packet handed to FeatureCallback as the 3rd argument.
    // The callback takes ownership; call svc.deleteOutput(p) when done.
    //
    // For service lifecycle events (TAG_START / TAG_STOP): data is null.
    // For task results  (status==Ok):        data is the actual_output* from biz_func.
    // For cancelled tasks (status==Cancelled): data is null.
    struct FeatureOutput {
        const char* tag;     // submitted tag, or TAG_START/TAG_STOP for lifecycle events
        Status      status;  // Ok or Cancelled
        void*       data;    // heap-allocated actual typed output struct; null if none
    };

    // ---------- Service lifecycle tags ----------
    // Used as FeatureOutput::tag for service-level events (ticket=0, data=null).
    static const char* const TAG_START = "start";  // service transitioned to Running
    static const char* const TAG_STOP  = "stop";   // service transitioned to NotRunning

    // ---------- Callback ----------
    // Fired for every task result and for service lifecycle events (TAG_START/TAG_STOP).
    //
    //   ticket — matches the value returned by submit(); 0 for lifecycle events.
    //   output — FeatureOutput*; always non-null.  Call svc.deleteOutput(output) when done.
    //   input  — FeatureInput* that was passed to submit(); null for lifecycle events.
    //            Call svc.deleteInput(input) when done.
    //
    // The service stores no client state; the callback owns both pointers.
    typedef void(*FeatureCallback)(FeatureTicket ticket,
                                   void* output,
                                   void* input);

    // ---------- Init Options ----------
    typedef std::unordered_map<std::string, std::string> FeatureOptions;

    // ---------- Lower-lib interface ----------
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
        explicit FeatureService(FeatureCallback cb,
                                const FeatureOptions& opts     = FeatureOptions(),
                                const std::vector<IFeatureLib*>& libs = std::vector<IFeatureLib*>(),
                                IFeatureVerifier* verifier     = nullptr);
        ~FeatureService();

        FeatureService(const FeatureService&) = delete;
        FeatureService& operator=(const FeatureService&) = delete;

        // Lifecycle
        Status start();
        Status stop(bool join = false);

        // Register a handler for tag. Thread-safe; call before start().
        //   biz_func(actual_input*)  -> actual_output* (heap-allocated)
        //   free_input(actual_input*) frees the input after biz_func returns
        //   free_output(actual_output*) is embedded in FeatureOutput so the caller
        //     can free it via deleteFeatureOutput()
        void on(const char* tag,
                FeatureHandlerFn  biz_func,
                FeatureHandlerDel free_input,
                FeatureHandlerDel free_output);

        // Step 1 — build the actual typed input struct on the heap.
        // Makes a malloc'd copy of data[0..size); returns it as void*.
        // Free with std::free() directly if not passed to makeInputEvt().
        static void* makeFeatureInput(const void* data, size_t size);

        // Step 2 — wrap the typed input in a submit-ready FeatureInput envelope.
        // Returns null if tag is not registered or on allocation failure.
        // Pass the result to submit(); call deleteInput() if submit() returns 0.
        void* makeInputEvt(const char* tag, void* featureInput);

        // Submit a task.
        // input must be a FeatureInput* (from makeInputEvt()) cast to void*.
        // On success (non-zero ticket): service holds input until the callback fires,
        //   at which point the callback receives input as its 4th argument.
        // On failure (returns 0): caller must call svc.deleteInput(input).
        FeatureTicket submit(void* input);

        // Universal tag-based deleters for use by the callback owner.
        // Resolve deleters via dispatch_table_ (the start-time read-only snapshot) —
        // no lock is taken.  Safe to call with null; no-op data-free for lifecycle-event
        // outputs whose tags (TAG_START/TAG_STOP) are not in the table.
        void deleteInput (void* input);
        void deleteOutput(void* output);

        // Cancel a pending task.
        // If the task is still queued: fires the callback with status=Cancelled.
        // If the task is currently running: returns Ok but does not interrupt it.
        Status cancel(FeatureTicket ticket);

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
        // Unified output-build and callback-fire.
        // t==nullptr → service event: ticket=0, tag=tag, data=null, input=null.
        // t!=nullptr → task result:   ticket=t->id, tag from t->input->tag,
        //                             biz_func called if status==Ok.
        // cb_ is always valid; callback owns output and input, service frees neither.
        void dispatch(Task* t, const char* tag, Status status);

        // Parsing helpers
        static bool       parseBool(const std::string& s, bool* ok);
        static int        parseInt(const std::string& s, bool* ok);
        static long long  parseLongLong(const std::string& s, bool* ok);
        static double     parseDouble(const std::string& s, bool* ok);

        struct Task;
        void workerLoop();

    private:
        FeatureCallback cb_;

        // per-tag handler registry (guarded by handlers_mtx_)
        struct HandlerEntry {
            FeatureHandlerFn  biz_func;
            FeatureHandlerDel free_input;
            FeatureHandlerDel free_output;
        };
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
        FeatureTicket    id;
        // User tasks (internal == false):
        FeatureInput*    input;     // whole envelope; service holds it until callback fires
        FeatureHandlerFn biz_func;  // resolved at submit() time from dispatch_table_
        // Internal tasks (internal == true):
        std::function<void(std::vector<uint8_t>&)> fn;
        bool internal = false;
    };

} // namespace feat

#endif // FEATURE_SERVICE_H
