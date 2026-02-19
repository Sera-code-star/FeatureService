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
    // Build with makeInputEvt(tag, actual_input*) and pass to submit().
    //
    // Ownership:
    //   submit() success (non-zero ticket) -> service holds it; handed back via callback.
    //   submit() failure (returns 0)       -> caller must call deleteInput(p).
    //   Callback receives it as the 3rd arg; caller must call deleteInput(p) when done.
    struct FeatureInput {
        const char* tag;   // agreed tag identifying the (input, output) type pair
        void*       data;  // heap-allocated actual typed input struct
    };

    // ---------- FeatureOutput ----------
    // Heap-allocated result packet handed to FeatureCallback as the 2nd argument.
    // The callback takes ownership; call deleteOutput(p) when done.
    // deleteOutput looks up the free_output deleter via the static handler table using tag.
    //
    // For service lifecycle events (TAG_START / TAG_STOP): data is null.
    // For task results  (status==Ok):          data is the actual_output* from biz_func.
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
    //   output — FeatureOutput*; always non-null.  Call feat::deleteOutput(output) when done.
    //   input  — FeatureInput* that was passed to submit(); null for lifecycle events.
    //            Call feat::deleteInput(input) when done.
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

    // ---------- Global make / delete helpers ----------
    // These are free functions so callers can manage FeatureInput/FeatureOutput lifetimes
    // without holding a reference to the service.

    // Step 1 — allocate a malloc'd copy of data[0..size) as the actual typed input struct.
    // Free with std::free() if not passed to makeInputEvt().
    void* makeFeatureInput(const void* data, size_t size);

    // Step 2 — wrap featureInput* in a submit-ready FeatureInput envelope.
    // Returns null on allocation failure.  Submit returns 0 for unregistered tags.
    void* makeInputEvt(const char* tag, void* featureInput);

    // Release a FeatureInput: looks up free_input in the static handler table by tag,
    // calls it on fi->data, then frees the envelope.  Safe to call with null.
    void deleteInput(void* input);

    // Release a FeatureOutput: looks up free_output in the static handler table by tag,
    // calls it on fo->data, then deletes the envelope.  Safe to call with null;
    // no-op data-free for lifecycle outputs whose tags are absent from the table.
    void deleteOutput(void* output);

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

        // Per-tag handler record.  Public so deleteInput/deleteOutput can name the type
        // when accessing the static handler table.
        struct HandlerEntry {
            FeatureHandlerFn  biz_func;
            FeatureHandlerDel free_input;
            FeatureHandlerDel free_output;
        };

        // Register a handler for tag. Thread-safe; call before start().
        //   biz_func(actual_input*)  -> actual_output* (heap-allocated)
        //   free_input(actual_input*) frees the input after biz_func returns
        //   free_output(actual_output*) freed by deleteOutput() via the static table
        void on(const char* tag,
                FeatureHandlerFn  biz_func,
                FeatureHandlerDel free_input,
                FeatureHandlerDel free_output);

        // Submit a task.
        // input must be a FeatureInput* (from makeInputEvt()) cast to void*.
        // On success (non-zero ticket): service holds input until the callback fires,
        //   at which point the callback receives input as its 4th argument.
        // On failure (returns 0): caller must call feat::deleteInput(input).
        FeatureTicket submit(void* input);

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

        // Global helpers access the static handler table.
        friend void deleteInput(void* input);
        friend void deleteOutput(void* output);

    private:
        // Unified output-build and callback-fire.
        // t==nullptr → service event: ticket=0, tag=tag, data=null, input=null.
        // t!=nullptr → task result:   ticket=t->id, tag from t->input->tag,
        //                             biz_func called if status==Ok.
        // cb_ is always valid; callback owns output and input; service only deletes Task entities.
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

        // Shared across all instances; cleared by stop().
        // Written only before start() (on()); read lock-free by dispatch()/submit() after.
        static std::unordered_map<std::string, HandlerEntry> handlers_;
        static std::mutex handlers_mtx_;  // guards concurrent on() calls before start()

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
        FeatureHandlerFn biz_func;  // resolved at submit() time from handlers_
        // Internal tasks (internal == true):
        std::function<void(std::vector<uint8_t>&)> fn;
        bool internal = false;
    };

} // namespace feat

#endif // FEATURE_SERVICE_H
