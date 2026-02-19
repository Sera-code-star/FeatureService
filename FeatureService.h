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
    // Build with svc.makeFeatureInput(tag, actual_input*); the service looks up
    // free_input from the on() registration and embeds it here so that
    // deleteFeatureInput() can free everything without a service reference.
    //
    // Ownership:
    //   submit() success (non-zero ticket) -> service owns this; freed after biz_func returns.
    //   submit() failure (returns 0)       -> caller must call deleteFeatureInput().
    struct FeatureInput {
        const char*       tag;        // agreed tag identifying the (input, output) type pair
        void*             data;       // heap-allocated actual typed input struct
        FeatureHandlerDel free_data;  // how to free data (copied from on() registration)
    };

    // Universal C free function for FeatureInput*.
    // Calls fi->free_data(fi->data), then frees the wrapper.
    void deleteFeatureInput(void* p);

    // ---------- FeatureOutput ----------
    // Heap-allocated result packet handed to the universal FeatureCallback as void*.
    // The callback takes ownership; call deleteFeatureOutput(out) when done.
    //
    // For service lifecycle events (TAG_START / TAG_STOP): ticket=0, data=null.
    // For task results  (status==Ok):        data is the actual_output* from biz_func.
    // For cancelled tasks (status==Cancelled): data is null; input was freed by the service.
    struct FeatureOutput {
        const char*       tag;        // submitted tag, or TAG_START/TAG_STOP for service events
        FeatureTicket     ticket;     // 0 for service-level events
        Status            status;     // Ok or Cancelled
        void*             data;       // heap-allocated actual typed output struct; null if none
        FeatureHandlerDel free_data;  // how to free data; null if data is null
    };

    // Universal C free function for FeatureOutput*.
    // Calls fo->free_data(fo->data), then deletes the wrapper.
    void deleteFeatureOutput(void* p);

    // ---------- Service lifecycle tags ----------
    // Used as FeatureOutput::tag for service-level events (ticket=0, data=null).
    static const char* const TAG_START = "start";  // service transitioned to Running
    static const char* const TAG_STOP  = "stop";   // service transitioned to NotRunning

    // ---------- Callback ----------
    // Receives FeatureOutput* cast to void*.
    // Takes ownership of output; call deleteFeatureOutput(output) when done.
    typedef void(*FeatureCallback)(void* cb_user, void* output);

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

        // Step 2 — wrap the typed input in a submit-ready envelope.
        // Looks up free_input from the on() registration for tag and embeds it
        // so deleteFeatureInput() can chain-free featureInput + the wrapper.
        // Returns null if tag is not registered or on allocation failure.
        // Pass the result to submit(); call deleteFeatureInput() if submit() returns 0.
        void* makeInputEvt(const char* tag, void* featureInput);

        // Submit a task.
        // input must be a FeatureInput* (from makeFeatureInput()) cast to void*.
        // On success (non-zero ticket): service takes ownership of input.
        // On failure (returns 0): caller must call deleteFeatureInput(input).
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

    private:
        // Build and fire a FeatureOutput for a service lifecycle event (TAG_START/TAG_STOP).
        // ticket=0, data=null, status=Ok.
        void dispatchServiceEvent(const char* tag);

        // Run t->biz_func(t->raw_input) (unless status==Cancelled), free t->raw_input,
        // build FeatureOutput{tag,ticket,status,out_data}, and fire the callback.
        // Does NOT delete t; caller is responsible for that.
        void dispatchTask(Task* t, Status status);

        // Parsing helpers
        static bool       parseBool(const std::string& s, bool* ok);
        static int        parseInt(const std::string& s, bool* ok);
        static long long  parseLongLong(const std::string& s, bool* ok);
        static double     parseDouble(const std::string& s, bool* ok);

        struct Task;
        void workerLoop();

    private:
        FeatureCallback cb_;
        void*           cb_user_;

        // per-tag handler registry (guarded by handlers_mtx_)
        struct HandlerEntry {
            FeatureHandlerFn  biz_func;
            FeatureHandlerDel free_input;
            FeatureHandlerDel free_output;
        };
        std::unordered_map<std::string, HandlerEntry> handlers_;
        std::mutex handlers_mtx_;

        // read-only snapshot built once at start(); used by dispatchTask() with no lock
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
        FeatureTicket     id;
        // User tasks (internal == false): biz_func + raw input extracted at submit() time.
        const char*       tag;
        void*             raw_input;    // actual_input*; owned by this task until dispatched
        FeatureHandlerFn  biz_func;     // looked up in dispatch_table_ by submit()
        FeatureHandlerDel free_input;   // frees raw_input after biz_func returns (or on cancel)
        FeatureHandlerDel free_output;  // embedded in FeatureOutput for the caller
        // Internal tasks (internal == true): lifecycle work (e.g. lib init)
        std::function<void(std::vector<uint8_t>&)> fn;
        bool internal = false;
    };

} // namespace feat

#endif // FEATURE_SERVICE_H
