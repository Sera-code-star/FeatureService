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
#include <algorithm>
#include <utility>
#include <new>

namespace feat {

    using std::string;
    using std::vector;
    using std::deque;
    using std::unordered_map;
    using std::function;
    using std::atomic;
    using std::mutex;
    using std::lock_guard;
    using std::unique_lock;
    using std::condition_variable;
    using std::thread;
    using std::uint64_t;
    using std::move;
    using std::sort;
    using std::bind;
    using std::free;
    using std::malloc;
    using std::memcpy;
    using std::strncpy;
    using std::nothrow;
    using std::memory_order_relaxed;

    // ---------- Status / ServiceState ----------
    enum class Status : int {
        Ok        = 0,
        Invalid   = 1,
        BadState  = 2,
        Internal  = 3,
        NotFound  = 4,
        Cancelled = 5   // task was cancelled before BizFunc ran
    };

    enum class ServiceState : int {
        NotRunning   = 0,
        Initializing = 1,
        Running      = 2
    };

    // ---------- Ticket ----------
    typedef uint64_t FeatureTicket;

    // ---------- Handler function types ----------
    // BizFunc   : actual_input* -> actual_output*  (heap-allocated; freed by FreeOutput)
    // FreeInput : frees the actual_input* after BizFunc has run
    // FreeOutput: frees the actual_output* returned by BizFunc
    typedef void* (*FeatureHandlerFn) (void*);
    typedef void  (*FeatureHandlerDel)(void*);

    // ---------- FeatureInput ----------
    // Heap wrapper around the caller's typed input struct.
    //
    // Build with MakeInputEvt(Tag, actual_input*) and pass to Submit().
    //
    // Ownership:
    //   Submit() success (non-zero ticket) -> service holds it; handed back via callback.
    //   Submit() failure (returns 0)       -> caller must call DeleteInput(P).
    //   Callback receives it as the 3rd arg; caller must call DeleteInput(P) when done.
    struct FeatureInput {
        const char* Tag;   // agreed tag identifying the (input, output) type pair
        void*       Data;  // heap-allocated actual typed input struct
    };

    // ---------- FeatureOutput ----------
    // Heap-allocated result packet handed to FeatureCallback as the 2nd argument.
    // The callback takes ownership; call DeleteOutput(P) when done.
    // DeleteOutput looks up the FreeOutput deleter via the static handler table using Tag.
    //
    // For service lifecycle events (TagStart / TagStop): Data is null.
    // For task results  (Status==Ok):          Data is the actual_output* from BizFunc.
    // For cancelled tasks (Status==Cancelled): Data is null.
    struct FeatureOutput {
        const char* Tag;     // submitted tag, or TagStart/TagStop for lifecycle events
        Status      Status;  // Ok or Cancelled
        void*       Data;    // heap-allocated actual typed output struct; null if none
    };

    // ---------- Service lifecycle tags ----------
    // Used as FeatureOutput::Tag for service-level events (ticket=0, Data=null).
    static const char* const TagStart = "start";  // service transitioned to Running
    static const char* const TagStop  = "stop";   // service transitioned to NotRunning

    // ---------- Callback ----------
    // Fired for every task result and for service lifecycle events (TagStart/TagStop).
    //
    //   Ticket — matches the value returned by Submit(); 0 for lifecycle events.
    //   Output — FeatureOutput*; always non-null.  Call feat::DeleteOutput(Output) when done.
    //   Input  — FeatureInput* that was passed to Submit(); null for lifecycle events.
    //            Call feat::DeleteInput(Input) when done.
    //
    // The service stores no client state; the callback owns both pointers.
    typedef void(*FeatureCallback)(FeatureTicket Ticket,
                                   void* Output,
                                   void* Input);

    // ---------- Init Options ----------
    typedef unordered_map<string, string> str_opt;

    // ---------- Lower-lib interface ----------
    class IFeatureLib {
    public:
        virtual ~IFeatureLib() {}
        virtual bool Init(const str_opt& Opts) = 0;
        virtual vector<string> GetKeywords() const = 0;
        // Called by the service just before a task starts.  The lib polls *Flag
        // during processing and aborts early if it reads non-zero.
        // Called again with nullptr after the task completes to clear the reference.
        virtual void Inject(atomic<char>* Flag) = 0;
        // Business function: actual_input* -> heap-allocated actual_output*.
        // The init lambda binds a specific instance to this via bind so that
        // each tag's handler entry carries the right this-pointer automatically.
        virtual void* Biz(void* Input) = 0;
        // C-style deleter for the heap-allocated actual_output* returned by Biz().
        // Stored in HandlerEntry::FreeOutput; called by DeleteOutput() to free Data.
        virtual FeatureHandlerDel OutputDeleter() const = 0;
    };

    // ---------- Authorization verifier interface ----------
    class IFeatureVerifier {
    public:
        virtual ~IFeatureVerifier() {}
        virtual bool IsAuthorized(const vector<string>& Keywords) const = 0;
    };

    // ---------- Global make / delete helpers ----------
    // These are free functions so callers can manage FeatureInput/FeatureOutput lifetimes
    // without holding a reference to the service.

    // Step 1 — allocate a malloc'd copy of Data[0..Size) as the actual typed input struct.
    // Free with free() if not passed to MakeInputEvt().
    void* MakeFeatureInput(const void* Data, size_t Size);

    // Step 2 — wrap InputPtr in a submit-ready FeatureInput envelope.
    // Returns null on allocation failure.  Submit returns 0 for unregistered tags.
    void* MakeInputEvt(const char* Tag, void* InputPtr);

    // Release a FeatureInput: looks up FreeInput in the static handler table by Tag,
    // calls it on Fi->Data, then frees the envelope.  Safe to call with null.
    void DeleteInput(void* Input);

    // Release a FeatureOutput: looks up FreeOutput in the static handler table by Tag,
    // calls it on Fo->Data, then deletes the envelope.  Safe to call with null;
    // no-op data-free for lifecycle outputs whose tags are absent from the table.
    void DeleteOutput(void* Output);

    // ---------- FeatureService ----------
    class FeatureService {
    public:
        explicit FeatureService(FeatureCallback Cb,
                                const str_opt& Opts = str_opt(),
                                IFeatureVerifier* Verifier = nullptr);
        ~FeatureService();

        FeatureService(const FeatureService&) = delete;
        FeatureService& operator=(const FeatureService&) = delete;

        // Lifecycle
        Status Start();
        Status Stop(bool Join = false);

        // Per-tag handler record.  Public so DeleteInput/DeleteOutput can name the type
        // when accessing the static handler table.
        //
        //   BizFunc    — function<void*(void*)>; holds either a plain function pointer
        //                (external registration) or a bind result that carries the lib
        //                instance as its implicit this-pointer (init-lambda registration).
        //   FreeInput  — c-style deleter for actual_input*; may be nullptr.
        //   FreeOutput — c-style deleter for the void* returned by BizFunc; always set for
        //                lib-bound entries (from IFeatureLib::OutputDeleter()).
        struct HandlerEntry {
            function<void*(void*)> BizFunc;
            FeatureHandlerDel      FreeInput;
            FeatureHandlerDel      FreeOutput;  // c-style output deleter: void(*)(void*)
        };

        // Register a handler for Tag. Thread-safe; call before Start().
        //   BizFunc accepts any callable convertible to function<void*(void*)>,
        //   including plain function pointers and bind expressions.
        void On(const char* Tag,
                function<void*(void*)> BizFunc,
                FeatureHandlerDel FreeInput,
                FeatureHandlerDel FreeOutput);

        // Submit a task.
        // Input must be a FeatureInput* (from MakeInputEvt()) cast to void*.
        // On success (non-zero ticket): service holds Input until the callback fires,
        //   at which point the callback receives Input as its 4th argument.
        // On failure (returns 0): caller must call feat::DeleteInput(Input).
        FeatureTicket Submit(void* Input);

        // Cancel a pending task.
        // If the task is still queued:   removes it and fires callback with Status=Cancelled.
        // If the task is currently running: sets its ExitFlag so the lib can abort early;
        //   the Cancelled callback is fired by the worker when Fn() returns.
        Status Cancel(FeatureTicket Ticket);

        // State snapshot
        bool         IsRunning()      const { return _State.load() == ServiceState::Running; }
        bool         IsInitializing() const { return _State.load() == ServiceState::Initializing; }
        ServiceState State()          const { return _State.load(); }

        // Options accessors
        bool        HasOption(const string& Key) const;
        string      GetOption(const string& Key) const;
        string      GetOptionOr(const string& Key, const string& Fallback) const;
        bool        GetBoolOr(const string& Key, bool Fallback) const;
        int         GetIntOr(const string& Key, int Fallback) const;
        long long   GetLongLongOr(const string& Key, long long Fallback) const;
        double      GetDoubleOr(const string& Key, double Fallback) const;

        // Global helpers access the static handler table.
        friend void DeleteInput(void* Input);
        friend void DeleteOutput(void* Output);

    private:
        // Unified output-build and callback-fire.
        // T==nullptr → service event: Ticket=0, Tag=Tag, Data=null, Input=null.
        // T!=nullptr → task result:   Ticket=T->Id, Tag from T->Input->Tag,
        //                             Fn called if Stat==Ok.
        // _Cb is always valid; callback owns Output and Input; service only deletes Task entities.
        // TaskLib: if non-null, Inject(nullptr) is called after Fn() returns and before _Cb fires.
        void Dispatch(Task* T, const char* Tag, Status Stat, IFeatureLib* TaskLib = nullptr);

        // Parsing helpers
        static bool       ParseBool(const string& S, bool* Ok);
        static int        ParseInt(const string& S, bool* Ok);
        static long long  ParseLongLong(const string& S, bool* Ok);
        static double     ParseDouble(const string& S, bool* Ok);

        struct Task;
        void WorkerLoop();

    private:
        FeatureCallback _Cb;

        // Shared across all instances; never cleared (handlers are permanent for the
        // lifetime of the service object).
        // Written by On(); read lock-free by Dispatch()/Submit() after Start().
        static unordered_map<string, HandlerEntry> _Handlers;
        static mutex _HandlersMtx;  // guards concurrent On() calls

        atomic<ServiceState>                _State;
        const str_opt                       _Opts;
        unordered_map<string, void*>        _TagLibMap;  // tag -> IFeatureLib* (void-erased)
        IFeatureVerifier*                   _Verifier;
        vector<string>                      _AuthKeywords;

        thread                              _Worker;
        deque<Task*>                        _Queue;
        unordered_map<FeatureTicket, Task*> _Tasks;
        atomic<FeatureTicket>               _NextTicket;
        mutable mutex                       _Mtx;
        condition_variable                  _Cv;
        atomic<bool>                        _StopFlag;
        Task*                               _Current;
    };

    struct FeatureService::Task {
        FeatureTicket          Id;
        FeatureInput*          Input;    // whole envelope; service holds it until callback fires
        // Unified callable: void*(void*) signature for both user and internal tasks.
        // User tasks:     Fn = BizFunc (resolved from _Handlers at Submit() time);
        //                 called as Fn(Input->Data) -> actual_output* in Dispatch().
        // Internal tasks: Fn = lambda wrapping startup logic;
        //                 called as Fn(nullptr), return value ignored.
        function<void*(void*)> Fn;
        bool                   Internal = false;
        atomic<char>           ExitFlag { 0 };  // cooperative-cancel signal; &ExitFlag injected into libs
    };

} // namespace feat

#endif // FEATURE_SERVICE_H
