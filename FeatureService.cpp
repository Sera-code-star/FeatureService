#include "FeatureService.h"
#include <algorithm>
#include <cerrno>
#include <climits>
#include <cfloat>
#include <cstring>

namespace feat {

    // ---------- static member definitions ----------
    unordered_map<string, FeatureService::HandlerEntry> FeatureService::_Handlers;
    mutex FeatureService::_HandlersMtx;

    // ---------- DeleteInput ----------
    // Global: looks up FreeInput via the static handler table, calls it on Fi->Data,
    // then frees the envelope.  No-op data-free if Tag is absent (lifecycle events).
    void DeleteInput(void* P) {
        FeatureInput* Fi = static_cast<FeatureInput*>(P);
        if (!Fi) return;
        auto It = FeatureService::_Handlers.find(Fi->Tag);
        if (It != FeatureService::_Handlers.end() && It->second.FreeInput && Fi->Data)
            It->second.FreeInput(Fi->Data);
        free(Fi);
    }

    // ---------- DeleteOutput ----------
    // Global: looks up FreeOutput via the static handler table, calls it on Fo->Data,
    // then deletes the envelope.  No-op data-free for lifecycle outputs (Tag absent).
    void DeleteOutput(void* P) {
        FeatureOutput* Fo = static_cast<FeatureOutput*>(P);
        if (!Fo) return;
        auto It = FeatureService::_Handlers.find(Fo->Tag);
        if (It != FeatureService::_Handlers.end() && It->second.FreeOutput && Fo->Data)
            It->second.FreeOutput(Fo->Data);
        delete Fo;
    }

    // ---------- ctor / dtor ----------
    FeatureService::FeatureService(FeatureCallback Cb,
                                   const str_opt& Opts,
                                   IFeatureVerifier* Verifier)
        : _Cb(Cb)
        , _State(ServiceState::NotRunning)
        , _Opts(Opts)
        , _Verifier(Verifier)
        , _NextTicket(1)
        , _StopFlag(false)
        , _Current(nullptr)
    {}

    FeatureService::~FeatureService() {
        Stop(true);
    }

    // ---------- On ----------
    void FeatureService::On(const char* Tag,
                            function<void*(void*)> BizFunc,
                            FeatureHandlerDel FreeInput,
                            FeatureHandlerDel FreeOutput) {
        if (!Tag || !BizFunc) return;
        lock_guard<mutex> Lk(_HandlersMtx);
        _Handlers[Tag] = { move(BizFunc), FreeInput, FreeOutput };
    }

    // ---------- MakeFeatureInput ----------
    // Global: allocates a malloc'd copy of Data[0..Size). The caller passes this to
    // MakeInputEvt() as the InputPtr argument, or frees it with free().
    void* MakeFeatureInput(const void* Data, size_t Size) {
        if (!Data || Size == 0) return nullptr;
        void* P = malloc(Size);
        if (!P) return nullptr;
        memcpy(P, Data, Size);
        return P;
    }

    // ---------- MakeInputEvt ----------
    // Global: wraps InputPtr + Tag in a malloc'd FeatureInput envelope for Submit().
    // Tag validation happens in Submit() (returns 0 for unregistered tags).
    void* MakeInputEvt(const char* Tag, void* InputPtr) {
        if (!Tag) return nullptr;
        FeatureInput* Fi = static_cast<FeatureInput*>(malloc(sizeof(FeatureInput)));
        if (!Fi) return nullptr;
        Fi->Tag  = Tag;
        Fi->Data = InputPtr;
        return static_cast<void*>(Fi);
    }

    // ---------- Dispatch ----------
    // Unified output-build and callback-fire for both service events and task results.
    //
    // Service event (T == nullptr):
    //   Ticket=0, Tag=Tag, Status=Ok, Data=null, Input=null.
    //
    // Internal task (T->Internal == true):
    //   Calls T->Fn(nullptr); no user callback fired.
    //
    // User task (T->Internal == false):
    //   Ticket=T->Id, Tag=T->Input->Tag; calls T->Fn(Input->Data) if Stat==Ok.
    //   Does NOT delete T — caller (service) does.
    //
    // The service only deletes Task entities; DeleteInput/DeleteOutput look up
    // the static handler table to free input/output data.
    void FeatureService::Dispatch(Task* T, const char* Tag, Status Stat, IFeatureLib* TaskLib) {
        if (T && T->Internal) {
            try { T->Fn(nullptr); } catch (...) {}
            return;
        }

        FeatureTicket Ticket  = T ? T->Id         : 0;
        const char*   OutTag  = T ? T->Input->Tag : Tag;
        void*         Input   = T ? static_cast<void*>(T->Input) : nullptr;
        void*         OutData = nullptr;

        if (T && Stat == Status::Ok)
            OutData = T->Fn(T->Input->Data);  // actual_input* -> actual_output*

        // Reset lib injection immediately after Fn() returns — before the callback
        // hands output to the caller.  Clears the lib's ExitFlag pointer whether the
        // task completed normally or aborted early via ExitFlag.
        if (TaskLib) TaskLib->Inject(nullptr);

        // Option A: if the lib aborted early via ExitFlag, override Stat and discard
        // any partial output so the client sees the same shape as a queued cancel.
        if (T && T->ExitFlag.load()) {
            Stat    = Status::Cancelled;
            OutData = nullptr;
        }

        FeatureOutput* Out = new FeatureOutput();
        Out->Tag    = OutTag;
        Out->Status = Stat;
        Out->Data   = OutData;

        _Cb(Ticket, static_cast<void*>(Out), Input);
    }

    // ---------- Start ----------
    // Total number of hard-coded IFeatureLib instances the service owns.
    // Increment this and add the matching instance construction inside the
    // init lambda below whenever a new feature library is integrated.
#define FeatureLibCount 0

    Status FeatureService::Start() {
        ServiceState Expected = ServiceState::NotRunning;
        if (!_State.compare_exchange_strong(Expected, ServiceState::Initializing))
            return Status::Ok;  // already Initializing or Running

        _StopFlag.store(false);
        _AuthKeywords.clear();

        Task* InitTask = new (nothrow) Task();
        if (!InitTask) {
            _State.store(ServiceState::NotRunning);
            return Status::Internal;
        }
        InitTask->Id       = 0;
        InitTask->Internal = true;
        InitTask->Fn = [this](void*) -> void* {
            // ── Hard-coded lib instances (FeatureLibCount above) ─────────────────
            // Add one IFeatureLib* per feature domain and increment FeatureLibCount.
            // Example:
            //   IFeatureLib* Libs[] = { new ConcreteLib(), new OtherLib() };
            vector<IFeatureLib*> Libs = {
                /* new ConcreteLib(), */
            };

            // ── Phase 1: Init each lib instance ──────────────────────────────────
            for (size_t I = 0; I < Libs.size(); ++I) {
                if (_StopFlag.load()) { _AuthKeywords.clear(); return nullptr; }
                if (!Libs[I]->Init(_Opts)) {
                    ServiceState Exp = ServiceState::Initializing;
                    if (_State.compare_exchange_strong(Exp, ServiceState::NotRunning)) {
                        _AuthKeywords.clear();
                        Dispatch(nullptr, TagStop, Status::Ok);
                        _StopFlag.store(true);
                    }
                    return nullptr;
                }
            }

            // ── Phase 2: collect tags per lib; sets must be disjoint ─────────────
            // TagToLib maps each keyword -> its owning lib; overlap is a config error.
            unordered_map<string, IFeatureLib*> TagToLib;
            for (size_t I = 0; I < Libs.size(); ++I) {
                vector<string> Kw = Libs[I]->GetKeywords();
                for (size_t J = 0; J < Kw.size(); ++J) {
                    if (TagToLib.count(Kw[J])) {
                        ServiceState Exp = ServiceState::Initializing;
                        if (_State.compare_exchange_strong(Exp, ServiceState::NotRunning)) {
                            _AuthKeywords.clear();
                            Dispatch(nullptr, TagStop, Status::Ok);
                            _StopFlag.store(true);
                        }
                        return nullptr;
                    }
                    TagToLib[Kw[J]] = Libs[I];
                    _AuthKeywords.push_back(Kw[J]);
                }
            }

            if (_StopFlag.load()) { _AuthKeywords.clear(); return nullptr; }

            // ── Phase 2b: authorization check ─────────────────────────────────────
            // Verifier runs here, in the worker thread, after _AuthKeywords is fully
            // built.  If denied the service fails to Start, same shape as Init() fail.
            if (_Verifier && !_Verifier->IsAuthorized(_AuthKeywords)) {
                ServiceState Exp = ServiceState::Initializing;
                if (_State.compare_exchange_strong(Exp, ServiceState::NotRunning)) {
                    _AuthKeywords.clear();
                    Dispatch(nullptr, TagStop, Status::Ok);
                    _StopFlag.store(true);
                }
                return nullptr;
            }

            // ── Phase 3: register each tag via On() ──────────────────────────────
            // Hard-code one On() call per tag. Do NOT drive this from TagToLib.
            // On(Tag, bind(&ConcreteLib::Biz, Instance, _1), nullptr, Deleter)
            //   Tag     – routing key (c-string, must be in _AuthKeywords)
            //   bind    – binds the lib instance as implicit this
            //   nullptr – no pre-filter
            //   Deleter – static class function (e.g. ConcreteLib::DeleteOutput)
            //             or global function exported from the lib (e.g. concretelib_delete).
            //             Do NOT pass OutputDeleter() – it is a virtual instance method,
            //             not a plain function pointer.
            // Example:
            //   On("some_tag", bind(&ConcreteLib::Biz, ConcreteLib, std::placeholders::_1),
            //      nullptr, ConcreteLib::DeleteOutput);
            //   _TagLibMap["some_tag"] = ConcreteLib;

            sort(_AuthKeywords.begin(), _AuthKeywords.end());

            // ── Phase 4: transition to Running ───────────────────────────────────
            ServiceState Exp = ServiceState::Initializing;
            if (!_State.compare_exchange_strong(Exp, ServiceState::Running)) {
                _AuthKeywords.clear();
                return nullptr;
            }
            Dispatch(nullptr, TagStart, Status::Ok);
            return nullptr;
        };

        {
            lock_guard<mutex> Lk(_Mtx);
            _Tasks.clear();
            _Current = nullptr;
            _Queue.push_back(InitTask);
        }

        try {
            _Worker = thread(&FeatureService::WorkerLoop, this);
        }
        catch (...) {
            lock_guard<mutex> Lk(_Mtx);
            for (Task* Qt : _Queue) delete Qt;
            _Queue.clear();
            _State.store(ServiceState::NotRunning);
            return Status::Internal;
        }

        return Status::Ok;
    }

    // ---------- Stop ----------
    Status FeatureService::Stop(bool Join) {
        ServiceState Old = _State.load();
        bool WasActive = false;
        for (;;) {
            if (Old == ServiceState::NotRunning) break;
            if (_State.compare_exchange_weak(Old, ServiceState::NotRunning)) {
                WasActive = true; break;
            }
        }

        _StopFlag.store(true);
        _Cv.notify_all();

        if (Join && _Worker.joinable()) _Worker.join();

        if (!WasActive) return Status::Ok;

        // Collect pending user tasks under the lock, then fire Cancelled callbacks
        // outside it so the client can safely call feat::DeleteInput/DeleteOutput.
        vector<Task*> Pending;
        {
            lock_guard<mutex> Lk(_Mtx);
            for (Task* T : _Queue) {
                if (T->Internal) delete T;
                else { _Tasks.erase(T->Id); Pending.push_back(T); }
            }
            _Queue.clear();
            _Current = nullptr;
        }
        for (Task* T : Pending) {
            Dispatch(T, nullptr, Status::Cancelled);  // fires callback; client gets input back
            delete T;
        }
        _AuthKeywords.clear();

        if (!Join) Dispatch(nullptr, TagStop, Status::Ok);

        return Status::Ok;
    }

    // ---------- Submit ----------
    // Looks up Fn (BizFunc) for Fi->Tag at enqueue time; stores the whole FeatureInput*
    // in the Task (service does NOT free it).  The worker calls Fn(Fi->Data) via Dispatch()
    // and then fires the callback with both output and the original Input.
    // On failure (returns 0): Input is NOT consumed; caller must call feat::DeleteInput(P).
    FeatureTicket FeatureService::Submit(void* Input) {
        if (!Input) return 0;
        FeatureInput* Fi = static_cast<FeatureInput*>(Input);
        if (!Fi->Tag) return 0;
        if (_State.load() != ServiceState::Running) return 0;

        // Resolve BizFunc from _Handlers (read-only after Start(); no lock needed).
        auto It = _Handlers.find(Fi->Tag);
        if (It == _Handlers.end()) return 0;

        Task* T = new (nothrow) Task();
        if (!T) return 0;

        T->Id       = _NextTicket.fetch_add(1);
        T->Input    = Fi;                   // whole envelope; service holds until callback
        T->Fn       = It->second.BizFunc;
        T->Internal = false;

        {
            lock_guard<mutex> Lk(_Mtx);
            if (_StopFlag.load()) { delete T; return 0; }  // caller still owns Fi
            _Queue.push_back(T);
            _Tasks[T->Id] = T;
        }
        _Cv.notify_one();
        return T->Id;
    }

    // ---------- Cancel ----------
    Status FeatureService::Cancel(FeatureTicket Ticket) {
        Task* Found = nullptr;

        {
            lock_guard<mutex> Lk(_Mtx);
            auto It = _Tasks.find(Ticket);
            if (It == _Tasks.end()) return Status::NotFound;

            for (deque<Task*>::iterator Qit = _Queue.begin(); Qit != _Queue.end(); ++Qit) {
                if ((*Qit)->Id == Ticket) {
                    Found = *Qit;
                    _Queue.erase(Qit);
                    _Tasks.erase(Ticket);
                    break;
                }
            }

            // Not in queue: currently running — signal it to abort cooperatively.
            if (!Found) {
                if (_Current != It->second) return Status::NotFound;
                _Current->ExitFlag.store(1);  // lib polls this; Dispatch() fires Cancelled cb
                // _Tasks entry left intact; WorkerLoop erases it after Dispatch() returns.
            }
        }

        if (Found) {
            // Fire Cancelled callback; client receives original Input back and frees it.
            Dispatch(Found, nullptr, Status::Cancelled);
            delete Found;
        }
        return Status::Ok;
    }

    // ---------- WorkerLoop ----------
    void FeatureService::WorkerLoop() {
        for (;;) {
            Task* T = nullptr;
            {
                unique_lock<mutex> Lk(_Mtx);
                _Cv.wait(Lk, [this] { return _StopFlag.load() || !_Queue.empty(); });
                if (_StopFlag.load() && _Queue.empty()) return;
                T = _Queue.front(); _Queue.pop_front();
                if (!T->Internal) _Current = T;
            }

            FeatureTicket Id = T->Id;

            // Resolve which lib handles this task's Tag before Dispatch() clears T->Input.
            // Falls back to nullptr (no injection) if the Tag has no registered lib.
            IFeatureLib* TaskLib = nullptr;
            if (!T->Internal) {
                auto It = _TagLibMap.find(T->Input->Tag);
                if (It != _TagLibMap.end())
                    TaskLib = static_cast<IFeatureLib*>(It->second);
            }

            if (TaskLib) TaskLib->Inject(&T->ExitFlag);

            Dispatch(T, nullptr, Status::Ok, TaskLib);  // resets Inject before _Cb fires

            {
                lock_guard<mutex> Lk(_Mtx);
                if (_Current == T) _Current = nullptr;
                _Tasks.erase(Id);
            }
            delete T;
        }
    }

    // ---------- options ----------
    bool FeatureService::HasOption(const string& Key) const {
        return _Opts.find(Key) != _Opts.end();
    }
    string FeatureService::GetOption(const string& Key) const {
        auto It = _Opts.find(Key);
        if (It == _Opts.end()) return string();
        return It->second;
    }
    string FeatureService::GetOptionOr(const string& Key, const string& Fallback) const {
        auto It = _Opts.find(Key);
        if (It == _Opts.end()) return Fallback;
        return It->second;
    }

    bool FeatureService::ParseBool(const string& S, bool* Ok) {
        string T; T.reserve(S.size());
        for (size_t I = 0; I < S.size(); ++I) { char C = S[I]; if (C >= 'A' && C <= 'Z') C = char(C - 'A' + 'a'); T.push_back(C); }
        if (T == "1" || T == "true" || T == "yes" || T == "y") { if (Ok)*Ok = true; return true; }
        if (T == "0" || T == "false" || T == "no" || T == "n") { if (Ok)*Ok = true; return false; }
        if (Ok)*Ok = false; return false;
    }
    int       FeatureService::ParseInt(const string& S, bool* Ok) { char* E = 0; errno = 0; long V = strtol(S.c_str(), &E, 10); if (E == S.c_str() || *E != '\0' || errno == ERANGE || V<INT_MIN || V>INT_MAX) { if (Ok)*Ok = false; return 0; } if (Ok)*Ok = true; return (int)V; }
    long long FeatureService::ParseLongLong(const string& S, bool* Ok) { char* E = 0; errno = 0; long long V = strtoll(S.c_str(), &E, 10); if (E == S.c_str() || *E != '\0' || errno == ERANGE) { if (Ok)*Ok = false; return 0; } if (Ok)*Ok = true; return V; }
    double    FeatureService::ParseDouble(const string& S, bool* Ok) { char* E = 0; errno = 0; double V = strtod(S.c_str(), &E); if (E == S.c_str() || *E != '\0' || errno == ERANGE) { if (Ok)*Ok = false; return 0.0; } if (Ok)*Ok = true; return V; }

    double FeatureService::GetDoubleOr(const string& Key, double Fallback) const {
        auto It = _Opts.find(Key); if (It == _Opts.end()) return Fallback;
        bool Ok = false; double V = ParseDouble(It->second, &Ok); return Ok ? V : Fallback;
    }
    long long FeatureService::GetLongLongOr(const string& Key, long long Fallback) const {
        auto It = _Opts.find(Key); if (It == _Opts.end()) return Fallback;
        bool Ok = false; long long V = ParseLongLong(It->second, &Ok); return Ok ? V : Fallback;
    }
    int FeatureService::GetIntOr(const string& Key, int Fallback) const {
        auto It = _Opts.find(Key); if (It == _Opts.end()) return Fallback;
        bool Ok = false; int V = ParseInt(It->second, &Ok); return Ok ? V : Fallback;
    }
    bool FeatureService::GetBoolOr(const string& Key, bool Fallback) const {
        auto It = _Opts.find(Key); if (It == _Opts.end()) return Fallback;
        bool Ok = false; bool V = ParseBool(It->second, &Ok); return Ok ? V : Fallback;
    }

} // namespace feat
