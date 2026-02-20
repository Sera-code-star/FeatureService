#include "FeatureService.h"
#include <algorithm>
#include <cerrno>
#include <climits>
#include <cfloat>
#include <cstring>

namespace feat {

    // ---------- static member definitions ----------
    std::unordered_map<std::string, FeatureService::HandlerEntry> FeatureService::handlers_;
    std::mutex FeatureService::handlers_mtx_;

    // ---------- deleteInput ----------
    // Global: looks up free_input via the static handler table, calls it on fi->data,
    // then frees the envelope.  No-op data-free if tag is absent (lifecycle events).
    void deleteInput(void* p) {
        FeatureInput* fi = static_cast<FeatureInput*>(p);
        if (!fi) return;
        auto it = FeatureService::handlers_.find(fi->tag);
        if (it != FeatureService::handlers_.end() && it->second.free_input && fi->data)
            it->second.free_input(fi->data);
        std::free(fi);
    }

    // ---------- deleteOutput ----------
    // Global: looks up free_output via the static handler table, calls it on fo->data,
    // then deletes the envelope.  No-op data-free for lifecycle outputs (tag absent).
    void deleteOutput(void* p) {
        FeatureOutput* fo = static_cast<FeatureOutput*>(p);
        if (!fo) return;
        auto it = FeatureService::handlers_.find(fo->tag);
        if (it != FeatureService::handlers_.end() && it->second.free_output && fo->data)
            it->second.free_output(fo->data);
        delete fo;
    }

    // ---------- ctor / dtor ----------
    FeatureService::FeatureService(FeatureCallback cb,
                                   const FeatureOptions& opts,
                                   IFeatureVerifier* verifier)
        : cb_(cb)
        , state_(ServiceState::NotRunning)
        , opts_(opts)
        , verifier_(verifier)
        , next_ticket_(1)
        , stop_flag_(false)
        , current_(nullptr)
    {}

    FeatureService::~FeatureService() {
        stop(true);
    }

    // ---------- on ----------
    void FeatureService::on(const char* tag,
                            FeatureHandlerFn  biz_func,
                            FeatureHandlerDel free_input,
                            FeatureHandlerDel free_output) {
        if (!tag || !biz_func) return;
        std::lock_guard<std::mutex> lk(handlers_mtx_);
        handlers_[tag] = { biz_func, free_input, free_output };
    }

    // ---------- setTagLib ----------
    // Associates an IFeatureLib* (passed as void* to avoid an extra cast at the call site)
    // with a tag.  workerLoop() consults tagLibMap_ to inject the exit flag only into
    // the lib that owns the given tag, rather than broadcasting to every lib.
    // Must be called before start().
    void FeatureService::setTagLib(const char* tag, void* lib) {
        if (!tag || !lib) return;
        tagLibMap_[tag] = lib;
    }

    // ---------- makeFeatureInput ----------
    // Global: allocates a malloc'd copy of data[0..size). The caller passes this to
    // makeInputEvt() as the featureInput argument, or frees it with std::free().
    void* makeFeatureInput(const void* data, size_t size) {
        if (!data || size == 0) return nullptr;
        void* p = std::malloc(size);
        if (!p) return nullptr;
        std::memcpy(p, data, size);
        return p;
    }

    // ---------- makeInputEvt ----------
    // Global: wraps featureInput* + tag in a malloc'd FeatureInput envelope for submit().
    // Tag validation happens in submit() (returns 0 for unregistered tags).
    void* makeInputEvt(const char* tag, void* featureInput) {
        if (!tag) return nullptr;
        FeatureInput* fi = static_cast<FeatureInput*>(std::malloc(sizeof(FeatureInput)));
        if (!fi) return nullptr;
        fi->tag  = tag;
        fi->data = featureInput;
        return static_cast<void*>(fi);
    }

    // ---------- dispatch ----------
    // Unified output-build and callback-fire for both service events and task results.
    //
    // Service event (t == nullptr):
    //   ticket=0, tag=tag, status=Ok, data=null, input=null.
    //
    // Internal task (t->internal == true):
    //   Calls t->fn(nullptr); no user callback fired.
    //
    // User task (t->internal == false):
    //   ticket=t->id, tag=t->input->tag; calls t->fn(input->data) if status==Ok.
    //   Does NOT delete t — caller (service) does.
    //
    // The service only deletes Task entities; deleteInput/deleteOutput look up
    // the static handler table to free input/output data.
    void FeatureService::dispatch(Task* t, const char* tag, Status status) {
        if (t && t->internal) {
            try { t->fn(nullptr); } catch (...) {}
            return;
        }

        FeatureTicket ticket  = t ? t->id         : 0;
        const char*   out_tag = t ? t->input->tag : tag;
        void*         input   = t ? static_cast<void*>(t->input) : nullptr;
        void*         out_data = nullptr;

        if (t && status == Status::Ok)
            out_data = t->fn(t->input->data);  // actual_input* -> actual_output*

        // Option A: if the lib aborted early via exitFlag, override status and discard
        // any partial output so the client sees the same shape as a queued cancel.
        if (t && t->exitFlag.load()) {
            status   = Status::Cancelled;
            out_data = nullptr;
        }

        FeatureOutput* out = new FeatureOutput();
        out->tag    = out_tag;
        out->status = status;
        out->data   = out_data;

        cb_(ticket, static_cast<void*>(out), input);
    }

    // ---------- start ----------
    Status FeatureService::start() {
        ServiceState expected = ServiceState::NotRunning;
        if (!state_.compare_exchange_strong(expected, ServiceState::Initializing))
            return Status::Ok;  // already Initializing or Running

        stop_flag_.store(false);
        authKeywords_.clear();

        Task* initTask = new (std::nothrow) Task();
        if (!initTask) {
            state_.store(ServiceState::NotRunning);
            return Status::Internal;
        }
        initTask->id       = 0;
        initTask->internal = true;
        initTask->fn = [this](void*) -> void* {
            // Each entry in tagLibMap_ is a lib instance produced by that lower-lib's
            // create() factory.  Phase 1 verifies every create() succeeded (non-null)
            // before we attempt any initialisation.
            for (auto& kv : tagLibMap_) {
                if (!kv.second) {
                    ServiceState exp = ServiceState::Initializing;
                    if (state_.compare_exchange_strong(exp, ServiceState::NotRunning)) {
                        dispatch(nullptr, TAG_STOP, Status::Ok);
                        stop_flag_.store(true);
                    }
                    return nullptr;
                }
            }

            // Phase 2: call lib->init(opts_) on each unique instance, then collect the
            // tags (keywords) it owns.  tagLibMap_ was pre-populated by setTagLib() so
            // each tag is already bound to its lib instance; init just resets/readies it.
            std::vector<void*> seen;
            for (auto& kv : tagLibMap_) {
                if (std::find(seen.begin(), seen.end(), kv.second) != seen.end()) continue;
                seen.push_back(kv.second);
                if (stop_flag_.load()) { authKeywords_.clear(); return nullptr; }
                IFeatureLib* lib = static_cast<IFeatureLib*>(kv.second);
                if (!lib->init(opts_)) {
                    ServiceState exp = ServiceState::Initializing;
                    if (state_.compare_exchange_strong(exp, ServiceState::NotRunning)) {
                        authKeywords_.clear();
                        dispatch(nullptr, TAG_STOP, Status::Ok);
                        stop_flag_.store(true);
                    }
                    return nullptr;
                }
                // Collect the tags this lib instance handles into authKeywords_ so the
                // verifier can authorise incoming requests against the full keyword set.
                std::vector<std::string> kw = lib->getKeywords();
                for (size_t j = 0; j < kw.size(); ++j) authKeywords_.push_back(kw[j]);
            }

            if (stop_flag_.load()) { authKeywords_.clear(); return nullptr; }

            std::sort(authKeywords_.begin(), authKeywords_.end());
            authKeywords_.erase(std::unique(authKeywords_.begin(), authKeywords_.end()),
                                authKeywords_.end());

            // Phase 3: transition to Running
            ServiceState exp = ServiceState::Initializing;
            if (!state_.compare_exchange_strong(exp, ServiceState::Running)) {
                authKeywords_.clear();
                return nullptr;
            }
            dispatch(nullptr, TAG_START, Status::Ok);
            return nullptr;
        };

        {
            std::lock_guard<std::mutex> lk(mtx_);
            tasks_.clear();
            current_ = nullptr;
            queue_.push_back(initTask);
        }

        try {
            worker_ = std::thread(&FeatureService::workerLoop, this);
        }
        catch (...) {
            std::lock_guard<std::mutex> lk(mtx_);
            for (Task* qt : queue_) delete qt;
            queue_.clear();
            state_.store(ServiceState::NotRunning);
            return Status::Internal;
        }

        return Status::Ok;
    }

    // ---------- stop ----------
    Status FeatureService::stop(bool join) {
        ServiceState old = state_.load();
        bool wasActive = false;
        for (;;) {
            if (old == ServiceState::NotRunning) break;
            if (state_.compare_exchange_weak(old, ServiceState::NotRunning)) {
                wasActive = true; break;
            }
        }

        stop_flag_.store(true);
        cv_.notify_all();

        if (join && worker_.joinable()) worker_.join();

        if (!wasActive) return Status::Ok;

        // Collect pending user tasks under the lock, then fire Cancelled callbacks
        // outside it so the client can safely call feat::deleteInput/deleteOutput.
        std::vector<Task*> pending;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            for (Task* t : queue_) {
                if (t->internal) delete t;
                else { tasks_.erase(t->id); pending.push_back(t); }
            }
            queue_.clear();
            current_ = nullptr;
        }
        for (Task* t : pending) {
            dispatch(t, nullptr, Status::Cancelled);  // fires callback; client gets input back
            delete t;
        }
        authKeywords_.clear();

        if (!join) dispatch(nullptr, TAG_STOP, Status::Ok);

        return Status::Ok;
    }

    // ---------- submit ----------
    // Looks up fn (biz_func) for fi->tag at enqueue time; stores the whole FeatureInput*
    // in the Task (service does NOT free it).  The worker calls fn(fi->data) via dispatch()
    // and then fires the callback with both output and the original input.
    // On failure (returns 0): input is NOT consumed; caller must call feat::deleteInput(p).
    FeatureTicket FeatureService::submit(void* input) {
        if (!input) return 0;
        FeatureInput* fi = static_cast<FeatureInput*>(input);
        if (!fi->tag) return 0;
        if (state_.load() != ServiceState::Running) return 0;
        if (verifier_ && !verifier_->isAuthorized(authKeywords_)) return 0;

        // Resolve biz_func from handlers_ (read-only after start(); no lock needed).
        auto it = handlers_.find(fi->tag);
        if (it == handlers_.end()) return 0;

        Task* t = new (std::nothrow) Task();
        if (!t) return 0;

        t->id       = next_ticket_.fetch_add(1);
        t->input    = fi;                   // whole envelope; service holds until callback
        t->fn       = it->second.biz_func;
        t->internal = false;

        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (stop_flag_.load()) { delete t; return 0; }  // caller still owns fi
            queue_.push_back(t);
            tasks_[t->id] = t;
        }
        cv_.notify_one();
        return t->id;
    }

    // ---------- cancel ----------
    Status FeatureService::cancel(FeatureTicket ticket) {
        Task* found = nullptr;

        {
            std::lock_guard<std::mutex> lk(mtx_);
            auto it = tasks_.find(ticket);
            if (it == tasks_.end()) return Status::NotFound;

            for (std::deque<Task*>::iterator qit = queue_.begin(); qit != queue_.end(); ++qit) {
                if ((*qit)->id == ticket) {
                    found = *qit;
                    queue_.erase(qit);
                    tasks_.erase(ticket);
                    break;
                }
            }

            // Not in queue: currently running — signal it to abort cooperatively.
            if (!found) {
                if (current_ != it->second) return Status::NotFound;
                current_->exitFlag.store(1);  // lib polls this; dispatch() fires Cancelled cb
                // tasks_ entry left intact; workerLoop erases it after dispatch() returns.
            }
        }

        if (found) {
            // Fire Cancelled callback; client receives original input back and frees it.
            dispatch(found, nullptr, Status::Cancelled);
            delete found;
        }
        return Status::Ok;
    }

    // ---------- workerLoop ----------
    void FeatureService::workerLoop() {
        for (;;) {
            Task* t = nullptr;
            {
                std::unique_lock<std::mutex> lk(mtx_);
                cv_.wait(lk, [this] { return stop_flag_.load() || !queue_.empty(); });
                if (stop_flag_.load() && queue_.empty()) return;
                t = queue_.front(); queue_.pop_front();
                if (!t->internal) current_ = t;
            }

            FeatureTicket id = t->id;

            // Resolve which lib handles this task's tag before dispatch() clears t->input.
            // Falls back to nullptr (no injection) if the tag has no registered lib.
            IFeatureLib* taskLib = nullptr;
            if (!t->internal) {
                auto it = tagLibMap_.find(t->input->tag);
                if (it != tagLibMap_.end())
                    taskLib = static_cast<IFeatureLib*>(it->second);
            }

            if (taskLib) taskLib->inject(&t->exitFlag);

            dispatch(t, nullptr, Status::Ok);

            if (taskLib) taskLib->inject(nullptr);   // release pointer before Task is deleted

            {
                std::lock_guard<std::mutex> lk(mtx_);
                if (current_ == t) current_ = nullptr;
                tasks_.erase(id);
            }
            delete t;
        }
    }

    // ---------- options ----------
    bool FeatureService::hasOption(const std::string& key) const {
        return opts_.find(key) != opts_.end();
    }
    std::string FeatureService::getOption(const std::string& key) const {
        auto it = opts_.find(key);
        if (it == opts_.end()) return std::string();
        return it->second;
    }
    std::string FeatureService::getOptionOr(const std::string& key, const std::string& fallback) const {
        auto it = opts_.find(key);
        if (it == opts_.end()) return fallback;
        return it->second;
    }

    bool FeatureService::parseBool(const std::string& s, bool* ok) {
        std::string t; t.reserve(s.size());
        for (size_t i = 0; i < s.size(); ++i) { char c = s[i]; if (c >= 'A' && c <= 'Z') c = char(c - 'A' + 'a'); t.push_back(c); }
        if (t == "1" || t == "true" || t == "yes" || t == "y") { if (ok)*ok = true; return true; }
        if (t == "0" || t == "false" || t == "no" || t == "n") { if (ok)*ok = true; return false; }
        if (ok)*ok = false; return false;
    }
    int       FeatureService::parseInt(const std::string& s, bool* ok) { char* e = 0; errno = 0; long v = strtol(s.c_str(), &e, 10); if (e == s.c_str() || *e != '\0' || errno == ERANGE || v<INT_MIN || v>INT_MAX) { if (ok)*ok = false; return 0; } if (ok)*ok = true; return (int)v; }
    long long FeatureService::parseLongLong(const std::string& s, bool* ok) { char* e = 0; errno = 0; long long v = strtoll(s.c_str(), &e, 10); if (e == s.c_str() || *e != '\0' || errno == ERANGE) { if (ok)*ok = false; return 0; } if (ok)*ok = true; return v; }
    double    FeatureService::parseDouble(const std::string& s, bool* ok) { char* e = 0; errno = 0; double v = strtod(s.c_str(), &e); if (e == s.c_str() || *e != '\0' || errno == ERANGE) { if (ok)*ok = false; return 0.0; } if (ok)*ok = true; return v; }

    double FeatureService::getDoubleOr(const std::string& key, double fallback) const {
        auto it = opts_.find(key); if (it == opts_.end()) return fallback;
        bool ok = false; double v = parseDouble(it->second, &ok); return ok ? v : fallback;
    }
    long long FeatureService::getLongLongOr(const std::string& key, long long fallback) const {
        auto it = opts_.find(key); if (it == opts_.end()) return fallback;
        bool ok = false; long long v = parseLongLong(it->second, &ok); return ok ? v : fallback;
    }
    int FeatureService::getIntOr(const std::string& key, int fallback) const {
        auto it = opts_.find(key); if (it == opts_.end()) return fallback;
        bool ok = false; int v = parseInt(it->second, &ok); return ok ? v : fallback;
    }
    bool FeatureService::getBoolOr(const std::string& key, bool fallback) const {
        auto it = opts_.find(key); if (it == opts_.end()) return fallback;
        bool ok = false; bool v = parseBool(it->second, &ok); return ok ? v : fallback;
    }

} // namespace feat
