#include "FeatureService.h"
#include <algorithm>
#include <cerrno>
#include <climits>
#include <cfloat>
#include <cstring>

namespace feat {

    // ---------- deleteInput ----------
    // Looks up free_input in dispatch_table_ (the start-time snapshot, read-only
    // after start() — no lock needed), calls it on fi->data, then frees the envelope.
    void FeatureService::deleteInput(void* p) {
        FeatureInput* fi = static_cast<FeatureInput*>(p);
        if (!fi) return;
        auto it = dispatch_table_.find(fi->tag);
        if (it != dispatch_table_.end() && it->second.free_input && fi->data)
            it->second.free_input(fi->data);
        std::free(fi);
    }

    // ---------- deleteOutput ----------
    // Same: dispatch_table_ is read-only after start(); no lock needed.
    // No-op data-free for lifecycle tags (TAG_START/TAG_STOP) absent from the table.
    void FeatureService::deleteOutput(void* p) {
        FeatureOutput* fo = static_cast<FeatureOutput*>(p);
        if (!fo) return;
        auto it = dispatch_table_.find(fo->tag);
        if (it != dispatch_table_.end() && it->second.free_output && fo->data)
            it->second.free_output(fo->data);
        delete fo;
    }

    // ---------- ctor / dtor ----------
    FeatureService::FeatureService(FeatureCallback cb,
                                   const FeatureOptions& opts,
                                   const std::vector<IFeatureLib*>& libs,
                                   IFeatureVerifier* verifier)
        : cb_(cb)
        , state_(ServiceState::NotRunning)
        , opts_(opts)
        , libs_(libs)
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

    // ---------- makeFeatureInput ----------
    // Allocates a malloc'd copy of data[0..size). The caller passes this to
    // makeInputEvt() as the featureInput argument, or frees it with std::free().
    void* FeatureService::makeFeatureInput(const void* data, size_t size) {
        if (!data || size == 0) return nullptr;
        void* p = std::malloc(size);
        if (!p) return nullptr;
        std::memcpy(p, data, size);
        return p;
    }

    // ---------- makeInputEvt ----------
    // Wraps featureInput* + tag in a malloc'd FeatureInput envelope for submit().
    // Validates that tag is registered; returns null if not or on allocation failure.
    void* FeatureService::makeInputEvt(const char* tag, void* featureInput) {
        if (!tag) return nullptr;
        {
            std::lock_guard<std::mutex> lk(handlers_mtx_);
            if (handlers_.find(tag) == handlers_.end()) return nullptr;
        }
        FeatureInput* fi = static_cast<FeatureInput*>(std::malloc(sizeof(FeatureInput)));
        if (!fi) return nullptr;
        fi->tag  = tag;
        fi->data = featureInput;
        return static_cast<void*>(fi);
    }

    // ---------- dispatchServiceEvent ----------
    // Fires TAG_START or TAG_STOP: ticket=0, output{tag,Ok,null}, input=null.
    void FeatureService::dispatchServiceEvent(const char* tag) {
        FeatureOutput* out = new FeatureOutput();
        out->tag    = tag;
        out->status = Status::Ok;
        out->data   = nullptr;
        if (cb_) cb_(0, static_cast<void*>(out), nullptr);
        else     deleteOutput(static_cast<void*>(out));
    }

    // ---------- dispatchTask ----------
    // If status==Ok:        calls t->biz_func(t->input->data) -> actual_output*.
    // If status==Cancelled: skips biz_func; out->data is null.
    // Fires cb_(user, ticket, output, input) — service does NOT free either ptr.
    // The callback owns both; it must call svc.deleteOutput(output) and
    // svc.deleteInput(input) when done.  Does NOT delete t; caller owns t.
    void FeatureService::dispatchTask(Task* t, Status status) {
        void* out_data = nullptr;
        if (status == Status::Ok)
            out_data = t->biz_func(t->input->data);  // actual_input* -> actual_output*

        FeatureOutput* out = new FeatureOutput();
        out->tag    = t->input->tag;
        out->status = status;
        out->data   = out_data;

        // Pass both output and original input to the callback.
        // Service does NOT free either; client calls deleteInput / deleteOutput.
        if (cb_) {
            cb_(t->id, static_cast<void*>(out), static_cast<void*>(t->input));
        } else {
            // No callback registered: avoid leaks by cleaning up ourselves.
            deleteOutput(static_cast<void*>(out));
            deleteInput(static_cast<void*>(t->input));
        }
        t->input = nullptr;  // ownership transferred; guard against double-use
    }

    // ---------- start ----------
    Status FeatureService::start() {
        ServiceState expected = ServiceState::NotRunning;
        if (!state_.compare_exchange_strong(expected, ServiceState::Initializing))
            return Status::Ok;  // already Initializing or Running

        stop_flag_.store(false);
        authKeywords_.clear();
        { std::lock_guard<std::mutex> lk(handlers_mtx_); dispatch_table_ = handlers_; }

        Task* initTask = new (std::nothrow) Task();
        if (!initTask) {
            state_.store(ServiceState::NotRunning);
            return Status::Internal;
        }
        initTask->id       = 0;
        initTask->internal = true;
        initTask->fn = [this](std::vector<uint8_t>&) {
            // Phase 1: null-guard all libs
            for (size_t i = 0; i < libs_.size(); ++i) {
                if (!libs_[i]) {
                    ServiceState exp = ServiceState::Initializing;
                    if (state_.compare_exchange_strong(exp, ServiceState::NotRunning)) {
                        dispatchServiceEvent(TAG_STOP);
                        stop_flag_.store(true);
                    }
                    return;
                }
            }

            // Phase 2: init all libs, collect keywords
            for (size_t i = 0; i < libs_.size(); ++i) {
                if (stop_flag_.load()) { authKeywords_.clear(); return; }
                if (!libs_[i]->init(opts_)) {
                    ServiceState exp = ServiceState::Initializing;
                    if (state_.compare_exchange_strong(exp, ServiceState::NotRunning)) {
                        authKeywords_.clear();
                        dispatchServiceEvent(TAG_STOP);
                        stop_flag_.store(true);
                    }
                    return;
                }
                std::vector<std::string> kw = libs_[i]->getKeywords();
                for (size_t j = 0; j < kw.size(); ++j) authKeywords_.push_back(kw[j]);
            }

            if (stop_flag_.load()) { authKeywords_.clear(); return; }

            std::sort(authKeywords_.begin(), authKeywords_.end());
            authKeywords_.erase(std::unique(authKeywords_.begin(), authKeywords_.end()),
                                authKeywords_.end());

            // Phase 3: transition to Running
            ServiceState exp = ServiceState::Initializing;
            if (!state_.compare_exchange_strong(exp, ServiceState::Running)) {
                authKeywords_.clear();
                return;
            }
            dispatchServiceEvent(TAG_START);
        };

        {
            std::lock_guard<std::mutex> lk(mtx_);
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
        // outside it so the client can safely call deleteInput/deleteOutput.
        std::vector<Task*> pending;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            for (Task* t : queue_) {
                if (t->internal) delete t;
                else             pending.push_back(t);
            }
            queue_.clear();
            tasks_.clear();
            current_ = nullptr;
        }
        for (Task* t : pending) {
            dispatchTask(t, Status::Cancelled);  // fires callback; client gets input back
            delete t;
        }
        authKeywords_.clear();

        if (!join) dispatchServiceEvent(TAG_STOP);
        return Status::Ok;
    }

    // ---------- submit ----------
    // Looks up biz_func for fi->tag at enqueue time; stores the whole FeatureInput*
    // in the Task (service does NOT free it).  The worker calls biz_func(fi->data) and
    // then fires the callback with both output and the original input.
    // On failure (returns 0): input is NOT consumed; caller must call svc.deleteInput(p).
    FeatureTicket FeatureService::submit(void* input) {
        if (!input) return 0;
        FeatureInput* fi = static_cast<FeatureInput*>(input);
        if (!fi->tag) return 0;
        if (state_.load() != ServiceState::Running) return 0;
        if (verifier_ && !verifier_->isAuthorized(authKeywords_)) return 0;

        // Resolve biz_func now from the start()-time snapshot (no lock needed).
        auto it = dispatch_table_.find(fi->tag);
        if (it == dispatch_table_.end()) return 0;

        Task* t = new (std::nothrow) Task();
        if (!t) return 0;

        t->id       = next_ticket_.fetch_add(1);
        t->input    = fi;                   // whole envelope; service holds until callback
        t->biz_func = it->second.biz_func;
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

            // Not in queue: currently running; can't interrupt it
            if (!found && current_ != it->second)
                return Status::NotFound;
        }

        if (found) {
            // Fire Cancelled callback; client receives original input back and frees it.
            dispatchTask(found, Status::Cancelled);
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

            if (t->internal) {
                std::vector<uint8_t> out;
                try { t->fn(out); } catch (...) {}
                delete t;
            } else {
                FeatureTicket id = t->id;

                // biz_func and raw_input already resolved at submit() time;
                // dispatchTask calls t->biz_func(t->raw_input), frees raw_input, fires cb_.
                dispatchTask(t, Status::Ok);

                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    if (current_ == t) current_ = nullptr;
                    tasks_.erase(id);
                }
                delete t;
            }
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
